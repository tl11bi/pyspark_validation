from pyspark.sql.types import StringType
from pyspark.sql.functions import col, lit, unix_millis
from datetime import datetime
import os
import re

class BronzeIngestion:
    def __init__(self, _config):
        self._config = _config

    def load_file_to_bronze(self, job_id, job_run_id, folder_name) -> bool:
        folder_conf = self._config
        folder = folder_name
        source_type = folder_conf["source_type"]
        adls_path = RAMS_BRONZE_LOAD_PATH + source_type + "/"
        checkpoint_path = RAMS_BRONZE_CHECKPOINT_PATH + "/" + source_type + "/" + folder

        try:
            dbutils.fs.ls(adls_path + folder)

            file_format = folder_conf["file_format"].lower()
            source_id = folder_conf["source_id"]
            validation_template_id = folder_conf["validation_template_id"]
            validation_template = folder_conf["template"]

            reader = (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", file_format)
                .option("cloudFiles.schemaLocation", checkpoint_path)
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.maxFilesPerTrigger", "1")
            )

            # minimal format-specific options
            if file_format == "json":
                reader = reader.option("cloudFiles.json.failOnInvalidFile", "false")
                # optional schema hints example:
                # reader = reader.option("cloudFiles.schemaHints", "dealRid STRING, facilityRid STRING")
            elif file_format == "csv":
                reader = reader.option("cloudFiles.csv.header", "true")

            df = (
                reader.load(adls_path + folder)
                .withColumn("file_path", col("_metadata.file_path"))
                .withColumn("file_modification_time", unix_millis(col("_metadata.file_modification_time")))
                .withColumn("job_id", lit(job_id))
                .withColumn("run_id", lit(job_run_id))
                .withColumn("source_id", lit(source_id))
                .withColumn("folder", lit(folder))
                .withColumn("validation_template_id", lit(validation_template_id))
                .withColumn("validation_template", lit(validation_template))
                .withColumn("file_format", lit(file_format))
            )

            if file_format == "csv":
                # keep minimal sanitation (replace non-alnum with underscore)
                df = df.select(*[col(c).alias(re.sub(r"\W+", "_", c)) for c in df.columns])

            query = (
                df.writeStream.format("delta")
                .option("checkpointLocation", checkpoint_path)
                .foreachBatch(process_bronze_load_batch)
                .option("mergeSchema", "true")
                .queryName("BronzeLoad" + folder)
                .trigger(availableNow=True)
                .start()
            )

            query.awaitTermination()

        except Exception as e:
            if "java.io.FileNotFoundException" in str(e):
                print("FileNotFoundException")
                return False
            raise
        return True
    
    
    