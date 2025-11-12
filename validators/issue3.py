def process_bronze_load_batch(microBatchDF, batchId):
    print("process_bronze_load_batch")

    # Cheap emptiness check (works with classic & Connect)
    if microBatchDF.limit(1).count() == 0:
        print(f"Micro-batch {batchId} empty")
        return

    # Single action for first row
    first_record = microBatchDF.limit(1).collect()[0]

    file_name = first_record["file_path"]
    folder = first_record["folder"]
    job_id = first_record["job_id"]
    run_id = first_record["run_id"]
    source_id = first_record["source_id"]
    modification_time = first_record["file_modification_time"]  # corrected name
    validation_template = first_record["validation_template"]
    validation_template_id = first_record["validation_template_id"]
    file_format = first_record["file_format"].lower()

    ingestion_log_id = insertIngestionlog(job_id, run_id, file_name, source_id, modification_time)

    df = microBatchDF

    # Conditional flattening for nested formats
    if file_format in ("json", "parquet"):
        df, _ = flatten_all(df, sep=".", explode_arrays=True)

    # Load rules
    rules = SparkDataValidator.load_rules_json(validation_template)

    # Ensure header columns exist (add nulls for missing)
    for r in rules:
        if r.get("type") == "headers":
            for c in r.get("columns", []):
                if c not in df.columns:
                    df = df.withColumn(c, F.lit(None).cast("string"))

    # Attach ingestion_log_id (keep job/run for traceability)
    df = df.withColumn("ingestion_log_id", F.lit(ingestion_log_id))

    validator = SparkDataValidator(
        spark_session=df.sparkSession,
        id_cols=_infer_id_cols(rules),
        fail_fast=False,
        fail_mode="return",
    )

    try:
        is_valid, valid_df, errors_df = validator.validate(df, rules)
    except Exception as e:
        updateIngestionlog(ingestion_log_id, "failed", f"validator exception: {e}")
        print(f"Validation crashed: {e}")
        return

    if is_valid:
        updateIngestionlog(ingestion_log_id, "success", "validation passed")
    else:
        sample = errors_df.limit(5).collect()
        msg = "; ".join(f"{r.column}:{r.message}" for r in sample)
        updateIngestionlog(ingestion_log_id, "failed", msg)
        print(f"Validation failed count={errors_df.count()} sample={sample}")