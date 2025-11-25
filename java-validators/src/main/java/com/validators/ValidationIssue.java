package com.validators;

import java.util.Objects;

/**
 * Represents a validation issue found during rule schema validation.
 * Includes rule metadata, severity level (ERROR or WARN), and a descriptive message.
 */
public class ValidationIssue {
    private final String ruleName;
    private final String ruleType;
    private final String path;
    private final String level;  // "ERROR" | "WARN"
    private final String message;

    public ValidationIssue(String ruleName, String ruleType, String path, String level, String message) {
        this.ruleName = ruleName;
        this.ruleType = ruleType;
        this.path = path;
        this.level = level;
        this.message = message;
    }

    public String getRuleName() {
        return ruleName;
    }

    public String getRuleType() {
        return ruleType;
    }

    public String getPath() {
        return path;
    }

    public String getLevel() {
        return level;
    }

    public String getMessage() {
        return message;
    }

    public boolean isError() {
        return "ERROR".equals(level);
    }

    public boolean isWarning() {
        return "WARN".equals(level);
    }

    @Override
    public String toString() {
        return String.format("[%s] %s (%s) at %s: %s", 
            level, ruleName, ruleType, path, message);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValidationIssue that = (ValidationIssue) o;
        return Objects.equals(ruleName, that.ruleName) &&
               Objects.equals(ruleType, that.ruleType) &&
               Objects.equals(path, that.path) &&
               Objects.equals(level, that.level) &&
               Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ruleName, ruleType, path, level, message);
    }
}
