# 🧩 Deterministic Report Validator

This repository provides a **fully offline data validation pipeline** that verifies exported data files (e.g., from Redshift or on-prem warehouses) against a defined **rule base JSON**.  
It’s designed for environments with **restricted network access** or **sensitive data**, ensuring that **no data leaves the local system**.

---

## ⚙️ Overview

The validator can process both **small and large datasets**:

- For **small to medium files (< 1 GB)**, it uses **Pandas**.
- For **large files (> 1 GB)**, it automatically switches to **Apache Spark** for distributed processing.

It validates datasets based on a JSON rule base that defines:
- Expected field names and aliases  
- Severity levels, nullability, and uniqueness  
- Conditional dependencies or field relationships  

---

## 🧱 Core Components

### 1. `load_rules(path)`
Loads and parses the JSON rule base containing field definitions and validation relationships.

### 2. `identify_columns(df, rules)`
Maps the input data’s columns to rule definitions:
- Matches columns to rule-defined fields and aliases.
- Identifies unmatched columns.
- Extracts default rule attributes (severity, nullable, distinct).
- Extracts conditional validation rules (edges) from the JSON.

### 3. `exec_valid(spark, file_path, rules_path, delimiter, format, force)`
Executes validation using either Spark or Pandas depending on file size.  
- Cleans and standardizes column names.  
- Loads the rule base and identifies relevant fields.  
- Prepares and prints column mapping for validation.  

### 4. `main(argv)`
Handles command-line execution.  
- Detects the appropriate processing engine (Spark/Pandas).  
- Parses arguments and initializes validation.  
- Stops Spark sessions post-run.

---

## 🧮 JSON Rule Base Structure

The rule base JSON file should define:
```json
{
  "nodes": [
    {
      "id": "field_1",
      "field": "customer_code",
      "aliases": ["cust_cd", "client_code"],
      "severity": "high",
      "nullable": false,
      "distinct": true
    }
  ],
  "edges": [
    {
      "condition": "greater_than",
      "fields": [["amount_outstanding", "sanction_limit"]]
    }
  ]
}
```

- **nodes**: Describe field-level checks (nullable, distinct, etc.).  
- **edges**: Define relational checks between fields.  

---

## 🚀 Usage

### Command
```bash
python validator.py <file_path> <rules_path.json> [--delimiter <delimiter>] [--format <format>.xlsx] [--force]
```

### Arguments
| Argument | Description |
|-----------|-------------|
| `<file_path>` | Path to input data file (CSV or delimited text). |
| `<rules_path.json>` | Path to rule definition JSON. |
| `--delimiter` | Optional. File delimiter (default: `|`). |
| `--format` | Optional. Output format name for reports. |
| `--force` | Optional. Forces certain checks to skip. |

### Example
```bash
python validator.py data/export_2025_10_27.txt rules/validation_rules.json --delimiter "|" --format report.xlsx --force
```

---

## 🧠 Notes

- Designed for **deterministic**, repeatable validations.  
- All processing is done **locally** (no cloud or API dependency).  
- Works with **pipe-delimited (`|`)** and **comma-separated** text files.  
- Automatically handles **large datasets** via Spark fallback.

---

## 🧾 Output (Current Version)

- Prints mapped columns and conditional checks.
- Next iteration may include:
  - Validation reports in Excel/CSV.
  - Rule severity aggregation.
  - Null/duplicate profiling per field.

---

## 🧑‍💻 Author
Developed by **Sashank**, as part of the **Deterministic Data Validation Pipeline** project.
