import pandas as pd
import os
import sys
import datetime
import shutil
import fnmatch
from shutil import copyfile
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.utils import *
from pyspark.sql import SparkSession
import json


def load_rules(path):
    # Load validation rules from JSON file
    print('Loading rule base....')
    with open(path, "r") as f:
        return json.load(f)


def identify_columns(df, rules):
    # Maps field name → rule ID
    fields = {}
    # Maps alias name → rule ID
    aliases_fields = {}
    # Stores default rule attributes (severity, nullable, distinct)
    default_checks = {}

    # Loop through rule nodes (field definitions)
    for node in rules["nodes"]:
        if node["field"].lower() not in fields:
            fields[node["field"].lower()] = node["id"]
            default_checks[node["id"]] = [node["severity"], node["nullable"], node["distinct"]]

        # Loop through aliases for each node and map them as well
        for alias in node["aliases"]:
            if alias.lower() not in aliases_fields:
                aliases_fields[alias.lower()] = node["id"]
                default_checks[node["id"]] = [node["severity"], node["nullable"], node["distinct"]]

    fields_to_check = {}   # Maps actual dataframe columns to rule IDs
    fields_not_found = []  # Tracks columns in data not found in rules

    # Match dataframe columns to rule-defined fields or aliases
    for col in df.columns:
        if col.lower() not in fields_to_check:
            if col.lower() in fields:
                fields_to_check[col.lower()] = fields[col.lower()]
            elif col.lower() in aliases_fields:
                fields_to_check[col.lower()] = aliases_fields[col.lower()]
            else:
                fields_not_found.append(col.lower())
        else:
            pass
            # print("Field already present", col)

    # Process conditional validation rules (edges)
    conditional_checks = {}
    for edge in rules["edges"]:
        for edge_field_pairs in edge["fields"]:
            # Case: single-field rule
            if len(edge_field_pairs) == 1 and edge_field_pairs[0] in fields_to_check.values():
                if edge["condition"] not in conditional_checks:
                    conditional_checks[edge["condition"]] = [[edge_field_pairs[0]]]
                else:
                    conditional_checks[edge["condition"]].append([edge_field_pairs[0]])

            # Case: rule involving two fields (relationship/condition)
            elif len(edge_field_pairs) > 1 and edge_field_pairs[0] in fields_to_check.values() and edge_field_pairs[1] in fields_to_check.values():
                if edge["condition"] not in conditional_checks:
                    conditional_checks[edge["condition"]] = [[edge_field_pairs[0], edge_field_pairs[1]]]
                else:
                    conditional_checks[edge["condition"]].append([edge_field_pairs[0], edge_field_pairs[1]])

    # Reverse mapping for reference (rule ID → column name)
    fields_to_check_rev_map = {value: key for key, value in fields_to_check.items()}

    return fields_to_check_rev_map, fields_not_found, default_checks, conditional_checks


def exec_valid(spark, file_path, rules_path, delimiter=",", format="", force=False):
    # Read data using Spark or Pandas depending on context
    if spark != "":
        df = spark.read.csv(file_path, sep=delimiter, header=True, inferSchema=True)
        # Clean column names
        for c in df.columns:
            df = df.withColumnRenamed(c, c.strip())
    else:
        df = pd.read_csv(file_path, sep=delimiter, low_memory=False)
        df.columns = df.columns.str.strip()
        df.columns = df.columns.str.replace(' ', '_')

    # Load rule definitions and identify relevant columns
    rules = load_rules(rules_path)
    cols_found, cols_notfound, field_def, col_checks = identify_columns(df, rules)

    # Apply column-based checks
    for chk_condition in col_checks:
        for col_chks in col_checks[chk_condition]:
            if len(col_chks) > 1:
                print(cols_found[col_chks[0]], cols_found[col_chks[1]])
            else:
                print(cols_found[col_chks[0]])


def main(argv):
    # Validate syntax
    if len(argv) < 3:
        print("Syntax: validator.py <file_path> <rules_path.json> [--delimiter <delimiter>] [--format <format>.xlsx] [--force]")
        sys.exit(1)

    # Parse command-line arguments
    file_name = argv[1]
    rules_path = argv[2]
    delimiter = "|"
    format_fname = ""
    force = False

    for i in range(3, len(argv)):
        if argv[i] == "--delimiter":
            delimiter = argv[i+1]
        elif argv[i] == "--format":
            format_fname = argv[i+1]
        elif argv[i] == "--force":
            force = True

    # Choose Spark if file is > 1GB, otherwise Pandas
    if ((os.path.getsize(file_name)/1024/1024) > 1000):
        print("File size greater than 1BG, using Spark for processing.")
        print("Initializing Spark session ...")
        spark = SparkSession.builder.appName("Deterministic Report Validator").getOrCreate()
    else:
        spark = ''
        print("Initializing Pandas for processing ...")

    # Execute validation
    exec_valid(spark, file_name, rules_path, delimiter, format_fname, force)

    # Stop Spark if started
    if spark != '':
        spark.stop()


if __name__ == "__main__":
    main(sys.argv)
