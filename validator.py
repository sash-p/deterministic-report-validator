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
    column_mapping = {}
    default_datatype = {}

    # Loop through rule nodes (field definitions)
    for node in rules["nodes"]:
        if node["field"].lower() not in fields:
            fields[node["field"].lower()] = node["id"]
            default_checks[node["id"]] = [node["severity"], node["nullable"], node["distinct"]]
            default_datatype[node["id"]] = node["dtype"]

        # Loop through aliases for each node and map them as well
        for alias in node["aliases"]:
            if alias.lower() not in aliases_fields:
                aliases_fields[alias.lower()] = node["id"]
                default_checks[node["id"]] = [node["severity"], node["nullable"], node["distinct"]]
                default_datatype[node["id"]] = node["dtype"]

    fields_to_check = {}   # Maps actual dataframe columns to rule IDs
    fields_not_found = []  # Tracks columns in data not found in rules

    # Match dataframe columns to rule-defined fields or aliases
    for col in df.columns:
        if col not in fields_to_check:
            if col in fields:
                fields_to_check[col] = fields[col] #fields_to_check[col] = node_id

                if default_datatype[fields[col]] == "varchar":
                    df[col] = df[col].astype(str)
                elif default_datatype[fields[col]] == "date":   
                    df[col] = df[col].astype('datetime64[ns]')
                elif default_datatype[fields[col]] == "float":
                    df[col] = df[col].astype(float)
                elif default_datatype[fields[col]] == "int":
                    df[col] = df[col].astype(int)

            elif col in aliases_fields:
                fields_to_check[col] = aliases_fields[col] #fields_to_check[col] = node_id 

                if default_datatype[aliases_fields[col]] == "varchar":
                    df[col] = df[col].astype(str)
                elif default_datatype[aliases_fields[col]] == "date":   
                    df[col] = df[col].astype('datetime64[ns]')
                elif default_datatype[aliases_fields[col]] == "float":
                    df[col] = df[col].astype(float)
                elif default_datatype[aliases_fields[col]] == "int":
                    df[col] = df[col].astype(int)
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
            df = df.withColumnRenamed(c, c.strip().lower())
    else:
        df = pd.read_csv(file_path, sep=delimiter, low_memory=False, dtype=object)
        df.columns = [col.strip().lower().replace(' ','_') for col in df.columns]

    # Load rule definitions and identify relevant columns
    rules = load_rules(rules_path)
    cols_found, cols_notfound, field_def, col_checks = identify_columns(df, rules)

    # Apply column-based checks
    operators = ['+', '-', '*', '/', '%', '>', '<', '>=', '<=', '==', '!=']
    col_checks_results = {}
    print(col_checks)
    for chk_condition in col_checks:
        col_checks_results[chk_condition] = []

        for field_in_chk_cond in col_checks[chk_condition]:
            condition_break=chk_condition.strip().split(' ')
            conditional_eqn=" "

            if len(field_in_chk_cond) > 1:
                if len(condition_break) == 3:
                    condition_break[0] = cols_found[field_in_chk_cond[0]]
                    condition_break[2] = cols_found[field_in_chk_cond[1]]
                    conditional_eqn = " ".join(condition_break)
                else:
                    print("Complex condition not supported yet.")
                    continue
            else:
                for i in range(len(condition_break)):
                    if i==0:
                        condition_break[0] = cols_found[field_in_chk_cond[0]]
                    else:
                        if i%2==0:
                            condition_break[i] = cols_found[field_in_chk_cond[i-1]]
                        else:
                            if i not in operators:
                                conditional_eqn = conditional_eqn[:-2]
                                i+=1
                            else:
                                conditional_eqn += condition_break[i]

            if conditional_eqn != " ":
                col_checks_results[chk_condition].append(len(df[~df.eval(conditional_eqn)]))

        else:
            op_flag = "N"
            for op in range(len(operators)):
                if condition_break[op] in operators:
                    conditional_eqn=f"{cols_found[field_in_chk_cond[0]]} {chk_condition.replace('field1','').replace('len()','str.len()')}"
                    n_conditional_eqn=f"not({conditional_eqn})"
                    op_flag = "Y"
                    
            if op_flag == "Y":
                col_checks_results[chk_condition].append(len(df.eval(n_conditional_eqn)))
                
    print(col_checks_results)
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
        print("File size greater than 1GB, using Spark for processing.")
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
