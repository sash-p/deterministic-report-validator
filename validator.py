import pandas as pd
import os
import sys
from pandas.io.formats import style
import datetime
import shutil
import fnmatch
from shutil import copyfile
from pyspark.sql.funcitions import *
from pyspark.sql.types import *
from pyspark.sql.utils import *
from pyspark.sql import SparkSession
import json
from jsonpath_ng import jsonpath, parse

def load_rules(path):
    print("Loading rule base ...")
    with open(path,"r'") as f:
        return json.loads(f)
    
def exec_valid(spark,file_path,rules_path,delimiter="|",format="",force=False):
    rules = load_rules(rules_path)
    fields={}
    aliases_fields={}

    if spark!='':
        df=spark.read.csv(file_path,sep=delimiter,header=True,inferSchema=True)

        for c in df.columns:
            df=df.withColumnRenamed(c,c.strip().replace(" ","_").replace("-","_"))

    else:
        df=pd.read_csv(file_path,sep=delimiter,low_memory=False)

    for col in df.columns:
        if col.lower() in fields.valies():
            print("Found node: ",col)

def main(argv):
    if len(argv)<3:
        print("Syntax: validator.py <file_path> <rules_path.json> [--delimiter <delimiter>] [--format <format>.xlsx] [--force]")
        sys.exit(1)

    file_name=argv[1]
    rules_path=argv[2]
    delimiter="|"
    format_fname=""
    force=False

    for i in range(3,len(argv)):
        if argv[i]=="--delimiter":
            delimiter=argv[i+1]
        elif argv[i]=="--format":
            format_fname=argv[i+1]
        elif argv[i]=="--force":
            force=True

    if((os.path.getsize(file_name)/1024/1024)>1000):
        print("File size greater than 1BG, using Spark for processing.")
        print("Initializing Spark session ...")

        spark = SparkSession.builder.appName("Deterministic Report Validator").getOrCreate()

    else:
        spark=''
        print("Initializing Pandas for processing ...")


    exec_valid(spark,file_name,rules_path,delimiter,format_fname,force)

    if spark!='':
        spark.stop()

if __name__ == "__main__":
    main(sys.argv)    
