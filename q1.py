import os
from pyspark import SparkContext


def run():

    # Retrieve the MASTER environment variable, this variable is set by the script that handles your submission.
    master = os.getenv('MASTER')

    # TODO: Switch to the second definition of sparkConf for local execution.
    spark_context = SparkContext(master)
    # spark_context = SparkContext("local[*]")

    # TODO: Change the path up to Votes.csv for local execution.
    string_rdd_votes = spark_context.textFile("/Votes.csv")

    # TODO: Implement Question 1 here.
    RDDafterMap = string_rdd_votes.flatMap(lambda line: line.split(","))
    RDDfilterdate = RDDafterMap.filter(lambda x: x == '01-01-2012')
    q11 = RDDfilterdate.count()
    return q11
