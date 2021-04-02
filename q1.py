import os
from pyspark import SparkContext


def run():

    # Retrieve the MASTER environment variable, this variable is set by the script that handles your submission.
    master = os.getenv("MASTER")

    # TODO: Switch to the second definition of sparkConf for local execution.
    spark_context = SparkContext(master)
    # spark_context = SparkContext("local[*]")

    # TODO: Change the path up to Votes.csv for local execution.
    string_rdd_votes = spark_context.textFile("/Votes.csv")

    # TODO: Implement Question 1 here.
    RDDafterMap = string_rdd_votes.flatMap(lambda line: line.split(","))
    RDDfilterdate = RDDafterMap.filter(lambda x: x == "01-01-2012")
    q11 = RDDfilterdate.count()
    print(">> [q11: " + str(q11) + "]")

    # Q1.2
    split = string_rdd_votes.map(lambda line: line.split(","))
    rdd = split.map(lambda x: x[1:3])
    rdd2 = rdd.map(lambda k: (tuple(k), 1))
    rdd3 = rdd2.reduceByKey(lambda a, b: a + b)
    rdd4 = rdd3.filter(lambda x: x[1] > 9)
    q12 = rdd4.count()
    print(">> [q12: " + str(q12) + "]")

    # Q1.3
    split = string_rdd_votes.map(lambda line: line.split(","))
    rdd = split.map(lambda x: (x[0], x[3]))
    distinct = rdd.distinct()
    dateCounter = distinct.map(lambda k: (k[1], 1))
    dates = dateCounter.reduceByKey(lambda a, b: a + b)
    get_col = lambda a, b: a[b]
    maxDate = dates.map(lambda x: get_col(x, 1)).max()
    finalMap = dates.filter(lambda y: y[1] == q12)
    answer = finalMap.map(lambda x: get_col(x, 0)).collect()
    print(">> [q12: " + answer[0] + "]")
