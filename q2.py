from pyspark import SparkContext


def run():
    # TODO: Implement Question 2 here.
    # how the fuck do you import the data from q1?
    spark_context = SparkContext.getOrCreate()
    string_rdd_votes = spark_context.textFile("Votes_small.csv")
    a = string_rdd_votes.map(lambda line: line.split(","))
    get_last_col = (lambda x: x[4].split(';'))
    b = a.map(lambda s: s[:4] + [get_last_col(s)])
    c = b.map(lambda s: s + [len(s[4])])
    max_names = c.max(lambda s: s[5])[5]
    d = []
    for i in range(max_names):
        d.append(c.filter(lambda s: s[5] > i).map(lambda s: s[:4] + [s[4][i]]).persist())

    e = spark_context.union(d)
    for rdd in d:
        rdd.unpersist()
    # f = e.sortBy(lambda s: ','.join(s[:4]))
    # g = e.sortBy(lambda s: s[4])
    checkers = ['Churchill', 'Hannibal', 'Robespierre']
    for name in checkers:
        print('>> [q2: ' + name + ': ' + str(e.filter(lambda s: s[4] == name).count()) + ']')
