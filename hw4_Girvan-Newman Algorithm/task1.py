import sys, time, itertools
from graphframes import GraphFrame
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


if __name__ == '__main__':
    start = time.time()

    filter_threshold = int(sys.argv[1])
    input_file = sys.argv[2]
    output_file = sys.argv[3]

    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sparkSession = SparkSession(sc)
    sc.setLogLevel("ERROR")
    
    uid_bidxes_dict = sc.textFile(input_file).filter(lambda x: not x.startswith('user_id')) \
        .map(lambda x: x.split(',')).groupByKey() \
        .mapValues(lambda bids: sorted(list(bids))) \
        .collectAsMap()
    
    uid_pairs_candidates = list(itertools.combinations(list(uid_bidxes_dict.keys()), 2))
    edge_list, vertex_set = [], set()

    for pair in uid_pairs_candidates:
        if len(set(uid_bidxes_dict[pair[0]]).intersection(
                set(uid_bidxes_dict[pair[1]]))) >= filter_threshold:
            edge_list.append(tuple((pair[0], pair[1])))
            edge_list.append(tuple((pair[1], pair[0])))
            vertex_set.add(pair[0])
            vertex_set.add(pair[1])

    vertices = sc.parallelize(list(vertex_set)).map(lambda uid: (uid,)).toDF(['id'])
    edges = sc.parallelize(edge_list).toDF(["src", "dst"])
   
    graphframe = GraphFrame(vertices, edges)
    
    LabelPropagation = graphframe.labelPropagation(maxIter=5)

    # label_usridx: [usr_idx, community label]
    # label_usridx: {'community label': [user ids], ...}
    communities = LabelPropagation.rdd.coalesce(1) \
        .map(lambda kv: (kv[1], kv[0])) \
        .groupByKey().map(lambda label_usrids: sorted(list(label_usrids[1]))) \
        .sortBy(lambda usr_ids: (len(usr_ids), usr_ids)).collect()
    
    with open(output_file, 'w+') as f:
        for usr_id in communities:
            f.writelines(str(usr_id)[1:-1] + "\n")
        f.close()

    print("Duration: %d s." % (time.time() - start))