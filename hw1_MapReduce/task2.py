import json
import sys
from time import *
from operator import add
from pyspark import SparkContext

# F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
def get_top_b_businesses(user_business_rdd, top_b):
    return user_business_rdd.map(lambda kv: (kv[1], 1)).reduceByKey(add). \
        takeOrdered(top_b, key=lambda kv: (-kv[1], kv[0]))

def review_partitioner(key):
    return ord(key[:1])

# default:
def defualt_test(review_filepath):
    sc = SparkContext.getOrCreate()
    begin_time1 = time()
    reviews = sc.textFile(review_filepath).map(lambda row: json.loads(row))
    business_default_rdd = reviews.map(lambda kv: (kv['business_id'], 1))
    
    default_dict = dict()
    #business_default_rdd = business_default_rdd.partitionBy(int(n_partition))

    default_dict['n_partition'] = business_default_rdd.getNumPartitions()
    default_dict['n_items'] = business_default_rdd.glom().map(len).collect()

    
    get_top_b_businesses(business_default_rdd, top_b)
    end_time1 = time()
    default_dict['exe_time'] = end_time1 - begin_time1

    
    return default_dict

# customized:
def customized_test(review_filepath):
    sc = SparkContext.getOrCreate()
    begin_time2 = time()
    reviews = sc.textFile(review_filepath).map(lambda row: json.loads(row))
    business_customized_rdd = reviews.map(lambda kv: (kv['business_id'], 1))

    customized_dict = dict()
    business_customized_rdd = business_customized_rdd.partitionBy(int(n_partition), review_partitioner)

    customized_dict['n_partition'] = business_customized_rdd.getNumPartitions()
    customized_dict['n_items'] = business_customized_rdd.glom().map(len).collect()

    
    get_top_b_businesses(business_customized_rdd, top_b)
    end_time2 = time()
    customized_dict['exe_time'] = end_time2 - begin_time2

    return customized_dict

if __name__ == '__main__':

    review_filepath = sys.argv[1]
    output_filepath = sys.argv[2]
    n_partition = sys.argv[3]
    top_b = 10

    result_dict = dict()
    result_dict['default'] =  defualt_test(review_filepath)
    result_dict['customized'] = customized_test(review_filepath)

    #print(result_dict)
    with open(output_filepath, 'w+') as output_file:
        json.dump(result_dict, output_file, indent=2)
    output_file.close()