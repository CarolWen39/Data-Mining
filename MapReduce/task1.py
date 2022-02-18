import sys
from operator import add

from pyspark import SparkContext
import json
from datetime import datetime


# A. The total number of reviews
def get_total_num_review(review_ids_rdd):
    return review_ids_rdd.count()


#B. The number of reviews in certain year
def get_num_review_in_certain_year(review_date_rdd, year):
    return review_date_rdd.filter(lambda kv: datetime.strptime(
        kv[1], '%Y-%m-%d %H:%M:%S').year == year).count()


# C. The number of distinct users who have wrote reviews
def get_distinct_user(user_ids_rdd):
    return user_ids_rdd.distinct().count()


# D. Top m users who wrote the largest numbers of reviews and the number of reviews they wrote
def get_top_u_users(business_user_rdd, top_u):
    return business_user_rdd.map(lambda kv: (kv[1], 1)).reduceByKey(add). \
        takeOrdered(top_u, key=lambda kv: (-kv[1], kv[0]))


# E. The number of distinct businesses that have been reviewed
def get_distinct_businesses(businesses_ids_rdd):
    return businesses_ids_rdd.distinct().count()

# F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
def get_top_b_businesses(user_business_rdd, top_b):
    return user_business_rdd.map(lambda kv: (kv[1], 1)).reduceByKey(add). \
        takeOrdered(top_b, key=lambda kv: (-kv[1], kv[0]))



if __name__ == '__main__':
    review_filepath = sys.argv[1]
    output_filepath = sys.argv[2]
    year = 2018
    top_u = 10
    top_b = 10
    
    sc = SparkContext.getOrCreate()
    result_dict = dict()

    reviews = sc.textFile(review_filepath).map(lambda row: json.loads(row))

    review_ids_rdd = reviews.map(lambda kv: kv['review_id'])
    result_dict['n_review'] = get_total_num_review(review_ids_rdd)

    review_date_rdd = reviews.map(lambda kv: (kv['review_id'], kv['date']))
    result_dict['n_review_2018'] = get_num_review_in_certain_year(review_date_rdd, year)

    user_ids_rdd = reviews.map(lambda kv: kv['user_id'])
    result_dict['n_user'] = get_distinct_user(user_ids_rdd)

    business_user_rdd = reviews.map(lambda kv: (kv['business_id'], kv['user_id']))
    result_dict['top10_user'] = get_top_u_users(business_user_rdd, top_u)

    businesses_ids_rdd  = reviews.map(lambda kv: kv['business_id'])
    result_dict['n_business'] =get_distinct_businesses(businesses_ids_rdd)

    user_business_rdd = reviews.map(lambda kv: (kv['user_id'], kv['business_id']))
    result_dict['top10_business'] = get_top_b_businesses(user_business_rdd, top_b)

    #print(result_dict)

    with open(output_filepath, 'w+') as output_file:
        json.dump(result_dict, output_file, indent=2, separators=(',',':'))
    output_file.close()
