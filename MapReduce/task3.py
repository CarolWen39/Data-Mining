import sys
import json
from time import time
from pyspark import SparkContext

BUSINESS_ID = 'business_id'
CITY = 'city'
STAR = 'stars'


# method 1
def load_file(file_path, key):
    records = open(file_path, encoding='utf8').readlines()
    return list(map(lambda r: {key[0]: json.loads(r)[key[0]],
                               key[1]: json.loads(r)[key[1]]}, records))


def group_by_business_id(dict_list):
    # 'business_id': ('star', 'count')
    grouped_dict = dict()
    for item in dict_list:
        if item[BUSINESS_ID] not in grouped_dict.keys():
            grouped_dict[item[BUSINESS_ID]] = (float(item[STAR]), 1)
        else:
            star_value = grouped_dict.get(item[BUSINESS_ID])[0] + item[STAR]
            star_count = grouped_dict.get(item[BUSINESS_ID])[1] + 1
            grouped_dict.update({item[BUSINESS_ID]: (star_value, star_count)})

    return grouped_dict

def merge_dict(business_city_dict):
    # 'business_id': ['city']
    merge_dict = dict()
    for item in business_city_dict:
        if item[BUSINESS_ID] not in merge_dict.keys():
            merge_dict[item[BUSINESS_ID]] = [item[CITY]]
        else:
            merge_dict[item[BUSINESS_ID]].append(item[CITY])
    return merge_dict

def left_join(id_star_dict, id_city_dict):
    left_joined_dict = dict()
    for id, stars in id_star_dict.items():
        ''' id = business_id
            stars = (total_star, total_star_count) '''
        if id_city_dict.get(id) is not None:
            for city in id_city_dict.get(id):
                if city not in left_joined_dict.keys():
                    left_joined_dict[city] = stars
                else:
                    star_value = left_joined_dict.get(city)[0] + stars[0]
                    star_count = left_joined_dict.get(city)[1] + stars[1]
                    left_joined_dict.update({city: (star_value, star_count)})

    return {k: float(v[0] / v[1]) for k, v in left_joined_dict.items()}


# method 2
def get_total_avg_star(business_star_rdd, business_city_rdd):
    joined_rdd = business_city_rdd.leftOuterJoin(business_star_rdd)

    new_rdd = joined_rdd \
        .map(lambda kvv: kvv[1]) \
        .filter(lambda kv: kv[1] is not None) \
        .groupByKey() \
        .map(lambda row: (row[0], sum(row[1]) / len(row[1]))) \
        .sortBy(lambda row:(-row[1], row[0])).collect()
    
    return new_rdd

if __name__ == '__main__':
    review_filepath = sys.argv[1]
    business_filepath = sys.argv[2]
    output_a_filepath = sys.argv[3]
    output_b_filepath = sys.argv[4]
    top_n = '10'

    ''' m1: Collect all the data, sort in python, and then print the first 10 cities '''
    begin1 = time()
    business_star_dict = load_file(review_filepath, [BUSINESS_ID, STAR]) # {'business_id': xxx, 'star': xxx}
    business_city_dict = load_file(business_filepath, [BUSINESS_ID, CITY]) # {'business_id': xxx, 'city': xxx}

    id_star_dict = group_by_business_id(business_star_dict)
    id_city_dict = merge_dict(business_city_dict)
    # left join two dictionaries on business_id
    joined_dict = left_join(id_star_dict, id_city_dict)

    result_A_python = sorted(joined_dict.items(), key=lambda kv: (-kv[1], kv[0]))
    print_b_python = result_A_python[:int(top_n)]
    print(print_b_python)
    end1 = time()

    ''' m2: Sort in Spark, take the first 10 cities, and then print these 10 cities '''
    begin2 = time()
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")
    review = sc.textFile(review_filepath).map(lambda r: json.loads(r))
    business = sc.textFile(business_filepath).map(lambda r: json.loads(r))

    business_star_rdd = review.map(lambda kv: (kv[BUSINESS_ID], kv[STAR]))
    business_city_rdd = business.map(lambda kv: (kv[BUSINESS_ID], kv[CITY]))
    
    result_a_spark = get_total_avg_star(business_star_rdd, business_city_rdd)
    print_b_spark = result_a_spark[:int(top_n)]
    print(print_b_spark)
    end2 = time()
    

    '''A. average stars for each city'''
    with open(output_a_filepath, 'w+') as a_file:
        a_file.write("city,stars\n")
        for i in range(len(result_a_spark)):
            a_file.write(result_a_spark[i][0]+','+str(result_a_spark[i][1])+'\n')

    '''B. compare the execution time of using two methods to print top 10 cities with highest stars.'''
    result_B = dict()
    result_B['m1'] = end1 - begin1
    result_B['m2'] = end2- begin2
    result_B['reason'] = "When the amount of data is not large, the running time of Spark and Python are similar,and even Spark will be slower. But when the amount of data is large enough, Spark using distributed computing will be much faster than Python."
    with open(output_b_filepath, 'w+') as b_file:
        json.dump(result_B, b_file, indent=2)

    