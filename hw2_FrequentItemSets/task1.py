from pyspark import SparkConf, SparkContext
import sys, itertools, collections, time
from operator import add
from math import ceil

# get all single items for a_priori Pass 1
def get_singleitem(baskets):
    singleitem = set()
    for b in baskets:
        for i in b:
            singleitem.add(frozenset({i}))
    return singleitem

def filter_non_frequent(baskets, candidates, threshold):
    candidates_num = len(candidates)
    if candidates_num == 0:
        return set()
    
    # count the frequency of candidate itemsets in 'candidates'
    candidates_itemsets_dict = collections.defaultdict(int)
    for b in baskets:
        for c in candidates:
            if b.issuperset(c):
                candidates_itemsets_dict[c] += 1
    #print("threshold"+str(threshold))
    #print(candidates_itemsets_dict)
    # filter those candidates that frequency less than threshold
    frequent_candidates_set = set()
    for c, v in candidates_itemsets_dict.items():
        if v >= threshold:
            frequent_candidates_set.add(c)
    
    return frequent_candidates_set


def construct_candidates(itemset, k):
    result = set()
    for c1 in itemset:
        for c2 in itemset:
            c = c1.union(c2)
            if (len(c) == k):
                result.add(c)
    return result

# filter those items of k-gram is frequent but, some of (k-1)-gram subsets is not frequent
def prune_non_frequent(candidates_set, prev_frequent_set, k):
    result = candidates_set.copy()
    for c in candidates_set:
        k_1_subset = itertools.combinations(c, k-1)
        #print("subset")
        #print(list(k_1_subset))
        for s in k_1_subset:
            # if one (k-1)-size subset is not frequent, that this k-size candidate cannot be frequent
            if frozenset(s) not in prev_frequent_set:
                result.remove(c)
                break

    return result


def a_priori(baskets, support_threshold):
    # {'single_candidate': count}
    C1 = get_singleitem(baskets)
    # {'single_frequent_item': count}
    L1 = filter_non_frequent(baskets, C1, support_threshold)
    
    result = dict()
    result[1] = L1

    L_curr = L1
    k = 2
    while L_curr:
        # generate candidate itemsets
        construct_set = construct_candidates(L_curr, k)
        # prune candidates itemsets which subsets are not frequent
        candidate_after_prune = prune_non_frequent(construct_set, L_curr, k)
        # filter candidates itemsets which are not frequent
        candidate_after_filter = filter_non_frequent(baskets, candidate_after_prune , support_threshold)
        L_curr = candidate_after_filter
        # update to result
        result[k] = L_curr
        k += 1
    

    return result


def write_outputfile(file, candidates):
    candidates.sort(key = lambda l: len(l))
    n = len(candidates)
    candidates_dict = collections.defaultdict(list)
    for i in range(n):
        nn = len(candidates[i])
        candidates_dict[nn].append(candidates[i])
    
    size = len(candidates_dict)
    for i in range(1, size+1):
        sub_candidates = sorted(list(map(lambda c: sorted(list(c)), candidates_dict[i])))
        if len(sub_candidates) > 0:
            if len(sub_candidates[0]) == 1:
                file.write(','.join("('{}')".format(str(sc[0])) for sc in sub_candidates) + '\n\n')
            else:
                file.write(','.join(str(tuple(sc)) for sc in sub_candidates) + '\n\n')

'''SON Pass 1'''
def SON_Pass1(iterator):
        sub_basket = list(iterator)
  
        sub_threshold = ceil((len(sub_basket) / baskets_num) * threshold)
       
        sub_result = a_priori(sub_basket, sub_threshold)
        # sub_result is dict(), {"itemset_size": itemsets}
        
        candidates_result = set()
        for size, itemsets in sub_result.items():
            for itemset in itemsets:
                candidates_result.add(itemset)
 
        return candidates_result
'''SON Pass 2'''
def SON_Pass2(iterator):
        counts = collections.defaultdict(int)
        sub_baskets = list(iterator)

        for basket in sub_baskets:
            for candidate in candidates:
                if basket.issuperset(candidate) :
                    counts[candidate] += 1
        
        return [(itemset, count) for itemset, count in counts.items()]


if __name__ == "__main__":

    start_time = time.time()

    case_number = int(sys.argv[1])
    threshold = int(sys.argv[2])
    input_filepath = sys.argv[3]
    output_filepath = sys.argv[4]

    conf = SparkConf()
    sc = SparkContext(conf = conf)
    sc.setLogLevel("ERROR")

    idRDD = sc.textFile(input_filepath).filter(lambda id: not id.startswith("user_id")).map(lambda id: id.split(',')) 
   
    # prepare the RDD used in Case 2 [business_id: [user_ids]]
    if case_number == 2:
        idRDD = idRDD.map(lambda kv: [kv[1], kv[0]])

    basketsRDD = idRDD.groupByKey().mapValues(set) \
        .map(lambda kv:  kv[1]).cache()
    baskets_num = basketsRDD.count()
    #SON Pass 1
    candidates = basketsRDD.mapPartitions(SON_Pass1).distinct().collect()

    # print candidates
    with open(output_filepath, 'w+') as f:
        f.write("Candidates:\n")
        write_outputfile(f, candidates)

    #SON Pass 2
    frequence_itemsets = basketsRDD.mapPartitions(SON_Pass2).reduceByKey(add) \
        .filter(lambda kv: kv[1] >= threshold) \
        .map(lambda kv: kv[0]).collect()

    # print frequent itemsets
    with open(output_filepath, 'a') as f:
        f.write("Frequent Itemsets:\n")
        write_outputfile(f, frequence_itemsets)

    end_time = time.time()
    print("Duration: {0:.4f}".format(end_time - start_time))
