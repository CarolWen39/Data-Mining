import sys, time, copy, random, collections, math, itertools
from sklearn.cluster import KMeans
import numpy as np

def update_discard_set(labels, random_load, dimensions_idx_dict, discard_set):
    """
    update discard set
    """
    for label, point in zip(labels, random_load):
        if label not in discard_set:
            discard_set[label] = dict()
            discard_set[label]['N'] = [dimensions_idx_dict[tuple(point)]]
            discard_set[label]['SUM'] = point
            discard_set[label]['SUMSQ'] = point ** 2
        else:
            discard_set[label]['N'].append(dimensions_idx_dict[tuple(point)])
            discard_set[label]['SUM'] += point
            discard_set[label]['SUMSQ'] += point ** 2

def update_compression_set(labels, retain_set, dimensions_idx_dict, compression_set, RS):
    """
    update compression set
    """
    for label, point in zip(labels, retain_set):
        if label not in RS:
            if label not in compression_set:
                compression_set[label] = dict()
                compression_set[label]['N'] = [dimensions_idx_dict[tuple(point)]]
                compression_set[label]['SUM'] = point
                compression_set[label]['SUMSQ'] = point ** 2
            else:
                compression_set[label]['N'].append(dimensions_idx_dict[tuple(point)])
                compression_set[label]['SUM'] += point
                compression_set[label]['SUMSQ'] += point ** 2

def eachrun_output(run_idx, DS, CS, RS):
    """
    return information after each run
    """
    ds_cnt, cs_cnt, rs_cnt = 0, 0, 0
    for label in DS:
        ds_cnt += len(DS[label]['N'])
    for label in CS:
        cs_cnt += len(CS[label]['N'])
    rs_cnt = len(RS)
    return "Round " + str(run_idx) + ": " + str(ds_cnt) + "," + str(len(CS)) + "," + str(cs_cnt) + "," + str(rs_cnt) + "\n"

def maha_dis_point_cluster(point, cluster):
    """
    Calculate Mahalanobis distance between a point and a cluster
    """
    centroid = cluster['SUM'] / len(cluster['N'])
    sigma = cluster['SUMSQ'] / len(cluster['N']) - (cluster['SUM']/len(cluster['N']))**2
    z = (point - centroid) / sigma
    return math.sqrt(np.dot(z, z))

def maha_dis_cluster_cluster(cluster1, cluster2):
    """
    Calculate Mahalanobis distance between a cluster and another
    """
    centroid1 = cluster1['SUM'] / len(cluster1['N'])
    centroid2 = cluster2['SUM'] / len(cluster2['N'])
    sigma1 = cluster1['SUMSQ'] / len(cluster1['N']) - (cluster1['SUM']/len(cluster1['N']))**2
    sigma2 = cluster2['SUMSQ'] / len(cluster2['N']) - (cluster2['SUM']/len(cluster2['N']))**2
    z1 = (centroid1 - centroid2) / sigma1
    z2 = (centroid1 - centroid2) / sigma2
    return min(math.sqrt(np.dot(z1, z1)), math.sqrt(np.dot(z2, z2)))

def write_file(output, discard_set, compression_set, retain_set, output_file, dimensions_idx_dict):
    print(dimensions_idx_dict)
    output += '\nThe clustering results:\n'
    for cluster in discard_set:
        discard_set[cluster]["N"] = set(discard_set[cluster]["N"])
    if compression_set:
        for cluster in compression_set:
            compression_set[cluster]["N"] = set(compression_set[cluster]["N"])

    RS_set = set()
    for point in retain_set:
        RS_set.add(dimensions_idx_dict[tuple(point)])

    for point in range(len(idx_dimensions_dict)):
        if point in RS_set:
            output += str(point) + ",-1\n"
        else:
            for cluster in discard_set:
                if point in discard_set[cluster]["N"]:
                    output += str(point) + "," + str(cluster) + "\n"
                    break
            for cluster in compression_set:
                if point in compression_set[cluster]["N"]:
                    output += str(point) + ",-1\n"
                    break

    with open(output_file, "w") as out_file:
        out_file.writelines(output)

if __name__ == '__main__':
    input_file = sys.argv[1]
    n_cluster = int(sys.argv[2])
    output_file = sys.argv[3]

    start_time = time.time()

    discard_set, compression_set, retain_set = dict(), dict(), []

    with open(input_file) as file:
        data = file.readlines()
    # list of list: [[], [], ...]
    data = list(map(lambda x: x.strip('\n').split(','), data))
    # dictionary: {idx: dimensions, ...}
    idx_dimensions_dict = dict([(int(l[0]), tuple(list(map(lambda x:float(x), l[2:])))) for l in data])
    n_len = len(data)
    # {dimensions: idx}
    dimensions_idx_dict = dict(zip(list(idx_dimensions_dict.values()), list(idx_dimensions_dict.keys())))

    data = list(map(lambda x: np.array(x), list(idx_dimensions_dict.values())))
    random.shuffle(data)

    ''' step 1. load 20% data randomly ''' 
    sub_len = round(n_len/5)
    random_load = data[0: sub_len]

    ''' step 2. K-means '''
    kmeans_2 = KMeans(n_clusters=n_cluster * 25).fit(random_load)

    ''' step 3. move all the clusters that contain only one point to RS (outliers) '''
    # {cluster_id: number of points in this cluster}
    cluster_dict = collections.defaultdict(int)
    for label in kmeans_2.labels_:
        cluster_dict[label] += 1
    RS_index = []
    for label in cluster_dict:
        if cluster_dict[label] < 20:
            RS_index += [i for i, x in enumerate(kmeans_2.labels_) if x == label]
    
    # add isolated points into RS
    for idx in RS_index:
        retain_set.append(random_load[idx])
    
    # delete points from random load data
    for index in reversed(sorted(RS_index)):
        random_load.pop(index)

    ''' Step 4. Run K-Means to the rest of the data points with K = the number of input clusters '''
    kmeans_4 = KMeans(n_clusters=n_cluster).fit(random_load)

    ''' Step 5. Use the K-Means result from Step 4 to generate the DS clusters '''
    update_discard_set(kmeans_4.labels_, random_load, dimensions_idx_dict, discard_set)
    print(discard_set.keys())

    ''' Step 6. Run K-Means on the points in the RS with a large K to generate CS and RS '''
    if len(retain_set) > 1:
        kmeans_6 = KMeans(n_clusters=len(retain_set)-1).fit(retain_set)
    else:
        kmeans_6 = KMeans(n_clusters=len(retain_set)).fit(retain_set)
    cluster_dict_6 = collections.defaultdict(int)
    for label in kmeans_6.labels_:
        cluster_dict_6[label] += 1
    # label that only contain one point
    RS_6 = []
    for label in cluster_dict_6:
        if cluster_dict_6[label] == 1:
            RS_6.append(label)
    
    
    RS_index = []
    if RS_6:
        for key in RS_6:
            RS_index.append(list(kmeans_6.labels_).index(key)) 
    
    # generate compression set
    update_compression_set(kmeans_6.labels_, retain_set, dimensions_idx_dict, compression_set, RS_6)
    
    # update retain set
    new_retain_set = []
    for idx in reversed(sorted(RS_index)):
        new_retain_set.append(retain_set[idx])
    retain_set = copy.deepcopy(new_retain_set)
    
    output = 'The intermediate results:\n'
    output += eachrun_output(1, discard_set, compression_set, retain_set)
    print(output)
    
    for run_id in range(2, 6):
        ''' Step 7. Load another 20% of the data randomly '''
        if run_id < 5:
            random_load = data[sub_len*(run_id-1): sub_len*run_id]
        else:
            random_load = data[sub_len*(run_id-1): ]

        ''' Step 8. compare them to each of the DS using the Mahalanobis Distance and 
                    assign to the nearest DS cluster '''
        ds_index = set()
        for i in range(len(random_load)):
                point = random_load[i]
                min_dis = math.inf
                min_cluster = -1
                for cluster in discard_set:
                    m_distance = maha_dis_point_cluster(point, discard_set[cluster])
                    if min_dis > m_distance:
                        min_dis = m_distance 
                        min_cluster = cluster
                
                if min_dis < 2 * math.sqrt(len(point)):
                    # add point to DS
                    discard_set[min_cluster]["N"].append(dimensions_idx_dict[tuple(point)])
                    discard_set[min_cluster]["SUM"] += point
                    discard_set[min_cluster]["SUMSQ"] += point ** 2
                    # add index to DS_index
                    ds_index.add(i)
        
        ''' Step 9. For the new points that are not assigned to DS clusters, using the Mahalanobis Distance and
                    assign the points to the nearest CS clusters'''
        cs_index = set()
        for i in range(len(random_load)):
            if i not in ds_index:
                point = random_load[i]
                min_dis = math.inf
                min_cluster = -1
                for cluster in compression_set:
                    m_distance = maha_dis_point_cluster(point, discard_set[cluster])
                    if min_dis > m_distance:
                        min_dis = m_distance 
                        min_cluster = cluster
                
                if min_dis < 2 * math.sqrt(len(point)):
                    # add point to CS
                    compression_set[min_cluster]["N"].append(dimensions_idx_dict[tuple(point)])
                    compression_set[min_cluster]["SUM"] += point
                    compression_set[min_cluster]["SUMSQ"] += point ** 2
                    # add index to CS_index
                    ds_index.add(i)

        ''' Step 10. For the new points that are not assigned to a DS cluster or a CS cluster, assign them to RS '''
        for i in range(len(random_load)):
            if i not in ds_index and i not in cs_index:
                retain_set.append(random_load[i])

        ''' Step 11. Run K-Means on the RS with a large K (e.g., 5 times of the number of the input clusters) 
                     to generate CS (clusters with more than one points) and RS '''
        if len(retain_set) > 1:
            kmeans_11 = KMeans(n_clusters=len(retain_set)-1).fit(retain_set)
        else:
            kmeans_11 = KMeans(n_clusters=len(retain_set)).fit(retain_set)
        CS_cluster_set = set(compression_set.keys())
        RS_cluster_set = set(kmeans_11.labels_)
        intersection = CS_cluster_set.intersection(RS_cluster_set)
        union = CS_cluster_set.union(RS_cluster_set)
        # to avoid adding points to cluster already exists in CS, must change the duplicate labels
        change_dict = dict()
        for ii in intersection:
            while True:
                random_int = random.randint(100, len(random_load))
                if random_int not in union:
                    break
            change_dict[ii] = random_int
            union.add(random_int)
        # get the new k-means labels
        labels = list(kmeans_11.labels_)
       
        for i in range(len(labels)):
            if labels[i] in change_dict:
                labels[i] = change_dict[labels[i]]
        
        cluster_dict_11 = collections.defaultdict(int)
        for label in labels:
            cluster_dict_11[label] += 1

        RS_11 = []
        for label in cluster_dict_11:
            if cluster_dict_11[label] == 1:
                RS_11.append(label)

        RS_index = []
        if RS_11:
            for key in RS_11:
                RS_index.append(labels.index(key)) 
        update_compression_set(labels, retain_set, dimensions_idx_dict, compression_set, RS_11)
        
        new_retain_set = []
        for idx in reversed(sorted(RS_index)):
            new_retain_set.append(retain_set[idx])
        retain_set = copy.deepcopy(new_retain_set)
     
        ''' Step12. Merge CS clusters that have a Mahalanobis Distance '''
        flag = True
        while True:
            cluster_tuple = list(itertools.combinations(list(compression_set.keys()), 2))
            original_cluster = set(compression_set.keys())
            merge_list = []
            for c1, c2 in cluster_tuple:
                m_distance = maha_dis_cluster_cluster(compression_set[c1], compression_set[c2])
                if m_distance < 2 * math.sqrt(len(compression_set[c1]['SUM'])):
                    compression_set[c1]['N'] = compression_set[c1]['N'] + compression_set[c2]['N']
                    compression_set[c1]['SUM'] += compression_set[c2]['SUM']
                    compression_set[c1]['SUMSQ'] += compression_set[c2]['SUMSQ']
                    compression_set.pop(c2)
                    flag = False
                    break
            new_cluster = set(compression_set.keys())
            # no clusters to merge
            if new_cluster == original_cluster:
                break

        ''' the last run (after the last chunk of data), merge CS clusters with DS clusters '''
        if run_id == 5:
            cs_cluster = list(compression_set.keys())
            if not compression_set:
                for cluster_cs in cs_cluster:
                    distance_dict = dict()
                    min_distance = math.inf
                    cluster = -1
                    for cluster_ds in discard_set:
                        m_distance = maha_dis_cluster_cluster(discard_set[cluster_ds], compression_set[cluster_cs])
                        if m_distance < min_distance:
                            min_distance = m_distance
                            cluster = cluster_ds

                    if m_distance < 2 * math.sqrt(len(compression_set[cluster_cs]['SUM'])):
                        discard_set[cluster]['N'] = discard_set[cluster]['N'] + compression_set[cluster_cs]['N']
                        discard_set[cluster]['SUM'] += compression_set[cluster_cs]['SUM']
                        discard_set[cluster]['SUMSQ'] += compression_set[cluster_cs]['SUMSQ']
                        compression_set.pop(cluster_cs)

        output += eachrun_output(run_id, discard_set, compression_set, retain_set)

    # write file
    write_file(output, discard_set, compression_set, retain_set, output_file, dimensions_idx_dict)
    
    print(f"Duration: {time.time() - start_time}")