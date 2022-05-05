import sys, time, itertools, collections, random

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


class MyGraphFrame:
    def __init__(self, vertices: list, edges: dict):
        self.vertices = vertices
        self.edges = edges

        self.betweenness_dict = dict()
        self.sorted_betweenness_list = None
        # store the credits/contribution 
        self.vertex_credit_dict = dict()
        self.__init_credit_dict__()

        self.__count_edges()
        self.best_communities = None

    def __init_credit_dict__(self):
        # each node is leaf node at the algorithm begins
        [self.vertex_credit_dict.setdefault(vertex, 1.0) for vertex in self.vertices]
    
    def __count_edges(self):
        """
        private Funtion, count the total number of edges, and a set of edges, sorted by usr_id
        """
        edge_conut = 0
        edges_set = set()
        for n1, n2 in edges.items():
            for n in n2:
                node_pair = (n1, n) if n1 < n else (n, n1)
                if node_pair not in edges_set:
                    edges_set.add(node_pair)
                    edge_conut += 1
        
        self.m = edge_conut
        self.edges_set = edges_set

        # with open('mxtrix_my', 'w+') as f:
        #     f.writelines(str(self.m)+'\n')
        #     for betweenes_tuple in sorted(self.adajcency_matrix, key=lambda k:k[0]):
        #         f.writelines(str(betweenes_tuple) + '\n')
        #     f.close()

    def _bfs_tree(self, root):
        """
        Protected Funtion, build a tree given certain root
        return:  {'xxxxxx': (0, []),
                  'aaaaaa': (1, ['xxxxx']),
                  'bbbbbb': (1, ['xxxxx']),
                  '123111': (2, ['aaaaa', 'bbbbb'])}
        """
        bfs_tree = dict()
        # tuple (level, parent)
        bfs_tree[root] = (0, [])

        visited_node = set()
        # use queue to store nodes need to visit, break the while loop until there is no elements in queue
        node_queue = collections.deque([root])

        while node_queue:
            current_node = node_queue.popleft()
            visited_node.add(current_node)
            for child in self.edges[current_node]:
                child_level = bfs_tree[current_node][0] + 1
                # this child has not been visited
                if child not in visited_node:
                    visited_node.add(child)
                    bfs_tree[child] = (child_level, [current_node])
                    node_queue.append(child)
                # this child has been visited and existed child parents are the same level as current node
                elif child_level == bfs_tree[child][0]:
                    bfs_tree[child][1].append(current_node)

        return {k: v for k, v in sorted(bfs_tree.items(), key=lambda kv: -kv[1][0])}

    def _shortest_path(self, bfs_tree):
        """
        Protected Funtion, traverse this bfs_tree and get each children shortest path from root node
        Parameter: {'JM0GL6Dx4EuZ1mprLk5Gyg': (2, ['MtdSCXtmrSxj_uZOJ5ZycQ']), ....}
        return: the number of shortest path that each node has 
            {'9xM8upr_n9jchUDKxqSGHw': 6, 'JteQGisqOf_pklq7GA0Rww': 2, ...}
        """
        # level_dict: {1: [(), (), ...]}
        # tuples above are (child_node, [parent_nodes]), which means parent_nodes need KEY steps to get child_node
        level_dict = dict()
        shortest_path_dict = dict()

        for child_node, level_parents in bfs_tree.items():
            level_dict.setdefault(level_parents[0], []).append((child_node, level_parents[1]))

        # level_dict.keys(): dict_keys([9, 8, 7, 6, 5, 4, 3, 2, 1, 0])
        # loop from 0 to the max level to avoid the situation of parent not in shortest_path_dict
        for level_key in range(0, len(level_dict.keys())):
            for (child_node, parent_node_list) in level_dict[level_key]:
                if len(parent_node_list) > 0:
                    shortest_path_dict[child_node] = sum([shortest_path_dict[parent]
                                                          for parent in parent_node_list])
                else:
                    # leaf node set to 1
                    shortest_path_dict[child_node] = 1

        return shortest_path_dict

    def _credit_calculation(self, bfs_tree, shortest_path_dict):
        """
        Protected Funtion, traverse this bfs_tree and calculate credits/contributions for each node as root
        Parameter: 
            bfs_tree: return value from _bfs_tree
            shortest_path_dict: return value from _shortest_path
        return: 
        """
        credict_dict = self.vertex_credit_dict.copy()
        credict_result_dict = dict()
        for child, level_parents in bfs_tree.items():
            if len(level_parents[1]) > 0:
                # distribute credit of a node to its edges depends on number of shortest paths
                shortest_path_num = sum([shortest_path_dict[parent] for parent in level_parents[1]])
                for parent in level_parents[1]:
                    # sort in each pair
                    user_pair = (child, parent) if child < parent else (parent, child)
                    # the credit of each child_parent pair are 
                    credit = float(float(credict_dict[child]) * int(shortest_path_dict[parent]) / shortest_path_num)
                    credict_result_dict[user_pair] = credit
                    # update every parent node weight
                    credict_dict[parent] += credit

        return credict_result_dict

    def compute_betweennes(self):
        """
        compute betweenness of each user pair
        :return: list of tuple(user_pair, float)
                [(('cyuDrrG5eEK-TZI867MUPA', 'l-1cva9rA8_ugLrtSdKAqA'),4234.0), ... 
        """
        self.betweenness_dict = dict()
        for node in self.vertices:
            # for each node, build its bfs tree
            bfs_tree = self._bfs_tree(root=node)
            # traverse this bfs tree, and find the shorted path from this node to its connected nodes
            shortest_path_dict = self._shortest_path(bfs_tree)
            # traverse this bfs tree, and calculate credits/contributions
            contribution_dict = self._credit_calculation(bfs_tree, shortest_path_dict)
            
            for key, value in contribution_dict.items():
                if key in self.betweenness_dict.keys():
                    self.betweenness_dict[key] += value
                else:
                    self.betweenness_dict[key] = value
        
        # divide by 2 to get true betweenness
        self.betweenness_dict = dict(map(lambda kv: (kv[0], float(kv[1] / 2)),
                     self.betweenness_dict.items()))
        # sort
        self.sorted_betweenness_list = sorted(
            self.betweenness_dict.items(), key=lambda kv: (-kv[1], kv[0][0]))
        
        return self.sorted_betweenness_list        
    
    def _detect_communities(self):
        """
        Protected Funtion, detect communities based on self.edge, using bfs to find the connected sub graph
        return: a list of set() which contain communities
        """
        communities = []

        visited_node = set()
        # use queue to store nodes need to visit, break the while loop until there is no elements in queue
        node_queue = collections.deque()
        # store the nodes which are in the same communities, det to set() when each inner loop end
        one_community = set()

        # random pick a root to detect communities
        root = self.vertices[random.randint(0, len(self.vertices) - 1)]
        node_queue.append(root)
        one_community.add(root)

        while len(visited_node) < len(self.vertices):
            
            while node_queue:
                current_node = node_queue.popleft()
                visited_node.add(current_node)
                one_community.add(current_node)

                for child in self.edges[current_node]:
                    if child not in visited_node:
                        visited_node.add(child)
                        one_community.add(child)
                        node_queue.append(child)
            
            # store this one community into communities
            communities.append(sorted(one_community))
            one_community = set()
            # select one node from unvisited nodes and begin a new inner loop
            if len(visited_node) < len(self.vertices):
                node_queue.append(set(self.vertices).difference(visited_node).pop())

        return communities

    def _modularity(self):
        """
        Protected Funtion, compute modularity of partitioning S of graph G
        return: the current community and its modularity
        """
        # find the communities in this graph
        current_communities = self._detect_communities()
        modularity_sum = 0.0
        for community in current_communities:
            for node_pair in itertools.combinations(list(community), 2):
                node_pair = (node_pair[0], node_pair[1]) if node_pair[0] < node_pair[1] else (node_pair[1], node_pair[0])
                # degree of node i and j in each node_pair
                ki = len(self.edges[node_pair[0]])
                kj = len(self.edges[node_pair[1]])
                Aij = 1.0 if node_pair in self.edges_set else 0.0
                modularity_sum += Aij - ki * kj / (2 * self.m)

        return current_communities, modularity_sum / (2 * self.m)

    def _remove_highest_betweeness(self, betweeness):
        """
        Protected Funtion, roremove the highest betweeness(edges) in the graph and also delete it from self.edges
        Parameter: list of tuple [(('WXlxViTwXHPBvhioljN9PQ', 'ae7zi8F0B6l_JCITh1mXDg'), 1.0), ...]
        """
        highest_edge_pair = betweeness[0][0]
        if self.edges[highest_edge_pair[0]] is not None and highest_edge_pair[1] in self.edges[highest_edge_pair[0]]:
            self.edges[highest_edge_pair[0]].remove(highest_edge_pair[1])

        if self.edges[highest_edge_pair[1]] is not None and highest_edge_pair[0] in self.edges[highest_edge_pair[1]]:
            self.edges[highest_edge_pair[1]].remove(highest_edge_pair[0])

    def compute_communities(self):
        """
        detect the communities iteratively by running Girvan-Newman
        return: a list of set() which contain communities
        """
        # modularity belongs to [-1, 1]
        self._remove_highest_betweeness(self.sorted_betweenness_list)
        self.best_communities, max_modularity = self._modularity()
        self.sorted_betweenness_list = self.compute_betweennes()

        # repeat delete highest edges and recalculate betweenness of edges until modularity has not larger than before
        while 1:
            self._remove_highest_betweeness(self.sorted_betweenness_list)
            current_communities, current_modularity = self._modularity()
            self.sorted_betweenness_list = self.compute_betweennes()
            if max_modularity < current_modularity:
                self.best_communities = current_communities
                max_modularity = current_modularity
            else:
                break
        
        return sorted(self.best_communities, key=lambda kv: (len(kv), kv[0], kv[1]))

if __name__ == '__main__':
    start = time.time()

    filter_threshold = int(sys.argv[1])
    input_file = sys.argv[2]
    betweenness_output_file = sys.argv[3]
    communities_output_file = sys.argv[4]

    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sparkSession = SparkSession(sc)
    sc.setLogLevel("ERROR")
    
    uid_bid_dict = sc.textFile(input_file).filter(lambda x: not x.startswith('user_id')) \
        .map(lambda x: x.split(',')).groupByKey() \
        .mapValues(lambda bids: sorted(list(bids))) \
        .collectAsMap()
    
    uid_pairs_candidates = list(itertools.combinations(list(uid_bid_dict.keys()), 2))
    edge_list, vertex_set = [], set()

    for pair in uid_pairs_candidates:
        if len(set(uid_bid_dict[pair[0]]).intersection(
                set(uid_bid_dict[pair[1]]))) >= filter_threshold:
            vertex_set.add(pair[0])
            vertex_set.add(pair[1])
            edge_list.append(tuple((pair[0], pair[1])))
            edge_list.append(tuple((pair[1], pair[0])))

    # lists of user-id: ['vxR_YV0atFxIxfOnF9uHjQ', 'xxxxxxx', ...]
    vertices = sc.parallelize(list(vertex_set)).collect()
    # dictionary of user-list of users: {'vxR_YV0atFxIxfOnF9uHjQ': ['xxxx','xxxx'], ....}
    edges = sc.parallelize(edge_list).groupByKey() \
        .mapValues(lambda uidxs: sorted(list(set(uidxs)))).collectAsMap()
    '''
    2.1 Betweenness Calculation
    '''
    graphframe = MyGraphFrame(vertices, edges)
    betweeness_result = graphframe.compute_betweennes()
    
    with open(betweenness_output_file, 'w') as f:
        for betweenes_tuple in betweeness_result:
            f.writelines(str(betweenes_tuple[0]) + ',' + str(round(betweenes_tuple[1], 5)) + '\n')
        
    '''
    2.2 Community Detection
    '''
    community_result = graphframe.compute_communities()

    with open(communities_output_file, 'w') as f:
        for community in community_result:
            f.writelines(str(community)[1:-1]+ '\n')
        

    print("Duration: %d s." % (time.time() - start))