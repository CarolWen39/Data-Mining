import sys, random, time, binascii, csv
from blackbox import BlackBox

m = 69997
hash_num = 10

def random_hash_param(hash_num):
    """
    generate random hash functions parameters a and b
    :param hash_num: number of hash functions
    :return: a list of hash function ((a * s + b) % p) % m
    """
    hash_func_list = []
    param_as = random.sample(range(1, sys.maxsize - 1), hash_num)
    param_bs = random.sample(range(0, sys.maxsize - 1), hash_num)
    for a, b in zip(param_as, param_bs):
        hash_func_list.append((a, b))
    
    return hash_func_list

def myhash(user_id):
    """
    generate hash functions
    :param hash_num: user_id
    :return: a list of hash function (a * s + b) % m
    """
    hash_param = random_hash_param(hash_num)
    hash_func = []
    for a, b in hash_param:
        user_num = int(binascii.hexlify(user_id.encode('utf8')), 16)
        hash_func.append(((a * user_num + b) % 2333333) % m)
    
    return hash_func

if __name__ == "__main__":

    input_file = sys.argv[1]
    stream_size = int(sys.argv[2]) # 300
    num_of_asks = int(sys.argv[3]) # more than 30
    output_file = sys.argv[4]

    start_time = time.time()
    bx = BlackBox()
    
    hash_param = random_hash_param(hash_num)
    zeros = [0] * hash_num
    estimator_sum = 0.0
    res = []

    for batch in range(num_of_asks):
        stream_users = bx.ask(input_file, stream_size)

        current_set = set()
        for user in stream_users:
            current_set.add(user)

        for i in range(len(stream_users)):
            user_num = int(binascii.hexlify(stream_users[i].encode('utf8')), 16)
            for idx in range(len(hash_param)):
                a, b = hash_param[idx]
                hash_val_binary = bin(((a * user_num + b) % 2333333) % m)
                trailing_zero = len(hash_val_binary.split("1")[-1])
                if trailing_zero > zeros[idx]:
                    zeros[idx] = trailing_zero
            
        estimator = 0
        avg_estimator = round(sum(map(lambda x: 2 ** x, zeros)) / len(zeros))
        estimator_sum += avg_estimator
        zeros = [0] * hash_num
        res.append([batch, stream_size,  avg_estimator])

    print(f"sum of all estimations / sum of all ground truths = {estimator_sum / (stream_size * num_of_asks)}")

    file =  open(output_file, 'w+')
    writer = csv.writer(file, dialect = 'excel')
    writer.writerow(['Time','Ground Truth','Estimation'])
    writer.writerows(res)
    file.close()

    print("Duration: " + str(time.time() - start_time))