import sys, random, time, binascii, csv
from blackbox import BlackBox

bit_array = [0] * 69997
hash_num = 5
m = 69997


def random_hash_param(hash_num):
    """
    generate random hash functions parameters a and b
    :param hash_num: number of hash functions
    :return: a list of tuple (a, b), len is hash_num
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
    :return: a list of hash function ((a * s + b) % p) % m
    """
    hash_param = random_hash_param(hash_num)
    hash_func = []
    for a, b in hash_param:
        user_num = int(binascii.hexlify(user_id.encode('utf8')), 16)
        hash_func.append(((a * user_num + b) % 23333333) % m)
    
    return hash_func

if __name__ == "__main__":

    input_file = sys.argv[1]
    stream_size = int(sys.argv[2]) # 100
    num_of_asks = int(sys.argv[3]) # more than 30
    output_file = sys.argv[4]

    start_time = time.time()

    bx = BlackBox()
    previous_users = set()
    hash_param = random_hash_param(hash_num)
    fpr_list = []

    for batch in range(num_of_asks):

        stream_users = bx.ask(input_file, stream_size)
        positive_false = [0] * stream_size
        for i in range(len(stream_users)):
            user_num = int(binascii.hexlify(stream_users[i].encode('utf8')), 16)
            hash_func = []
            # if hash_1_num == hash_num means all bits are already been 1 and this user_num will be discard
            hash_1_num = 0 
            for a, b in hash_param:
                hash_val = ((a * user_num + b) % 23333333) % m
                # this user_num not is the array
                if bit_array[hash_val] == 0:
                    break
                else:
                    hash_1_num += 1
            if hash_1_num == len(hash_param):
                positive_false[i] = 1

        fp = 0  
        for i in range(len(stream_users)):
            if stream_users[i] not in previous_users and positive_false[i] == 1:
                fp += 1
        fpr = fp / (len(positive_false) - sum(positive_false) + fp)
        fpr_list.append([batch, fpr])

        for user in stream_users:
            previous_users.add(user)
            for a, b in hash_param:
                user_num = int(binascii.hexlify(user.encode('utf8')), 16)
                hash_val = ((a * user_num + b) % 23333333) % m
                bit_array[hash_val] = 1

    #print(fpr_list)

    file =  open(output_file, 'w+')
    writer = csv.writer(file, dialect = 'excel')
    writer.writerow(['Time','FPR'])
    writer.writerows(fpr_list)
    file.close()

    print("Duration: " + str(time.time() - start_time))