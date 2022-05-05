import sys, random, time, csv
from blackbox import BlackBox


if __name__ == "__main__":
    random.seed(553)

    input_file = sys.argv[1]
    stream_size = int(sys.argv[2]) # 100
    num_of_asks = int(sys.argv[3]) # more than 30
    output_file = sys.argv[4]

    start_time = time.time()
    
    seqnum = 0
    bx = BlackBox()
    sample_user = [] # len should be less than 100
    sample_size = 100
    sample_cnt = 0
    res = []
    
    for batch in range(num_of_asks):
        stream_users = bx.ask(input_file, stream_size)
        # directly store the uesr_id
        if batch == 0:
            for id in stream_users:
                sample_user.append(id)
            sample_cnt = 100
        else:
            for id in stream_users:
                sample_cnt += 1
                sample_accept_prob = random.random()
                if sample_accept_prob < sample_size / sample_cnt:
                    pos = random.randint(0, 99)
                    sample_user[pos] = id
        
        res.append([sample_cnt, sample_user[0], sample_user[20], sample_user[40], sample_user[60],sample_user[80]])

    file =  open(output_file, 'w+')
    writer = csv.writer(file, dialect = 'excel')
    writer.writerow(['seqnum','0_id','20_id','40_id','60_id','80_id'])
    writer.writerows(res)
    file.close()

    print("Duration: " + str(time.time() - start_time))