import praw
import pickle
import os
from tqdm import tqdm
from collections import defaultdict
from multiprocessing import Pool
from itertools import repeat
import numpy as np
import argparse
import prawcore

from pdb import set_trace

# reddit = praw.Reddit(
#     client_id="XAWWcdEAoBj5NyhDFhoZGg",
#     client_secret="376aCdcCboyRVXCoff5wusr-Y19ahQ",
#     password="jianjian.",
#     user_agent="USERAGENT",
#     username="dev_jian",
#     ratelimit_seconds=60,
# )

reddit = praw.Reddit(
    client_id="R28puZYRno9JelIiMNtwGA",
    client_secret="6PLNOoZapjk2AW-BMBXoeQuSmLywUg",
    password="nssrhksdn!@#",
    user_agent="USERAGENT",
    username="nss_kw",
    ratelimit_seconds=60,
)

ap = argparse.ArgumentParser(description='reddit crawler')
ap.add_argument('--year', type=int, default=2021, 
                help='year to crawl')

args = ap.parse_args()

year = args.year

# posts_dict: 
# key:  msg_id
# value: dict{msg_id:'', poster:'', thread_id:'', \
#.            quote: '', msg:''}

# threads_dict: 
# key:  thread_id
# value: [dict{msg_id:'', poster:'', thread_id:'', \
#             quote: '', msg:''}, ...]


if os.path.exists('data/reddit_btc_parsed_{}'.format(year)):
    with open('data/reddit_btc_parsed_{}'.format(year), "rb") as f:
        posts_dict, threads_dict = pickle.load(f)

else: 
    posts_dict = {}
    threads_dict = defaultdict(list)

    
submission_list = list()
file_list = os.listdir('data/submissions/{}'.format(year))
for sub in tqdm(file_list):
    with open('data/submissions/{}/'.format(year) + sub, 'rb') as f:
        tmp_list = pickle.load(f)
    submission_list += tmp_list
    
print("# of submissions: {}".format(len(submission_list)))


for i, sub in enumerate(tqdm(submission_list)):
    sub_id = sub['id']
    # skip already saved 
    if sub_id in threads_dict: 
        # print("{} have already saved".format(sub_id))
        continue
        
    submission = reddit.submission(sub_id)
    try:
        submission._fetch()
    except prawcore.exceptions.NotFound:
        continue

    # filtering 
    if submission.selftext == '[deleted]':
         threads_dict[sub_id] = None
    elif submission.selftext == '[removed]':
         threads_dict[sub_id] = None
    elif submission.selftext == '':
         threads_dict[sub_id] = None
    else:
        threads_dict[sub_id] += [{'msg_id': sub_id, \
                                  'poster': sub['author'], \
                                  'thread_id': sub['id'], \
                                  'msg': submission.selftext, \
                                  'timestamp': submission.created_utc \
                                 }]
        posts_dict[sub_id] = {'msg_id': sub_id, \
                              'poster': sub['author'], \
                              'thread_id': sub['id'], \
                              'msg': submission.selftext, \
                              'timestamp': submission.created_utc}

        # comment
        submission.comments.replace_more(limit=None)
        for comment in submission.comments.list():
            prefix, pid = comment.parent_id.split('_')
            if prefix == 't3':
                quote = None
            else:
                quote = pid

            if comment.author != None: 
                poster = comment.author.name
            else:
                poster = None

            threads_dict[sub['id']] += [{'msg_id': comment.id, \
                                         'poster': poster, \
                                         'thread_id': sub['id'], \
                                         'msg': comment.body, \
                                         'quote': quote, \
                                         'timestamp': comment.created_utc} \
                                       ] 
            posts_dict[comment.id] = {'msg_id': comment.id, \
                                      'poster': poster, \
                                      'thread_id': sub['id'], \
                                      'msg': comment.body, \
                                      'quote': quote, \
                                      'timestamp': comment.created_utc}

    # save
    if i%100 == 0: 
        print("update dictionary...")
        with open('data/reddit_btc_parsed_{}'.format(year), "wb") as f:
            pickle.dump([posts_dict, threads_dict], f)
        
print("update dictionary...")
with open('data/reddit_btc_parsed_{}'.format(year), "wb") as f:
    pickle.dump([posts_dict, threads_dict], f)
        
# num_proc = 8

# pool = Pool(processes=num_proc)

# submission_sublists = np.array_split(submission_list, num_proc)

# pool.map(make_dict, submission_sublists)
# pool.close() 
# pool.join()


    
    