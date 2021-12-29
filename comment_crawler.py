import praw
import pickle
import os
from tqdm import tqdm
from collections import defaultdict
from pdb import set_trace

reddit = praw.Reddit(
    client_id="XAWWcdEAoBj5NyhDFhoZGg",
    client_secret="376aCdcCboyRVXCoff5wusr-Y19ahQ",
    password="jianjian.",
    user_agent="USERAGENT",
    username="dev_jian",
    ratelimit_seconds=60,
)

# posts_dict: 
# key:  msg_id
# value: dict{msg_id:'', poster:'', thread_id:'', \
#.            quote: '', msg:''}

# threads_dict: 
# key:  thread_id
# value: [dict{msg_id:'', poster:'', thread_id:'', \
#             quote: '', msg:''}, ...]


if os.path.exists('data/reddit_btc_parsed'):
    with open("data/reddit_btc_parsed", "rb") as f:
        posts_dict, threads_dict = pickle.load(f)

else: 
    posts_dict = {}
    threads_dict = defaultdict(list)

    
submission_list = list()
file_list = os.listdir('data/submissions')
for sub in tqdm(file_list):
    with open('data/submissions/' + sub, 'rb') as f:
        tmp_list = pickle.load(f)
    submission_list += tmp_list
    
print("# of submissions: {}".format(len(submission_list)))


for sub in tqdm(submission_list):
    sub_id = sub['id']
    # already saved 
    if sub_id in threads_dict: 
        continue
    
    submission = reddit.submission(sub_id)

    # filtering 
    if submission.selftext == '[deleted]':
        # print("deleted")
        continue
    if submission.selftext == '[removed]':
        # print("deleted")
        continue
    elif submission.selftext == '':
        # print("deleted 2")
        continue

    threads_dict[sub['id']] += [{'msg_id': sub['id'], \
                                 'poster': sub['author'], \
                                 'thread_id': sub['id'], \
                                 'msg': submission.selftext}] 
    posts_dict[sub['id']] = {'msg_id': sub['id'], \
                             'poster': sub['author'], \
                             'thread_id': sub['id'],
                             'msg': submission.selftext}

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
                                     'quote': quote} \
                                   ] 
        posts_dict[comment.id] = {'msg_id': comment.id, \
                                  'poster': poster, \
                                  'thread_id': sub['id'], \
                                  'msg': comment.body, \
                                  'quote': quote} 

    with open("data/reddit_btc_parsed", "wb") as f:
        pickle.dump([posts_dict, threads_dict], f)
        
# print(posts_dict)
# print(threads_dict)
# set_trace()

    
    