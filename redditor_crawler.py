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
import pandas as pd

from pdb import set_trace

reddit1 = praw.Reddit(
    client_id="Z_XahTL1F5Qh78M1duhrag",
    client_secret="-1oma1BFTHRHaTEc8gzVyviijolS8w",
    password="nssadmin!",
    user_agent="USERAGENT",
    username="nss_mk",
    ratelimit_seconds=60,
)

reddit2 = praw.Reddit(
    client_id="N_JPpabcnEVN5sk9gsCrpQ",
    client_secret="uZjkpKcY5OXdKt0Mw20aSaXhy22uSQ",
    password="nssadmin!",
    user_agent="USERAGENT",
    username="NSS11223344",
    ratelimit_seconds=60,
)

reddit3 = praw.Reddit(
    client_id="XAWWcdEAoBj5NyhDFhoZGg",
    client_secret="376aCdcCboyRVXCoff5wusr-Y19ahQ",
    password="jianjian.",
    user_agent="USERAGENT",
    username="dev_jian",
    ratelimit_seconds=60,
)

reddit4 = praw.Reddit(
    client_id="R28puZYRno9JelIiMNtwGA",
    client_secret="6PLNOoZapjk2AW-BMBXoeQuSmLywUg",
    password="nssrhksdn!@#",
    user_agent="USERAGENT",
    username="nss_kw",
    ratelimit_seconds=60,
)

reddit_list = [reddit1, reddit2, reddit3, reddit4]
# reddit_list = [reddit1, reddit2]

ap = argparse.ArgumentParser(description='reddit crawler')
ap.add_argument('--year', type=int, default=2019, 
                help='year to crawl')

args = ap.parse_args()

year = args.year


with open('data/reddit_btc_parsed_{}'.format(year), "rb") as f:
    posts_dict, threads_dict = pickle.load(f)

df = pd.DataFrame(posts_dict.values())


if os.path.exists('data/redditors_dict_{}'.format(year)):
    with open('data/redditors_dict_{}'.format(year), "rb") as f:
        redditors_dict = pickle.load(f)

else: 
    redditors_dict = {}

    
redditors_list = df['poster'].dropna().unique().tolist()
print('# of redditor: {}'.format(len(redditors_list)))

for i, redditor_name in enumerate(tqdm(redditors_list)):
    if redditor_name in redditors_dict:
        continue

    tmp_reddit = reddit_list[(i%len(reddit_list))]
    redditor = tmp_reddit.redditor(redditor_name)
    
    try:
        redditor._fetch()
    except prawcore.exceptions.NotFound:
        continue

    if hasattr(redditor, 'is_suspended'):
        redditors_dict[redditor_name] = {'is_suspended': True}
    else:
        redditors_dict[redditor_name] = {'name': redditor_name, \
                                         'comment_karma': redditor.comment_karma, \
                                         'link_karma': redditor.link_karma, \
                                         'created_utc': redditor.created_utc, \
                                         'is_employee': redditor.is_employee, \
                                         'is_mod': redditor.is_mod, \
                                         'is_gold': redditor.is_gold, \
                                         'is_suspended': False,
                                         'trophies': redditor.trophies(),
                                        }

    if i%100 == 0: 
        print("update dictionary...")
        with open('data/redditors_dict_{}'.format(year), "wb") as f:
            pickle.dump(redditors_dict, f)

    
print("update dictionary...")
with open('data/redditors_dict_{}'.format(year), "wb") as f:
    pickle.dump(redditors_dict, f)


