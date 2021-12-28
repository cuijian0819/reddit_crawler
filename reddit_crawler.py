import time
from bs4 import BeautifulSoup
from lxml import etree
import os
from tqdm import tqdm
from datetime import datetime
from psaw import PushshiftAPI
import dateutil.relativedelta as relativedelta
import argparse
import pickle

ap = argparse.ArgumentParser(description='reddit crawler')
ap.add_argument('--year', type=int, default=2021, 
                help='year to crawl')

args = ap.parse_args()

year = args.year

print("start to crawl {}".format(year))

api = PushshiftAPI()

start_date = datetime(year, 1, 1)

for i in range(365):
    end_date = start_date + relativedelta.relativedelta(days=(i+1))
    posted_after = int(start_date.timestamp())
    posted_before = int(end_date.timestamp())
    query = api.search_submissions(subreddit='Bitcoin', \
                                   after=posted_after, \
                                   before=posted_before)
    
    submissions = list()
    for element in query:
        submissions.append(element.d_)
    
    fname = 'data/meta/{}_{}_{}'.format(start_date.year, \
                                        start_date.month, \
                                        start_date.day)
    with open(fname, 'wb') as f:
        pickle.dump(submissions, f)
        
    print(fname, len(submissions))