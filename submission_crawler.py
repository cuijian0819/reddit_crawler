import time
import os
from datetime import datetime
from psaw import PushshiftAPI
import dateutil.relativedelta as relativedelta
import argparse
import pickle
import pandas as pd

ap = argparse.ArgumentParser(description='reddit crawler')
ap.add_argument('--year', type=int, default=2021, 
                help='year to crawl')
# ap.add_argument('--sr', default='Bitcoin', 
#                 help='subreddit name')

args = ap.parse_args()

year = args.year
# subreddit = args.sr

print("start to crawl {}".format(year))

api = PushshiftAPI()
# df = pd.read_csv('../subreddit22/subreddits')
# subreddits = list(df['subr'])
subreddits = ['youtube', 'NewTubers']

for subreddit in subreddits:
    os.makedirs('data/submissions/{}'.format(subreddit), exist_ok=True)
    for i in range(365):
        end_date = datetime(year, 1, 1) + relativedelta.relativedelta(days=(i+1))
        start_date = end_date - relativedelta.relativedelta(days=1)
        posted_after = int(start_date.timestamp())
        posted_before = int(end_date.timestamp())
        fname = 'data/submissions/{}/{}_{}_{}'.format(subreddit,
                                            start_date.year, \
                                            start_date.month, \
                                            start_date.day)
        if os.path.exists(fname):
            print('submissions on {} have already crawled'.format(fname))
            continue
        
        print('start crawl submissions on {}...'.format(fname))
        query = api.search_submissions(subreddit=subreddit, \
                                    after=posted_after, \
                                    before=posted_before)
        
        submissions = list()
        for element in query:
            submissions.append(element.d_)
        
        with open(fname, 'wb') as f:
            pickle.dump(submissions, f)
        print(fname, len(submissions))

        time.sleep(1)
