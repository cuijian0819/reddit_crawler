# reddit_crawler
Reddit Crawler with Reddit API and Pushshift 


## How to run 

nohup python -u submission_crawler.py --year 2020 > log/submissions.log &

nohup python -u redditor_crawler.py --year 2021 > log/redditor_2021.log & 

nohup python -u comment_crawler.py --year 2021 > log/comment_2021.log &


## Reference

https://praw.readthedocs.io/en/stable/

https://psaw.readthedocs.io/en/latest/#description
