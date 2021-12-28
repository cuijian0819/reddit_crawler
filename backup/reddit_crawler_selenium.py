from selenium import webdriver
import lxml
import time
from pdb import set_trace
from selenium.webdriver.common.by import By
import os
from tqdm import tqdm
import time

driver = webdriver.Firefox(executable_path=os.getcwd()+"/geckodriver")

# driver = webdriver.Chrome("./chromedriver")

url="https://www.reddit.com/r/Bitcoin/"

response = driver.get(url)
driver.implicitly_wait(10)


last_height = driver.execute_script("return document.body.scrollHeight")

page_num = 0
while True:
    # Scroll down to bottom
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    # Wait to load page
    time.sleep(10)

    with open('data/raw/reddit_btc_{}.html'.format(page_num), 'w') as f:
        f.write(driver.page_source)
        page_num += 1


    # Calculate new scroll height and compare with last scroll height
    new_height = driver.execute_script("return document.body.scrollHeight")
    if new_height == last_height:
        print("scroll to end")
        break
    last_height = new_height

