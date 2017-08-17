from pymongo import MongoClient
from bs4 import BeautifulSoup
import requests
import time
import os

# A NYT API page has 10 articles
NUM_ARTICLE_PER_PAGE = 10

# API Endpoint
LINK = 'http://api.nytimes.com/svc/search/v2/articlesearch.json'

# API Key
KEY = os.environ['NYT_API_KEY']

# Starting date to scrape 'YYYYMMDD'
DATE = '20170801'

# MongoDB Client
CLIENT = MongoClient('mongodb://localhost:27017/')
DB = CLIENT['NYT_articles']
TAB = DB['articles_table']

# curl --head http://api.nytimes.com/svc/search/v2/articlesearch.json?api-key=7372d81fa8544972820eb39729fc5547

# make a single call to the NYT API given the link and payload and return the response
def single_query(link, payload):
    response = requests.get(link, params=payload)
    if response.status_code != 200:
        print 'WARNING', response.status_code
        return None
    else:
        return response.json()['response']

# get the number of pages
def get_number_of_pages(date, key, link):
    payload = {'begin_date': date,'api-key': key}
    response = single_query(link, payload)
    return response['meta']['hits'] / NUM_ARTICLE_PER_PAGE + 1

# retrieve meta data for each article and insert into MongoDB
def get_meta_data(date, tab, key, link):
    list_articles = []
    num_pages_to_retrieve = get_number_of_pages(date, key, link)
    for p in xrange(num_pages_to_retrieve):
        time.sleep(1.1)
        print "getting page: ", p
        payload = {'api-key': key, 'page': p+1, 'type_of_material': 'News'}
        response = single_query(link, payload)

        for metadata_one in response['docs']:
            tab.insert_one(metadata_one)

# retrive the raw text for a single article given the URL
def get_raw_text_one(url):
    response = requests.get(url).text
    soup = BeautifulSoup(response, 'html.parser')
    raw_html = soup.select('div.story-body.story-body-1 p.story-content')
    return [raw.get_text(' ',strip=True) for raw in raw_html]

# main method
def scrape_NYT():
    get_meta_data(DATE, TAB, KEY, LINK)
    urls = {a['_id']: a['web_url'] for a in TAB.find()}
    for _id, url in urls.iteritems():
        raw_text = get_raw_text_one(url)
        TAB.update_one({'_id': _id}, {
            '$set': {
                'raw_text': raw_text
            }
        }, upsert = False)

if __name__ == '__main__':
    scrape_NYT()
