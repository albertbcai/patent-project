import StringIO
import re
import zipfile
import requests
from bs4 import BeautifulSoup
import os
from clint.textui import progress
import sys
os.chdir(os.path.expanduser("~/Desktop"))
url_grant = 'https://bulkdata.uspto.gov/data2/patent/grant/redbook/fulltext/'
url_app = 'https://bulkdata.uspto.gov/data2/patent/application/redbook/fulltext/'
url = url_app
save_path = 'patent_parsing_applications/'
if not os.path.exists(save_path):
    os.mkdir(save_path)
os.chdir(save_path)

#year_list = [os.environ('SGE_TASK_ID')]

curr_file = 0
# start_file = 7213
year_list = range(2016, 2017)

# Iterate through each year specified
for year in year_list:
    year = str(year)
    if not os.path.exists(year):
        os.mkdir(year)
    os.chdir(year)
    curr_file = 0
    full_url = url + str(year)
    print("Loading USPTO website")
    uspto_website = requests.get(full_url, headers={'User-Agent': 'Mozilla/5.0'}).text
    print("Done")
    soup = BeautifulSoup(uspto_website, 'lxml')
    links = soup.findAll('a', attrs={'href': re.compile('zip$')})
    # Iterate through every .zip file on the year
    for link in links:
        string_link = str(link)
        start = '="'
        end = '.zip'
        filename = string_link[string_link.find(start):string_link.find(end, string_link.find(start))][2:] + '.zip'
        if os.path.exists(filename):
            continue
        print("Downloading file: %s for year: %s" % (filename, year))
        with open(filename, "wb") as f:
            url = requests.get(url_grant + year + '/' + filename, headers={'User-Agent': 'Mozilla/5.0'}, stream=True)
            total_length = url.headers.get('content-length')

            if total_length is None:  # no content length header
                f.write(url.content)
            else:
                dl = 0
                total_length = int(total_length)
                for data in url.iter_content(total_length / 50):
                    dl += len(data)
                    f.write(data)
                    done = int(50 * dl / total_length)
                    sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50 - done)))
                    sys.stdout.flush()
        print("File download complete")
    os.chdir('..')