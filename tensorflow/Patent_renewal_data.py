import requests
from bs4 import BeautifulSoup
import re
import StringIO
import zipfile
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker

# If True, download the file from online (updated weekly)
# If False, use the local .zip file path that has already been downloaded.
download = False
if download:
    # Download renewal data from online and then process it
    url = 'https://bulkdata.uspto.gov/data2/patent/maintenancefee/'
    uspto_website = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}).text
    soup = BeautifulSoup(uspto_website, 'lxml')
    links = soup.findAll('a', attrs={'href': re.compile('zip$')})
    string_link = str(links[0])
    start = '="'
    end = '.zip'
    file = string_link[string_link.find(start):string_link.find(end, string_link.find(start))][2:] + '.zip'
    print("Downloading renewal data")
    url = requests.get(url + file, headers={'User-Agent': 'Mozilla/5.0'})
    print("Done downloading data")
    f = StringIO.StringIO()
    f.write(url.content)
else:
    # Use downloaded renewal data to save time
    f = '/Users/albertcai/Desktop/MaintFeeEvents.zip'

# Initialize the SQL table, renewals
db = create_engine('sqlite:///patent_database3.db')
Session = sessionmaker(bind=db)
session = Session()
db.echo = False  # Try changing this to True and see what happens
metadata = MetaData(db)
renewals = Table('renewals', metadata,
    Column('patent_id', String()),
    Column('renewal_code', String()),
)
try: renewals.create()
except: print('Renewal table already created')
renewals = Table('renewals', metadata, autoload=True)


archive = zipfile.ZipFile(f, 'r')
file = archive.namelist()[0]
file = archive.open(file)
line_number = 0
# Iterate through each line in the file
for block in iter(lambda: file.read(53), ""):
    line_number += 1
    text_list = block.split(" ")
    '''Format of the text_list by index:
    0: U.S. Patent Number. 7 character alphanumeric field. Re-issue patent has RE in front.
    1: U.S. Application number. 2 characters of series code followed by 6 digit application number.
    2: Small Entity. Y is small entity, M is microentity, N is large entity, ? or _ which indicated that it is not available
    3: U.S. Application Filing Date. The date it was filed, in the format yyyymmdd
    4: U.S Grant Issue Date. The date the grant was issued, in formate of yyyymmdd
    5: Maintenance Fee Event Entry Date. The date the fee was entered, in format of yyyymmdd
    6: Maintanence Fee Event Code. The code for what the fee event is.
    7: Formatting'''
    patent_id = text_list[0]
    maintanence_code = text_list[6]
    renewals.insert().execute(patent_id=patent_id, renewal_code=maintanence_code)
    print('Line number %s complete' % (line_number))

