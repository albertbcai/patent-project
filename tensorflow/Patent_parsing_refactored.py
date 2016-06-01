from lxml import etree
import StringIO
import re
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
import zipfile
import requests
from bs4 import BeautifulSoup
import os

# Global variables
global saved_name
global saved_description
global saved_claim
global output_line
global cpc_version
global cpc_section
global cpc_class
global cpc_subclass
global cpc_main_group
global cpc_subgroup
global ipc_version
global ipc_section
global ipc_subclass
global ipc_main_group
global ipc_subgroup
global ipc_class
global classification_flag

def clear_data(session, meta):
    for table in reversed(meta.sorted_tables):
        print 'Clear table %s' % table
        session.execute(table.delete())
    session.commit()

# Delete the current SQL table so that we can overwrite it
#os.remove('/Users/albertcai/PycharmProjects/patent-project/tensorflow/patent_database.db')

# Initialize the SQL tables
db = create_engine('sqlite:///patent_database.db')
Session = sessionmaker(bind=db)
session = Session()
db.echo = False  # Try changing this to True and see what happens
metadata = MetaData(db)
clear_data(session, metadata)


abstracts = Table('abstracts', metadata,
    Column('patent_id', String()),
    Column('abstract', String()),
)
try: abstracts.create()
except: print('Abstract table already created')

descriptions = Table('descriptions', metadata,
    Column('patent_id', String()),
    Column('description', String()),
)
try: descriptions.create()
except: print('Description table already created')

claims = Table('claims', metadata,
    Column('patent_id', String()),
    Column('claim', String()),
)
try: claims.create()
except: print('Claim table already created')

inventors = Table('inventors', metadata,
    Column('patent_id', String()),
    Column('inventor', String()),
)
try: inventors.create()
except: print('Inventors table already created')

examiners = Table('examiners', metadata,
    Column('patent_id', String()),
    Column('examiner', String()),
)
try: examiners.create()
except: print('Examiners table already created')

citations = Table('citations', metadata,
    Column('patent_id', String()),
    Column('citation_id', String()),
)
try: citations.create()
except: print('Citations table already created')

classifications = Table('classifications', metadata,
    Column('patent_id', String()),
    Column('cpc_version', String()),
    Column('cpc_section', String()),
    Column('cpc_class', String()),
    Column('cpc_subclass', String()),
    Column('cpc_main_group', String()),
    Column('cpc_subgroup', String()),
    Column('main_classification', String()),
)

try: classifications.create()
except: print('Classifications table already created')

assignees = Table('assignees', metadata,
    Column('patent_id', String()),
    Column('assignee', String()),
)
try: assignees.create()
except: print('Assignees table already created')

def listXmls(file):
    #self = open(file)
    self = file
    output = StringIO.StringIO()
    line = self.readline()
    output.write(line)
    line = self.readline()
    while line is not '':
      if '<?xml version="1.0" encoding="UTF-8"?>' in line:
        line = line.replace('<?xml version="1.0" encoding="UTF-8"?>', '')
        output.write(line)
        output.seek(0)
        yield output
        output = StringIO.StringIO()
        output.write('<?xml version="1.0" encoding="UTF-8"?>')
      elif '<?xml version="1.0"?>' in line:
        line = line.replace('<?xml version="1.0"?>', '')
        output.write(line)
        output.seek(0)
        yield output
        output = StringIO.StringIO()
        output.write('<?xml version="1.0"?>')
      else:
        output.write(line)
      try:
        line = self.readline()
      except StopIteration:
        break
    output.seek(0)
    yield output

def output_to_file(elem, doc_title):
    global saved_name
    global saved_claim
    global saved_description
    global output_line
    global inventor_index
    global citation_index
    global assignee_index
    global examiner_index
    global classification_index
    global cpc_version
    global cpc_section
    global cpc_class
    global cpc_subclass
    global cpc_main_group
    global cpc_subgroup
    global ipc_version
    global ipc_section
    global ipc_subclass
    global ipc_main_group
    global ipc_subgroup
    global ipc_class
    global classification_flag

    output_line = "".join(elem.itertext()).replace('\n', ' ')
    ancestor_tags = "".join(x.tag for x in elem.iterancestors()) + elem.tag
    try:
        # Write to inventor list
        if ancestor_tags.find('inventor')!= -1 or ancestor_tags.find('applicants')!= -1:
            if elem.tag == 'last-name':
                global saved_name
                saved_name = output_line
            if elem.tag == 'first-name':
                full_name = output_line + " " + saved_name
                try: inventors.insert().execute(patent_id=doc_title, inventor=full_name)
                except: print('error when entering file for inventors')
        # Write to the claim text
        if ancestor_tags.find('claim-text') != -1:
            saved_claim += output_line
        # Write to the description list
        if ancestor_tags.find('description') != -1:
            saved_description += output_line
        # Write to citations list
        if ancestor_tags.find('citation') != -1:
            if elem.tag =='doc-number':
                if re.compile('[\\W]').search(output_line) is not None:
                    citation = re.split('[\\W]', output_line)[1]
                else:
                    citation = output_line
                try: citations.insert().execute(patent_id=doc_title, citation_id=citation)
                except: print('error when entering file for citations')
        # Write to assignees list
        if ancestor_tags.find('assignee') != -1:
            if elem.tag =='orgname' or elem.tag == 'organization-name':
                try:
                    assignees.insert().execute(patent_id=doc_title, assignee=output_line)
                except: print('error when entering file for assignees')
        # Write to examiner list
        if ancestor_tags.find('examiners') != -1:
            if elem.tag == 'last-name':
                global saved_name
                saved_name = output_line
            if elem.tag == 'first-name':
                full_name = output_line + " " + saved_name
                try:
                    examiners.insert().execute(patent_id=doc_title, examiner=full_name)
                except: print('error when entering file for examiners')
        # Write to classification list
        if ancestor_tags.find('main-cpc') != -1 or ancestor_tags.find(''):
            if ancestor_tags.find('cpc-version-indicator') != -1 or ancestor_tags.find('classification-us-primary') != -1:
                cpc_version = output_line
            if elem.tag == 'section':
                cpc_section = output_line
            if elem.tag == 'class':
                cpc_class = output_line
            if elem.tag == 'subclass':
                cpc_subclass = output_line
            if elem.tag == 'main-group':
                cpc_main_group = output_line
            if elem.tag == 'subgroup':
                cpc_subgroup = output_line
        # Write to classification list for other files; tag 'main classification'
        if ancestor_tags.find('main-classification') != -1 and ancestor_tags.find('classification-national') != -1 and classification_flag == True:
            try:
                classifications.insert().execute(patent_id=doc_title, main_classification=output_line)
                classification_flag = False
            except:
                print('error when entering file for main_classification')
        # Write to abstract list
        if elem.tag == 'abstract' or elem.tag == 'subdoc-abstract':
            try:
                abstracts.insert().execute(patent_id=doc_title, abstract=output_line)
            except: print('error when entering file for abstracts')
    except UnicodeEncodeError:
        print('unicode error')
        return
    except IndexError:
        print('index error')
    return

abstracts = Table('abstracts', metadata, autoload=True)
assignees = Table('assignees', metadata, autoload=True)
citations = Table('citations', metadata, autoload=True)
claims = Table('claims', metadata, autoload=True)
classifications = Table('classifications', metadata, autoload=True)
descriptions = Table('descriptions', metadata, autoload=True)
examiners = Table('examiners', metadata, autoload=True)
inventors = Table('inventors', metadata, autoload=True)

#location = '/Users/albertcai/Desktop/Patents/ipg160209.xml'
#location = '/Users/albertcai/Desktop/Patents/ipa150101.zip'
#location = '/Users/albertcai/Desktop/test_xml.xml'
url_grant = 'https://bulkdata.uspto.gov/data2/patent/grant/redbook/fulltext/'
year_list = ['2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016']
#year = '2015'
curr_file = 0
start_file = 7213
# Iterate through each year specified
for year in year_list:
    full_url = url_grant + year
    uspto_website = requests.get(full_url, headers={'User-Agent': 'Mozilla/5.0'}).text
    soup = BeautifulSoup(uspto_website, 'lxml')
    links = soup.findAll('a', attrs={ 'href': re.compile('zip$') })
    # Iterate through every .zip file on the year
    for link in links:
        string_link = str(link)
        start = '="'
        end = '.zip'
        file = string_link[string_link.find(start):string_link.find(end, string_link.find(start))][2:] + '.zip'

        print("Downloading file:%s for year:%s" % (file, year))
        url = requests.get(url_grant + year + '/' + file, headers={'User-Agent': 'Mozilla/5.0'})
        print("File download complete. Begin parsing.")
        f = StringIO.StringIO()
        f.write(url.content)
        archive = zipfile.ZipFile(f, 'r')
        filelist = archive.namelist()
        for file in filelist:
            file = archive.open(file)
            for xml in listXmls(file):
                saved_string = ''
                saved_description = ''
                saved_claim = ''
                classification_flag = True
                curr_file += 1
                if curr_file < start_file: continue

                print('Document ' + str(curr_file) + ' is being parsed.')
                events = ("start", "end")
                context = etree.iterparse(xml, events=events)
                try:
                    doc = etree.parse(xml).getroot().attrib['file']
                    doc_title = doc.split('\t')[0].split('-')[0].replace('US', '')
                    output_line = doc + '\t'
                except KeyError:
                    doc_title = 'No title found'

                for action, elem in context:
                    # If we find a beginning tag, output to file
                    if action == 'start':
                        # print elem.tag
                        output_to_file(elem, doc_title)
                    # If we find an ending tag, the current field is done, so go back a tab and field in the outputline
                    if action == 'end':
                        continue
                try:
                    claims.insert().execute(patent_id=doc_title, claim=saved_claim)
                except:
                    print('error when entering file for claims')
                try:
                    descriptions.insert().execute(patent_id=doc_title, description=saved_description)
                except:
                    print('error when entering file for descriptions')
                try:
                    classifications.insert().execute(patent_id=doc_title, cpc_version=cpc_version, cpc_section=cpc_section,
                                                     cpc_class=cpc_class, cpc_subclass=cpc_subclass,
                                                     cpc_main_group=cpc_main_group,
                                                     cpc_subgroup=cpc_subgroup)
                except:
                    # print('error when entering file for cpc classifications')
                    print('')






