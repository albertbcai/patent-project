from lxml import etree
import StringIO
import re
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
import zipfile
import requests
from bs4 import BeautifulSoup
from sqlalchemy.exc import *
import sys
import os


delimiter = "|"

# Initialize the SQL tables
table_name = 'patent_info'
db = create_engine('mysql://root:patentproject@localhost:3306/patent_parsing', echo=False)
Session = sessionmaker(bind=db)
session = Session()
metadata = MetaData(db)
patent_info = \
    Table(table_name, metadata,
          Column('patent_id', String(50), primary_key=True),
          Column('abstract', TEXT),
          Column('description', TEXT(4294967295)),
          Column('claim', TEXT(4294967295)),
          Column('inventors', TEXT),
          Column('applicants', TEXT),
          Column('examiners', TEXT),
          Column('citations', TEXT),
          Column('main_cpc_version', TEXT),
          Column('main_cpc_section', TEXT),
          Column('main_cpc_class', TEXT),
          Column('main_cpc_subclass', TEXT),
          Column('main_cpc_main_group', TEXT),
          Column('main_cpc_subgroup', TEXT),
          Column('further_cpc_version', TEXT),
          Column('further_cpc_section', TEXT),
          Column('further_cpc_class', TEXT),
          Column('further_cpc_subclass', TEXT),
          Column('further_cpc_main_group', TEXT),
          Column('further_cpc_subgroup', TEXT),
          Column('main_classification', TEXT),
          Column('search_cpc', TEXT),
          Column('search_main_classification', TEXT),
          Column('ipc_version', TEXT),
          Column('ipc_section', TEXT),
          Column('ipc_class', TEXT),
          Column('ipc_subclass', TEXT),
          Column('ipc_main_group', TEXT),
          Column('ipc_subgroup', TEXT),
          Column('locarno_edition', TEXT),
          Column('classification_locarno', TEXT),
          Column('assignee', TEXT),
          )
try:
    metadata.create_all()
except:
    print(sys.exc_info())
patent_info = Table(table_name, metadata, autoload=True)


def listXmls(file):
    # self = open(file)
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

def output_to_file(elem):
    global doc_title
    global inventor_name
    global applicant_name
    global examiner_name
    global tags

    output_line = "".join(elem.itertext()).replace('\n', ' ')
    output_line = etree.tostring(elem, method='text', encoding='unicode').strip()
    ancestor_tags = "".join(x.tag for x in elem.iterancestors()) + elem.tag
    try:

        # Write to inventor list
        if ancestor_tags.find('inventor') != -1:
            if elem.tag == 'last-name':
                inventor_name = output_line
            if elem.tag == 'first-name':
                inventor_name = output_line + " " + inventor_name
                tags['inventors'] += inventor_name + delimiter
        # Write to applicants list
        if ancestor_tags.find('applicants') != -1:
            if elem.tag == 'last-name':
                applicant_name = output_line
            if elem.tag == 'first-name':
                applicant_name = output_line + " " + applicant_name
                tags['applicants'] += applicant_name + delimiter
        # Write to the claim text
        if elem.tag == 'claim':
            tags['claim'] += output_line + delimiter
        # Write to the description list
        if ancestor_tags.find('description') != -1:
            if elem.tag == 'p':
                tags['description'] += output_line + '\n'
        # Write to citations list
        if ancestor_tags.find('citation') != -1:
            if elem.tag == 'doc-number':
                if re.compile('[\\W]').search(output_line) is not None:
                    citation = re.split('[\\W]', output_line)[1]
                else:
                    citation = output_line
                tags['citations'] += citation + delimiter
        # Write to assignees list
        if ancestor_tags.find('assignee') != -1:
            if elem.tag == 'orgname' or elem.tag == 'organization-name':
                tags['assignee'] += output_line + delimiter
        # Write to examiner list
        if ancestor_tags.find('examiners') != -1:
            if elem.tag == 'last-name':
                examiner_name = output_line
            if elem.tag == 'first-name':
                examiner_name = output_line + " " + examiner_name
                tags['examiners'] += examiner_name + delimiter
        # Write to main_cpc_classification list
        if ancestor_tags.find('main-cpc') != -1 or ancestor_tags.find(''):
            if ancestor_tags.find('cpc-version-indicator') != -1 or ancestor_tags.find(
                    'classification-us-primary') != -1:
                tags['main_cpc_version'] += output_line + delimiter
            if elem.tag == 'section':
                tags['main_cpc_section'] += output_line + delimiter
            if elem.tag == 'class':
                tags['main_cpc_class'] += output_line + delimiter
            if elem.tag == 'subclass':
                tags['main_cpc_subclass'] += output_line + delimiter
            if elem.tag == 'main-group':
                tags['main_cpc_main_group'] += output_line + delimiter
            if elem.tag == 'subgroup':
                tags['main_cpc_subgroup'] += output_line + delimiter
        # Write to further_cpc_classification list
        if ancestor_tags.find('further-cpc') != -1 or ancestor_tags.find(''):
            if ancestor_tags.find('cpc-version-indicator') != -1 or ancestor_tags.find(
                    'classification-us-primary') != -1:
                tags['further_cpc_version'] += output_line + delimiter
            if elem.tag == 'section':
                tags['further_cpc_section'] += output_line + delimiter
            if elem.tag == 'class':
                tags['further_cpc_class'] += output_line + delimiter
            if elem.tag == 'subclass':
                tags['further_cpc_subclass'] += output_line + delimiter
            if elem.tag == 'main-group':
                tags['further_cpc_main_group'] += output_line + delimiter
            if elem.tag == 'subgroup':
                tags['further_cpc_subgroup'] += output_line + delimiter
        # Write to classification list for other files; tag 'main classification'
        if ancestor_tags.find('main-classification') != -1 and ancestor_tags.find(
                'classification-national') != -1 and ancestor_tags.find('classification-search') == -1:
            if elem.tag == 'main-classification':
                tags['main_classification'] += output_line + delimiter
        # Write classification added by examiner
        if ancestor_tags.find('main-classification') != -1 and ancestor_tags.find(
                'classification-national') != -1 and ancestor_tags.find('classification-search') != -1:
            if elem.tag == 'main-classification':
                tags['search_main_classification'] += output_line + delimiter
        # Write cpc classification added by examiner
        if ancestor_tags.find('classification-search') != -1:
            if elem.tag == 'classification-cpc-text':
                tags['search_cpc'] += output_line + delimiter
        # Write to ipc classification list
        if ancestor_tags.find('classification-ipcr') != -1 or ancestor_tags.find('classification-ipc-primary') != -1:
            if ancestor_tags.find('ipc-version-indicator') != -1 or ancestor_tags.find(
                    'classification-us-primary') != -1:
                tags['ipc_version'] += output_line + delimiter
            if elem.tag == 'section':
                tags['ipc_section'] += output_line + delimiter
            if elem.tag == 'class':
                tags['ipc_class'] += output_line + delimiter
            if elem.tag == 'subclass':
                tags['ipc_subclass'] += output_line + delimiter
            if elem.tag == 'main-group':
                tags['ipc_main_group'] += output_line + delimiter
            if elem.tag == 'subgroup':
                tags['ipc_subgroup'] += output_line + delimiter
        # Write to locarno list
        if ancestor_tags.find('classification-locarno') != -1:
            if elem.tag == 'edition':
                tags['locarno_edition'] += output_line + delimiter
            if elem.tag == 'main-classification':
                tags['classification_locarno'] += output_line + delimiter
        # Write to abstract list
        if elem.tag == 'abstract' or elem.tag == 'subdoc-abstract':
            tags['abstract'] += output_line + delimiter
    except UnicodeEncodeError:
        print('unicode error')
        return
    except IndexError:
        print('index error')
    return


year_list = ['2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016']
year_list = ['2014']
#year_list = [os.environ('SGE_TASK_ID')]
global doc_title

curr_file = 0
# Put this to the file that was left off as last
start_file = 178811
end_file = 999999999

path = os.path.expanduser("~/Desktop/patent_parsing/")

for year in year_list:
    os.chdir(path)
    if not os.path.exists(year):
        os.mkdir(year)
    os.chdir(year)
    curr_week = 0
    for f in os.listdir(path+year):
        print('Opened file %s' % f)
        if f == '.DS_Store':
            continue
        curr_week += 1
        archive = zipfile.ZipFile(f, 'r')
        filelist = archive.namelist()
        for file in filelist:
            file = archive.open(file)
            for xml in listXmls(file):
                curr_file += 1
                if curr_file > end_file:
                    break
                if curr_file < start_file:
                    continue
                inventor_name = ''
                applicant_name = ''
                examiner_name = ''
                tags = {'abstract': '',
                        'description': '',
                        'claim': '',
                        'inventors': '',
                        'applicants': '',
                        'examiners': '',
                        'citations': '',
                        'main_cpc_version': '',
                        'main_cpc_section': '',
                        'main_cpc_class': '',
                        'main_cpc_subclass': '',
                        'main_cpc_main_group': '',
                        'main_cpc_subgroup': '',
                        'further_cpc_version': '',
                        'further_cpc_section': '',
                        'further_cpc_class': '',
                        'further_cpc_subclass': '',
                        'further_cpc_main_group': '',
                        'further_cpc_subgroup': '',
                        'main_classification': '',
                        'search_cpc': '',
                        'search_main_classification': '',
                        'ipc_version': '',
                        'ipc_section': '',
                        'ipc_class': '',
                        'ipc_subclass': '',
                        'ipc_main_group': '',
                        'ipc_subgroup': '',
                        'locarno_edition': '',
                        'classification_locarno': '',
                        'assignee': '',
                        }
                events = ("start", "end")
                context = etree.iterparse(xml, events=events)
                try:
                    doc = etree.parse(xml).getroot().attrib['file']
                    doc_title = doc.split('\t')[0].split('-')[0].replace('US', '')
                    #print('ID:' + doc_title)
                    output_line = doc + '\t'
                except KeyError:
                    print(sys.exc_info())
                    #print(etree.tostringlist(xml))
                    doc_title = 'No title found'
                print('Parsing %s from year %s and week %s: patent ID %s' % (str(curr_file), year, str(curr_week), doc_title))
                for action, elem in context:
                    # If we find a beginning tag, output to file
                    if action == 'start':
                        # print elem.tag
                        output_to_file(elem)
                    # If we find an ending tag, the current field is done, so go back a tab and field in the outputline
                    if action == 'end':
                        continue
                # Remove the delimiter from the end of each entry that has it
                for tag in tags:
                    if tags[tag].endswith(delimiter):
                        tags[tag] = tags[tag][:-len(delimiter)]
                    #tags[tag] = tags[tag].encode("utf-8")
                # Write the values to a SQL table
                try:
                    patent_info.insert().execute(
                        patent_id=doc_title,
                        abstract=tags['abstract'],
                        description=tags['description'],
                        claim=tags['claim'],
                        inventors=tags['inventors'],
                        applicants=tags['applicants'],
                        examiners=tags['examiners'],
                        citations=tags['citations'],
                        main_cpc_version=tags['main_cpc_version'],
                        main_cpc_section=tags['main_cpc_section'],
                        main_cpc_class=tags['main_cpc_class'],
                        main_cpc_subclass=tags['main_cpc_subclass'],
                        main_cpc_main_group=tags['main_cpc_main_group'],
                        main_cpc_subgroup=tags['main_cpc_subgroup'],
                        further_cpc_version=tags['further_cpc_version'],
                        further_cpc_section=tags['further_cpc_section'],
                        further_cpc_class=tags['further_cpc_class'],
                        further_cpc_subclass=tags['further_cpc_subclass'],
                        further_cpc_main_group=tags['further_cpc_main_group'],
                        further_cpc_subgroup=tags['further_cpc_subgroup'],
                        main_classification=tags['main_classification'],
                        search_cpc=tags['search_cpc'],
                        search_main_classification=tags['search_main_classification'],
                        ipc_version=tags['ipc_version'],
                        ipc_section=tags['ipc_section'],
                        ipc_class=tags['ipc_class'],
                        ipc_subclass=tags['ipc_subclass'],
                        ipc_main_group=tags['ipc_main_group'],
                        ipc_subgroup=tags['ipc_subgroup'],
                        locarno_edition=tags['locarno_edition'],
                        classification_locarno=tags['classification_locarno'],
                        assignee=tags['assignee']
                        )
                except DataError:
                    print(sys.exc_info())
                    print ('Length of description:', len(tags['description']))
                except StatementError:
                    print(sys.exc_info())











