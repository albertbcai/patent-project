from lxml import etree
import StringIO
import re
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
import zipfile
import sys
import os
import random
from time import sleep
from sqlalchemy.exc import OperationalError

# Bash script:
# qsub -N All2005 -o ~/job_output/ -j y -b y -t 1-52 'source ~/test/bin/activate; python < ~/python_scripts/patent_parsing_sqlite_array.py'
# -N: Job name
# -o: Directory to output files to
# -j: Join output
# -b: Accept binary input
# -t: Input variable as SGE_TASK_ID. This code runs from weeks 1-52 simultaneously.
# Run bash script to run code.

year = '2005'
week = str(os.environ.get('SGE_TASK_ID'))
delimiter = "|"

database_path = "~/databases/"
database_name = 'patent_grants_' + year
table_name = 'patent_info_' + year



database_path = os.path.expanduser(database_path)
if not os.path.exists(database_path):
    os.mkdir(database_path)
os.chdir(database_path)

# Initialize the SQL tables
try:
    db = create_engine('sqlite:///' + database_name + '.db', echo=False)
    Session = sessionmaker(bind=db)
    session = Session()
    metadata = MetaData(db)
    conn = db.connect()

    patent_info = \
        Table(table_name, metadata,
              Column('patent_id', String(50), primary_key=True),
              Column('patent_date', TEXT),
              Column('title', TEXT),
              Column('abstract', TEXT),
              Column('description', TEXT),
              Column('claims', TEXT),
              Column('inventors', TEXT),
              Column('applicants', TEXT),
              Column('examiners', TEXT),
              Column('assignee', TEXT),
              Column('citations', TEXT),
              Column('agents', TEXT),
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
              )
    metadata.create_all()
except:
    print(sys.exc_info())
patent_info = Table(table_name, metadata, autoload=True)

def listXmls(file):
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
def output_with_dict(dict, table):
    table.insert().execute(dict)
def fix_id(patent_id):
    patent_id = patent_id.replace(' ','')
    patent_id = patent_id.replace('&','')
    patent_id = patent_id.replace('/','')
    patent_id = patent_id.replace('-','')
    patent_id = patent_id.replace(':','')
    if patent_id.startswith('D0'):
        return patent_id[0] + patent_id[2:]
    if patent_id.startswith('RE0'):
        return patent_id[0:2] + patent_id[3:]
    if patent_id.startswith('0'):
        return patent_id[1:]
    if patent_id.startswith('PP0'):
        return patent_id[0:2] + patent_id[3:]
    return patent_id
def output_to_file(elem):
    global doc_title
    global patent_date
    global inventor_name
    global applicant_name
    global examiner_name
    global tags

    output_line = etree.tostring(elem, method='text', encoding='unicode').strip()
    ancestor_tags = "".join(x.tag for x in elem.iterancestors()) + elem.tag
    try:
        # Write to legal list
        if ancestor_tags.find('agents') != -1:
            if elem.tag == 'orgname':
                tags['agents'] += output_line + delimiter
            if elem.tag == 'last-name':
                inventor_name = output_line
            if elem.tag == 'first-name':
                inventor_name = output_line + " " + inventor_name
                tags['agents'] += inventor_name + delimiter
        # Write to invention title
        if elem.tag == 'invention-title' or elem.tag == 'title-of-invention':
            tags['title'] = output_line
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
            tags['claims'] += output_line + delimiter
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
global doc_title

curr_file = 0
# Put this to the file that was left off as last
#start_file = 178811
start_file = 0
end_file = 999999999
output_dict_list = []
path = os.path.expanduser("~/patent_parsing/" + str(year))
os.chdir(path)
f = week
print('Opened file %s' % f)
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
        # Reset the tags dictionary
        tags = {'patent_id': '',
                'patent_date': '',
                'abstract': '',
                'description': '',
                'claims': '',
                'title': '',
                'inventors': '',
                'applicants': '',
                'examiners': '',
                'citations': '',
                'agents': '',
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
        # Extract the document ID and the publication date
        try:
            doc = etree.parse(xml).getroot().attrib['file']
            patent_date = etree.parse(xml).getroot().attrib['date-publ']
            doc_title = doc.split('\t')[0].split('-')[0].replace('US', '')
            patent_id = fix_id(doc_title)
            tags['patent_id'] = patent_id
            tags['patent_date'] = patent_date
            #print('ID:' + doc_title)
            output_line = doc + '\t'
        except KeyError:
            print(sys.exc_info())
            #print(etree.tostringlist(xml))
            patent_id = 'No title found'
            # Skip this file; not a patent we care about
            continue
        print('Parsing %s from year %s and week %s: patent ID %s' % (str(curr_file), year, str(week), patent_id))
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
        # Write the values to a dictionary to be put into SQL table
        output_dict_list.append(tags)

write_success = False
while not write_success:
    try:
        conn.execute(patent_info.insert(), output_dict_list)
        print("Patents successfully inserted to database.")
        write_success = True
    except OperationalError:
        print('Error when writing. Error message:')
        print(sys.exc_info())
        print('Attempting to write again after pause.')
        sleep(random.random() * 2)
        print('Re-attempt execution.')
    except:
        print("Data NOT written to database. Error message:")
        print(sys.exc_info())
        write_success = True














