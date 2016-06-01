from lxml import etree
import StringIO
import re
import codecs
import os
from collections import defaultdict
import xmltodict
import subprocess
from sets import Set

def etree_to_dict(t):
    d = {t.tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                dd[k].append(v)
        d = {t.tag: {k:v[0] if len(v) == 1 else v for k, v in dd.items()}}
    if t.attrib:
        d[t.tag].update(('@' + k, v) for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
              d[t.tag]['#text'] = text
        else:
            d[t.tag] = text
    return d

# Take in test file as string

#location = '/Users/albertcai/Desktop/Patents/ipg160209 copy.xml'
location = '/Users/albertcai/Desktop/test_xml.xml'



def listXmls(file):
    self = open(file)
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

def find_all_children(elem, layer):
    '''Finds all the children of each patent and stores them to a file'''
    if type(elem.text) is not str and type(elem.text) is not unicode:
        text = '\n'
    else:
        text = elem.text + '\n'

    # Make output look prettier
    if text == '\n\n':
        text = '\n'

    #Try to write the output line
    try: outputline = '\t'*layer + elem.tag + ':' + text
    except TypeError:
        #print('Text not encoded because of a type error.')
        outputline = '\t'*layer + 'TypeError in text or tag\n'
    #print(outputline)

    try: output.write(outputline)
    except UnicodeEncodeError:
        #print('Character not encoded; do not output line')
        output.write('\t'*layer + elem.tag + ':' + 'Encoding error on this line\n')

    # Base case for recursion: element has no more children
    if not len(elem.getchildren()):
        return None

    # Recursive case: traverse the remaining children
    else:
        layer += 1
        for e in elem.getchildren():
            tag_set.add(elem.tag)
            # Test if root has children and search them recursively
            if len(elem) != 0:
                find_all_children(e, layer)
        return None

output_file = open('/Users/albertcai/Desktop/test2.txt','w')

curr_file = 0

# Initialize the SQL tables


# Initialize the separate output files
output_location = '/Users/albertcai/Desktop/Test_Patent/'
output_inventors = open(output_location + 'inventors_list.txt', 'w')
output_citations = open(output_location + 'citations_lists.txt', 'w')
output_assignees = open(output_location + 'assignees_lists.txt', 'w')
output_examiners = open(output_location + 'examiners_lists.txt', 'w')
output_classification = open(output_location + 'classification_lists.txt', 'w')
output_description = open(output_location + 'description_lists.txt', 'w')
output_abstract = open(output_location + 'abstract_lists.txt', 'w')
output_claim = open(output_location + 'claim_lists.txt', 'w')

# Initialize the sql tables
#import SQL_Initialize
#SQL_Initialize

# saved string to help with concatenating the data
global saved_string
# saved text strings to be concatenated for the end
global saved_description
global saved_claim

def stringify_children(node):
    s = node.text
    if s is None:
        s = ''
    s += etree.tostring(node)
    return s

def output_to_file(output_line, elem):
    try: output_line = output_line + '\t' + elem.tag + '\t' + elem.text + '\n'
    except TypeError:
        output_file.write('type error\n')
        return
    doc_title = output_line.split('\t')[0].split('-')[0].replace('US', '')
    global saved_claim
    global saved_description
    if saved_claim == '':
        saved_claim = doc_title
    if saved_description == '':
        saved_description = doc_title
    if elem.tag == 'b':
        return
    try:
        # Write to inventor list
        if output_line.find('inventor') != -1:
            if output_line.find('last-name') != -1:
                global saved_string
                saved_string = ' ' + output_line.split('last-name')[1]
            if output_line.find('first-name') != -1:
                output_line = doc_title + output_line.split('first-name')[1].replace('\n', '') + saved_string.replace('\t', '')
                output_inventors.write(output_line)
        # Write to the claim text
        if output_line.find('claim-text') != -1:
            saved_claim += "".join(elem.itertext()) + " "
        # Write to the description list
        if output_line.find('description') != -1:
            output_line = output_line.split('\tp\t')[1]
            try: output_file.write(output_line)
            except UnicodeEncodeError:
                return
            saved_description += output_line
        # Write to citations list
        if output_line.find('citation') != -1:
            if output_line.find('doc-number') != -1:
                output_line = doc_title + output_line.split('doc-number')[1]
                output_citations.write(output_line)
        # Write to assignees list
        if output_line.find('assignee') != -1:
            output_line = doc_title + output_line.split('orgname')[1]
            output_assignees.write(output_line)
        # Write to examiner list
        if output_line.find('primary-examiner') != -1:
            if output_line.find('last-name') != -1:
                saved_string = ' ' + output_line.split('last-name')[1]
            if output_line.find('first-name') != -1:
                output_line = doc_title + output_line.split('first-name')[1].replace('\n', '') + saved_string.replace(
                    '\t', '')
                output_examiners.write(output_line)
        # Write to classification list
        if output_line.find('main-cpc') != -1:
            output_line = doc_title + '\t' + output_line.split('\t')[-1]
            if len(output_line) > 30 or output_line.find('claim') != -1:
                return None
            output_classification.write(output_line)
        # Write to abstract list
        if output_line.find('abstract') != -1:
            abstract = output_line.split('\t')[-1]
            if len(abstract) < 10:
                return None
            output_line = doc_title + '\t' + abstract
            output_abstract.write(output_line)
    except UnicodeEncodeError:
        output_file.write('unicode error\n')
        return None
    except IndexError:
        output_file.write('list index error\n')
    return None


def remove_tags(text):
    tagless_text = re.sub('<(\/?)[a-zA-Z0-9=\"_ \t]*>', '', text)
    uncommented_text = re.sub('<!--[a-zA-Z0-9=<>_\n\"? ]*-->', '', tagless_text).replace('<!-- Hide XML section from browser', '')
    uncommented_text = uncommented_text.replace('&amp;', '&')
    return uncommented_text


def get_description(text):
    start = 'DETAILED DESCRIPTION</heading>'
    end = '<?DETDESC description="Detailed Description" end="tail"?>'
    description = remove_tags(text[text.find(start):text.find(end, text.find(start))])
    #print description
    return description

def parse_xml_as_text(xml_total):
    xml_list = xml_total.split('<?xml version="1.0" encoding="UTF-8"?>')
    for xml in xml_list:
        description = get_description(xml)

for xml in listXmls(location):
    saved_string = ''
    saved_description = ''
    saved_claim = ''
    curr_file += 1
    print('Document ' + str(curr_file) + ' is being parsed.')
    events = ("start", "end")
    context = etree.iterparse(xml, events=events)
    try: doc = etree.parse(xml).getroot().attrib['file']
    except KeyError: doc = 'No title found'
    outputline = doc + '\t'
    word = re.compile('[a-zA-Z0-9]*')
    for action, elem in context:
        if action == 'start':
            print(elem.tag)
            print(elem.text)
        # If we find a beginning tag and there are more children, add field to output and delimit with a tab
        if action == 'start' and len(elem.getchildren()):
            outputline += elem.tag
        # If we find a beginning tag and there are no children, write the text field (if it is not None) and start
        # a new line of output, but do not change the outputline
        if action == 'start' and not len(elem.getchildren()):
            if type(elem.text) is not str and type(elem.text) is not unicode:
                text = ''
            else:
                text = elem.text
            if outputline.endswith('\t'):
                outputline = outputline[:-1]
            output_to_file(outputline, elem)
            # print(outputline + '\t' + e.tag + '\t' + text + '\n')
        # If we find an ending tag, the current field is done, so go back a tab and field in the outputline
        if action == 'end':
            if outputline.endswith('\t'):
                outputline = outputline[:-1]
            if type(elem.tag) is not str and type(elem.tag) is not unicode:
                continue
            if outputline.endswith('action-date'):
                outputline = outputline[:-len('action-date')]
            if outputline.endswith(elem.tag):
                outputline = outputline[:-len(elem.tag)]
            if outputline.endswith('\t'):
                outputline = outputline[:-1]
        outputline += '\t'
    output_claim.write(saved_claim)
    output_description.write(saved_description)





'''
    context2 = etree.iterwalk(elem, events=events)
    # Iterate through every element in the file
        for action, e in context2:
            print e.tag, action
            # If we find a beginning tag and there are more children, add field to output and delimit with a tab
            if action == 'start' and len(e.getchildren()):
                outputline += e.tag
            # If we find a beginning tag and there are no children, write the text field (if it is not None) and start
            # a new line of output, but do not change the outputline
            if action == 'start' and not len(e.getchildren()):
                if type(e.text) is not str and type(e.text) is not unicode:
                    text = ''
                else:
                    text = e.text
                if outputline.endswith('\t'):
                    outputline = outputline[:-1]
                try: output_file.write(outputline + '\t' + e.tag + '\t' + text + '\n')
                except UnicodeError: output_file.write('unicode error\n')
                except TypeError: output_file.write('type error\n')
                #print(outputline + '\t' + e.tag + '\t' + text + '\n')
            # If we find an ending tag, the current field is done, so go back a tab and field in the outputline
            if action == 'end':
                if outputline.endswith('\t'):
                    outputline = outputline[:-1]
                if type(e.tag) is not str and type(e.tag) is not unicode:
                    continue
                if outputline.endswith(e.tag):
                    outputline = outputline[:-len(e.tag)]
                if outputline.endswith('\t'):
                    outputline = outputline[:-1]
            outputline += '\t'
            '''

'''Write to a sql file'''

import sqlalchemy as sa

db = sa.create_engine('patent_database')
db.echo = False  # Try changing this to True and see what happens
metadata = sa.MetaData
abstracts = sa.Table('abstracts', metadata,
    sa.Column('patent_id', sa.String(40), primary_key=True),
    sa.Column('abstract', sa.String(40)),
)
abstracts.create()
i = abstracts.insert()
i.execute(patent_id='Mary', abstract=30)

s = abstracts.select()
rs = s.execute()

row = rs.fetchone()
print 'Id:', row[0]
print 'Abstract:', row['abstract']
print 'abstract', row.abstract
print 'abstract', row[abstracts.c.password]

for row in rs:
    print row.name, 'is', row.age, 'years old'