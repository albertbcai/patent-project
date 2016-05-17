from pandas import *
import os, re

# change this location depending on where the files are saved; ideally, this would be online

location = 'C:\Users\TTH\Dropbox\Data Science Team\Top 100 Transcripts'
dirset = os.listdir(location)
os.chdir(location)

# gets the company name given the header string by splitting the string at the year and then by 'Earnings'
def get_name(earnings_call_string):
    split_text_by_year = re.split("\d{2,4}", earnings_call_string)
    if len(split_text_by_year) is not 2:
        return "Name not found"
    else:
        return split_text_by_year[1].strip()

# gets the year given the header string by regex matching 4 digits or adding '20' in front of 2 digits
def get_year(earnings_call_string):
    four_nums = re.compile("\d{4}")
    if four_nums.search(earnings_call_string) is not None:
        return four_nums.search(earnings_call_string).group(0)
    else:
        two_nums = re.compile("\d{2}")
        if two_nums.search(earnings_call_string) is not None:
            return "20" + two_nums.search(earnings_call_string).group(0)
        else:
            return "No year found"

# gets the quarter given the header string by splitting the string at the year
def get_quarter(earnings_call_string):
    split_text_by_year = re.split("\d{2,4}", earnings_call_string)
    if len(split_text_by_year) is not 2:
        return "Quarter not found"
    else:
        return split_text_by_year[0].strip()

def is_earnings_commentary(earnings_call_string):
    lowercase_string = earnings_call_string.lower()
    return "earnings commentary" in lowercase_string

def remove_tags(text):
    tagless_text = re.sub('<(\/?)[a-zA-Z0-9=\"_ ]*>', '', text)
    uncommented_text = re.sub('<!--[a-zA-Z0-9=<>_\n\"? ]*-->', '', tagless_text).replace('<!-- Hide XML section from browser', '')
    return uncommented_text

def should_be_filtered(header):
    # too long of a header indicates article is garbage
    if (len(header) > 100):
        return True
    else:
        return False
    

# Open the file to write
output = open('C:\Users\TTH\Documents\College Freshman Year\PURM Summer Stuff\Programming Workspace\EarningCall\\test.txt', 'w')

index = 0

excel_location = 'C:\Users\TTH\Documents\College Freshman Year\PURM Summer Stuff\Programming Workspace\EarningCall'

#Make dictionary of top 100 companies' tickers to standardized names
xls = ExcelFile(excel_location + '\\' + 'S&P 500.xls')
df = xls.parse(xls.sheet_names[0])
print df.to_dict()

# Iterate through every file in the directory
for trans in dirset:
    f = open(trans,'rb')
    file_content1 = f.read()
    f.close()
    file_content2 = file_content1.split('<DOCFULL> -->')
    if "WAG.HTML" in trans:
        pass

    # Iterate through every report in each file
    for quart in file_content2[1:len(file_content2)]:

        # Extract the partial header from each document
        start = '<DIV CLASS="c5"><P CLASS="c6"><SPAN CLASS="c7">'
        end = '</SPAN>'
        earnings_call_string = remove_tags(quart[quart.find(start):quart.find(end, quart.find(start))])

        #Extract the whole header for filtering purposes
        start = '<DIV CLASS="c5"><P CLASS="c6"><SPAN CLASS="c7">'
        end = '</P>'
        full_header = remove_tags(quart[quart.find(start):quart.find(end, quart.find(start))])
        
        if (should_be_filtered(full_header)):
            continue
        
        # Determine if it is an earnings commentary or conference
        commentary_flag = is_earnings_commentary(full_header)

        # Date is obtained without using the header
        start = '<DIV CLASS="c3"><P CLASS="c1"><SPAN CLASS="c4">'
        end = '</SPAN></P>'
        date = remove_tags(quart[quart.find(start):quart.find(end,quart.find(start))])

        # Process the title to find the quarter, year, and company name
        quarter = get_quarter(earnings_call_string)
        year = get_year(full_header)
        name = get_name(earnings_call_string)

        # indicates name came before quarter and year
        if name is '':
            only_name = re.sub('\d*Q\d*', '', earnings_call_string)
            only_name = re.sub('\d*', '', only_name)
            only_name = re.sub('(Quarter)?', '', only_name)
            only_name = re.sub('(First)?', '', only_name)
            only_name = re.sub('(Second)?', '', only_name)
            only_name = re.sub('(Third)?', '', only_name)
            only_name = re.sub('(Fourth)?', '', only_name)            
            name = only_name.strip()
            
            quarter = re.sub('\d{2,4}', '', earnings_call_string)
            quarter = quarter.replace(name, '').strip()

        # Start the text body at 'LENGTH' and end at 'reserves the rights to make changes to documents'
        start = 'LENGTH:'
        end = 'reserves the right to make changes to documents'
        text_body = quart[quart.find(start):quart.find(end)]

        # Extract the full text body from each document by removing tags
        text_body = remove_tags(text_body)

        # Remove new lines from text body
        text_body = text_body.replace('\n', '')

        # Write to file
        outputline = dirset[index].replace('.HTML','') + '\t' + \
                     name + '\t' + \
                     quarter + '\t' + \
                     year + '\t' + \
                     date + '\t' + \
                     str(commentary_flag) + '\t' + \
                     text_body
                     
        # Make a new line to separate output             
        output.write(outputline + '\n')

    index += 1

# Close the file
output.close()