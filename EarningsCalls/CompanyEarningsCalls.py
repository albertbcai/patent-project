import os, re, xlrd

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
    return "conference call" not in lowercase_string and ("earnings commentary" in lowercase_string or "event brief" in lowercase_string)

def remove_tags(text):
    tagless_text = re.sub('<(\/?)[a-zA-Z0-9=\"_ ]*>', '', text)
    uncommented_text = re.sub('<!--[a-zA-Z0-9=<>_\n\"? ]*-->', '', tagless_text).replace('<!-- Hide XML section from browser', '')
    uncommented_text = uncommented_text.replace('&amp;', '&')
    return uncommented_text

def should_be_filtered(header):
    # too long of a header indicates article is garbage
    if (len(header) > 100):
        return True
    else:
        return False
    
# fixes the quarter by replacing anomalies
def fix_quarter(quarter):
    fixed_quarter = quarter.replace('1Q', 'Q1')
    fixed_quarter = fixed_quarter.replace('2Q', 'Q2')
    fixed_quarter = fixed_quarter.replace('2Q', 'Q2')
    fixed_quarter = fixed_quarter.replace('3Q', 'Q3')
    fixed_quarter = fixed_quarter.replace('4Q', 'Q4')
    fixed_quarter = fixed_quarter.replace('Accenture', '')
    fixed_quarter = fixed_quarter.replace('Ford Motor', '')
    fixed_quarter = fixed_quarter.replace('General Electric', '')
    fixed_quarter = fixed_quarter.replace('\'', '')
    fixed_quarter = fixed_quarter.replace('Full Year', 'FY')
    fixed_quarter = fixed_quarter.replace('Half Year', 'HY')
    fixed_quarter = fixed_quarter.replace('First Quarter', 'Q1')
    fixed_quarter = fixed_quarter.replace('Second Quarter', 'Q2')
    fixed_quarter = fixed_quarter.replace('Third Quarter', 'Q3')
    fixed_quarter = fixed_quarter.replace('Fourth Quarter', 'Q4')
    fixed_quarter = fixed_quarter.replace('1st Quarter', 'Q1')
    fixed_quarter = fixed_quarter.replace('2nd Quarter', 'Q2')
    fixed_quarter = fixed_quarter.replace('3rd Quarter', 'Q3')
    fixed_quarter = fixed_quarter.replace('4th Quarter', 'Q4')
    fixed_quarter = fixed_quarter.replace('Report', '')
    fixed_quarter = fixed_quarter.replace('FullYear', 'FY')
    return fixed_quarter
        
def name_is_similar_enough(correct_name, input_name):
    if string_similarity(correct_name, input_name) > 0.42:
        return True
    else:
        return False

def get_bigrams(string):
    '''
    Takes a string and returns a list of bigrams
    '''
    s = string.lower()
    return [s[i:i+2] for i in xrange(len(s) - 1)]

def string_similarity(str1, str2):
    '''
    Perform bigram comparison between two strings
    and return a percentage match in decimal form
    '''
    pairs1 = get_bigrams(str1)
    pairs2 = get_bigrams(str2)
    union  = len(pairs1) + len(pairs2)
    hit_count = 0
    for x in pairs1:
        for y in pairs2:
            if x == y:
                hit_count += 1
                break
    return (2.0 * hit_count) / union

# Open the file to write
output = open('C:\Users\TTH\Documents\College Freshman Year\PURM Summer Stuff\Programming Workspace\EarningCall\\test.txt', 'w')

index = 0

excel_location = 'C:\Users\TTH\Documents\College Freshman Year\PURM Summer Stuff\Programming Workspace\EarningCall'

#Make dictionary of top 100 companies' tickers to standardized names
d = {}
wb = xlrd.open_workbook(excel_location + '\\' + 'S&P 500.xlsx')
sh = wb.sheet_by_index(1)
for i in range(2, 506):
    cell_value_class = sh.cell(i,0).value.encode('utf8')
    cell_value_id = sh.cell(i,1).value.encode('utf8')
    d[cell_value_class] = cell_value_id
   
# Iterate through every file in the directory
for trans in dirset:
    f = open(trans,'rb')
    file_content1 = f.read()
    f.close()
    file_content2 = file_content1.split('<DOCFULL> -->')
    
    ticker = dirset[index].replace('.HTML','')
    standard_name = ''
    if ticker not in d:
        standard_name = '$$$'
    else:
        standard_name = d[dirset[index].replace('.HTML','')]
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
        
        # Throw out commentaries and event briefs
        if is_earnings_commentary(full_header):
            continue
        

        # Date is obtained without using the header
        start = '<DIV CLASS="c3"><P CLASS="c1"><SPAN CLASS="c4">'
        end = '</SPAN></P>'
        date = remove_tags(quart[quart.find(start):quart.find(end,quart.find(start))])

        # Process the title to find the quarter, year, and company name
        quarter = get_quarter(earnings_call_string).replace('&amp;', '&')
        year = get_year(full_header)
        name = get_name(earnings_call_string).replace('&amp;', '&')

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
            
        # if ticker wasn't found, consider the first name to be the standard name    
        if standard_name is '$$$':
            standard_name = name

        if not name_is_similar_enough(standard_name, name):
            continue
        name = standard_name

        # Start the text body at LENGTH (if it exists) and end at 'reserves the rights to make changes to documents'
        start = "LENGTH"
        if quart.find(start) is -1:
            start = full_header.split(" ")[-1]
        end = 'reserves the right to make changes to documents'
        text_body = quart[quart.find(start):quart.find(end)]

        # Extract the full text body from each document by removing tags
        text_body = remove_tags(text_body)

        # Remove new lines from text body
        text_body = text_body.replace('\n', '')

        # Fix the quarters
        quarter = fix_quarter(quarter)
        
        # Write to file
        outputline = dirset[index].replace('.HTML','') + '\t' + \
                     name + '\t' + \
                     quarter + '\t' + \
                     year + '\t' + \
                     date + '\t' + \
                     text_body
                     
        # Make a new line to separate output             
        output.write(outputline + '\n')

    index += 1

# Close the file
output.close()