import os, re

# change this location depending on where the files are saved; ideally, this would be online
location = 'C:\Users\TTH\Dropbox\Data Science Team\Top 100 Transcripts'
dirset = os.listdir(location)
os.chdir(location)

# gets the company name given the header string by splitting the string at the year and then by 'Earnings'
def get_name(earnings_call_string):
    split_text_by_year = re.split("\d{2,4}", earnings_call_string)
    if len(split_text_by_year) is not 2:
        return "Name not found as no year was found"
    return split_text_by_year[1].strip()

# gets the year given the header string by regex matching 4 digits or adding '20' in front of 2 digits
def get_year(earnings_call_string):
    four_nums = re.compile("\d{4}")
    if four_nums.search(earnings_call_string) is not None:
        return four_nums.search(earnings_call_string).group(0)
    two_nums = re.compile("\d{2}")
    if two_nums.search(earnings_call_string) is not None:
        return "20" + two_nums.search(earnings_call_string).group(0)
    return "No year found"

# gets the quarter given the header string by splitting the string at the year
def get_quarter(earnings_call_string):
    split_text_by_year = re.split("\d{2,4}", earnings_call_string)
    if len(split_text_by_year) is not 2:
        return "Quarter not found as no year was found"
    return split_text_by_year[0].strip()

# Open the file to write
output = open('C:\Users\TTH\Documents\College Freshman Year\PURM Summer Stuff\Programming Workspace\EarningCall\\test.txt', 'w')

index = 0

# Iterate through every file in the directory
for trans in dirset:
    f = open(trans,'rb')
    file_content1 = f.read()
    f.close()
    file_content2 = file_content1.split('<DOCFULL> -->')

    # Iterate through every report in each file
    for quart in file_content2[1:len(file_content2)]:

        # Extract the header from each document
        start = '<DIV CLASS="c5"><P CLASS="c6"><SPAN CLASS="c7">'
        end = '</SPAN>'
        earnings_call_string = quart[quart.find(start):quart.find(end, quart.find(start))].replace('<DIV CLASS="c5"><P CLASS="c6"><SPAN CLASS="c7">', '')

        # Date is obtained without using the header
        start = '<DIV CLASS="c3"><P CLASS="c1"><SPAN CLASS="c4">'
        end = '</SPAN></P>'
        date = quart[quart.find(start):quart.find(end,quart.find(start))].replace('<DIV CLASS="c3"><P CLASS="c1"><SPAN CLASS="c4">','').replace('</SPAN><SPAN CLASS="c2">',' ')

        # Process the title to find the quarter, year, and company name
        quarter = get_quarter(earnings_call_string)
        year = get_year(earnings_call_string)
        name = get_name(earnings_call_string)

        # Extract the full text body from each document by removing tags
        text_body = re.sub('<(\/?)[a-zA-Z0-9=\"_ ]*>','',quart)
        text_body2 = re.sub('<!--[a-zA-Z0-9=<>_\n\"? ]*-->','',text_body).replace('<!-- Hide XML section from browser','')

        # Remove new lines from text body
        text_body2 = text_body2.replace('\n', '')

        # Write to file
        outputline = dirset[index].replace('.HTML','') + '\t' + \
                     name + '\t' + \
                     quarter + '\t' + \
                     year + '\t' + \
                     date + '\t' + \
                     text_body2
                     
        # Make a new line to separate output             
        output.write(outputline + '\n')

    index += 1

# Close the file
output.close()

#TODO: Filter for errors in the reports