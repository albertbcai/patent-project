from bs4 import BeautifulSoup, Comment
from os import listdir
from os.path import isfile, join
import re

location = "C:\Users\TTH\Dropbox\Data Science Team\Top 100 Transcripts"

# makes a new BeautifulSoup object with no comments. 
def make_new_soup(file_name):
    # "lxml" is much faster than html.parser but for some reason was crashing on this machine
    beautiful_soup = BeautifulSoup(open(location + "\\" + file_name), "html.parser")
    # removes the comments from the html
    comments = beautiful_soup.findAll(text=lambda text:isinstance(text, Comment))
    [comment.extract() for comment in comments]

    return beautiful_soup

# gets the company name given the header string by splitting the string at the year and then by 'Earnings'
def get_name(earnings_call_string):
    split_text_by_year = re.split("\d{2,4}", earnings_call_string)
    if len(split_text_by_year) is not 2:
        return "Name not found as no year was found"
    title = re.split("Earnings", split_text_by_year[1])
    if len(title) is not 2:
        return "Name not found as Earnings appeared several times"
    return title[0].strip()

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
    
# helper function to check if the given c5 tag is actually the header by seeing if it has exactly 3 children
def check_c5_is_earnings_call_string(tag):
    first_child = tag.contents[0]
    if len(first_child.contents) == 3:
        return True
    else:
        return False

# returns the header strings which are found by looking for div class = 'c5'    
def get_earnings_call_strings(beautiful_soup):
    c5_tags_list = soup.findAll("div", {"class": "c5"})
    earnings_call_strings = [t.text.encode('utf-8') for t in c5_tags_list if check_c5_is_earnings_call_string(t)]
    return earnings_call_strings

# helper function that given start and end tags concatenates every string between them. Note comments must be removed
def loop_until_next_tag(text, current_tag, next_tag):
        if (str(type(current_tag)) == "<class 'bs4.element.NavigableString'>"):
            text += current_tag.string.encode('utf-8')
            return loop_until_next_tag(text, current_tag.next, next_tag)
        if current_tag.next is next_tag:
            return text
        else:
            return loop_until_next_tag(text, current_tag.next, next_tag)

''' 
Gets all text bodies given the header TAG (not string). Unfortunately does not work entirely because
the text body corresponding to a bad header (i.e. not a real earnings call) will be absorbed in the 
previous header's text body.
'''
def get_text_body(earnings_call_string, stop_tag):
    return loop_until_next_tag("", earnings_call_string, stop_tag)  

def handle_last_text_body(earnings_call_string, beautiful_soup):
    #note that by getting page bodies within earnings call strings we miss the last page body
    last_tag = beautiful_soup.findAll("div")[-1]
    return loop_until_next_tag("", earnings_call_string, last_tag)
    
# to be iterated over with each value being put into the make_new_soup function
file_name_list = [f for f in listdir(location) if isfile(join(location, f))]

# for testing we only use the first file
soup = make_new_soup(file_name_list[0])

earnings_call_strings = get_earnings_call_strings(soup)

# TODO: iterate over every file. Currently we are just iterating over Apple's file
for i in range(len(earnings_call_strings)):
    current_header = earnings_call_strings[i]
    company_name = get_name(current_header)
    year = get_year(current_header)
    quarter = get_quarter(current_header)
    text_body = ""
    # the last text body is a corner case to be handled
    if (i is not len(earnings_call_strings - 1)):
        text_body = get_text_body(current_header, earnings_call_strings[i + 1])
    else:
        text_body = handle_last_text_body(current_header, soup)
    #TODO: put all these variables (name, year, quarter, etc.) in a CSV
