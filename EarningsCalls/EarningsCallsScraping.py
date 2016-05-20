import requests, os, re, xlrd, time

# Return True if a website does exist. Return False if it does not exist.
def check_if_page_exists(webpage):
    if type(webpage) is not str:
        webpage = webpage.text.encode('utf8')
    error = '<title>Sorry, the page you are looking for was not found | Seeking Alpha</title>'
    if webpage.find(error) is not -1:
        return False
    else:
        return True

# Gets the header from the webpage.
def get_header(webpage):
    title = re.split('<(/?)title>',webpage)
    title = title[2].replace('39','')
    return title

# Checks if file is a transcript because it contains a quarter.
def is_transcript(webpage):
    header = get_header(webpage)
    quarter1 = re.compile("Q[0-9]",flags=re.IGNORECASE)
    quarter2 = re.compile("[0-9]Q",flags=re.IGNORECASE)
    webcast = re.compile("webcast",flags=re.IGNORECASE)
    if quarter1.search(header) is None and quarter2.search(header) is None:
        print "no quarter found"
        return False
    elif webcast.search(header) is not None:
        print "Found a webcast"
        return False
    else:
        return True

# Checks if file is correct company.
def is_correct_company(webpage, correct_ticker):
    webpage = webpage.split('<meta content="Seeking Alpha" name="application-name" /><meta content="true" name="HandheldFriendly" /><meta content="noodp,noydir" name="robots" /><meta content="')
    webpage = webpage[1]
    found_ticker = re.compile('\((NYSE:)?(NASDAQ:)?\w{1,5}\)', flags=re.IGNORECASE)
    found_ticker = found_ticker.search(webpage).group(0).replace('(','').replace(')','').replace('NYSE:','').replace('NASDAQ:','').strip()
    if correct_ticker == found_ticker:
        return True
    print "not correct company"
    return False

# Gets the company name from the webpage.
def get_name(webpage):
    header = get_header(webpage)
    name = header.split('(')
    name = name[0]
    replace_list = ['Seeking','Alpha','Management','Transcript','Earnings','Call','Transcript','Discusses','CEO','Results','39s','39']
    quarter = re.compile("Q[0-9]", flags=re.IGNORECASE)
    year = re.compile("\d{4}")
    punctuation = re.compile('\W')
    name = re.sub(quarter,'',name)
    name = re.sub(year,'',name)
    name = re.sub(punctuation,'',name)
    for word in replace_list:
        name = name.replace(word,'')
    return name.strip()

# Gets the year from the webpage.
def get_year(webpage):
    header = get_header(webpage)
    four_nums = re.compile("2\d{3}")
    if four_nums.search(header) is not None:
        return four_nums.search(header).group(0)
    else:
        two_nums = re.compile("[0-1]\d{1}")
        if two_nums.search(header) is not None:
            return "20" + two_nums.search(header).group(0)
        else:
            return "No year found"

#Gets the quarter from the webpage.
def get_quarter(webpage):
    header = get_header(webpage)
    test_quarter = re.compile("[0-9]Q",flags=re.IGNORECASE)
    if test_quarter.search(header) is not None:
        quarter1 = re.compile("[0-9]Q",flags=re.IGNORECASE)
        q = quarter1.search(header).group(0)
        if q == "1Q": return "Q1"
        if q == "2Q": return "Q2"
        if q == "3Q": return "Q3"
        if q == "4Q": return "Q4"
        return q
    quarter2 = re.compile("Q[0-9]",flags=re.IGNORECASE)
    return quarter2.search(header).group(0)

# Remove tags from text
def remove_tags(text):
    tagless_text = re.sub('<(\/?)[a-zA-Z0-9=\"_ ]*>', '', text)
    uncommented_text = re.sub('<!--[a-zA-Z0-9=<>_\n\"? ]*-->', '', tagless_text).replace('<!-- Hide XML section from browser', '')
    uncommented_text = uncommented_text.replace('&amp;', '&')
    return uncommented_text

# Gets the text body from the webpage.
def get_textbody(webpage):
    start = '<p class="p p1">'
    end = '<strong>Copyright policy: </strong>'
    text_body = webpage[webpage.find(start):webpage.find(end)]
    text_body = remove_tags(text_body).replace('\n',' ').replace('\t',' ')
    return text_body

# Gets the date published from the webpage
def get_date(webpage):
    date = webpage.split('itemprop="datePublished">')
    date = date[1].split('</time>')
    date = date[0]
    return date

# Save transcript file
def save_file(path,filename,file):
    if not os.path.exists(path):
        os.makedirs(path)
    with open(os.path.join(path, filename), 'wb') as temp_file:
        temp_file.write(file)
    return None

def name_is_similar_enough(correct_name, input_name):
    similar = string_similarity(correct_name,input_name)
    print(correct_name)
    print(input_name)
    print(similar)
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

location = '/Users/albertcai/Box Sync/Work/PURM/'

'''Part 1: Scrape data from online'''
'''
#Make list of top 500 companies' tickers
company_list = []
wb = xlrd.open_workbook(location +'S&P 500.xlsx')
sh = wb.sheet_by_index(1)
for i in range(1, 506):
    ticker = sh.cell(i,0).value.encode('utf8')
    company_list.append(ticker)

# Iterate through every company on the list of the fortune 500
for ticker in reversed(company_list):
    print ticker
    index = 1
    page = 1
    # Keep searching transcripts until finding a website that doesn't exist
    while True:
        location = 'http://seekingalpha.com/symbol/' + ticker + '/transcripts/' + str(page)
        webpage = requests.get(location, headers={'User-Agent': 'Mozilla/5.0'}).text.encode('utf8')
        if(not check_if_page_exists(webpage)): break
        article = '<a href="/article/'
        webpage_list = webpage.split(article)[1:]
        end = '" sasource="qp_transcripts">'
        for t in webpage_list:
            filename = ticker + str(index) + '.html'
            path = '/Users/albertcai/Box Sync/Work/PURM/transcripts/'
            # Only search for specific transcript if it isn't already saved
            if not os.path.isfile(path + filename) or os.stat(path + filename).st_size is 0:
                #Find each transcript online
                transcript_url = 'http://seekingalpha.com/article/' + t[:t.find(end)]
                transcript = requests.get(transcript_url, headers={'User-Agent': 'Mozilla/5.0'}).text.encode('utf8')
                print filename

                # Wait to let page load
                time.sleep(0)

                # Save file
                save_file(path,filename,transcript)

            # Increment the filename number
            index += 1
        #Increment the page number for searching
        page += 1
'''


#Part 2: Put data into spreadsheet

# Open the file to write
output = open(location + 'test.txt','w')

#Make dictionary of top 100 companies' tickers to standardized names
d = {}
wb = xlrd.open_workbook(location + 'S&P 500.xlsx')
sh = wb.sheet_by_index(1)
for i in range(1, 506):
    cell_value_class = sh.cell(i,0).value.encode('utf8')
    cell_value_id = sh.cell(i,1).value.encode('utf8')
    d[cell_value_class] = cell_value_id

dirset = os.listdir(location + 'transcripts')
os.chdir(location + 'transcripts')
i = -1
for transcript in dirset:
    i += 1
    print(i)
    f = open(transcript, 'rb')
    transcript = f.read()
    f.close()
    l = len(transcript)

    # Skip the DS.Store file or other files that are too small
    if l < 20000 :
        continue
    #Don't extract data if not a transcript
    if not is_transcript(transcript):
        continue

    # Don't extract data if not correct ticker
    ticker = re.sub('\d{1}', '', dirset[i]).replace('.html', '')
    if not is_correct_company(transcript,ticker):
        continue

    # Extract data from the transcript
    quarter = get_quarter(transcript)
    year = get_year(transcript)
    name = get_name(transcript)
    date = get_date(transcript)
    text_body = get_textbody(transcript)

    # Standardize the names
    standard_name = d[ticker]
    name = standard_name

    #Write to file
    outputline = ticker + '\t' + \
                 name + '\t' + \
                 quarter + '\t' + \
                 year + '\t' + \
                 date + '\t' + \
                 text_body
    output.write(outputline + '\n')

    print(ticker + '\t' + \
          name + '\t' + \
          quarter + '\t' + \
          year + '\t' + \
          date + '\t')
# Close the file
output.close()