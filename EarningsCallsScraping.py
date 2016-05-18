import requests, os, re, xlrd

def check_if_page_exists(webpage):
    #Return True if a website does exist. Return False if it does not exist.
    if type(webpage) is not str:
        webpage = webpage.text.encode('utf8')
    error = '<title>Sorry, the page you are looking for was not found | Seeking Alpha</title>'
    if webpage.find(error) is not -1:
        return False
    else:
        return True




#Make list of top 500 companies' tickers
excel_location = '/Users/albertcai/Box Sync/Work/PURM'
company_list = []
wb = xlrd.open_workbook(excel_location + '/' + 'S&P 500.xlsx')
sh = wb.sheet_by_index(1)
for i in range(1, 506):
    ticker = sh.cell(i,0).value.encode('utf8')
    company_list.append(ticker)

# Iterate through every company on the list of the fortune 500
for ticker in company_list:
    index = 1
    page = 1
    # Keep searching transcripts until finding a website that doesn't exist
    while True:
        print ticker + str(page)
        location = 'http://seekingalpha.com/symbol/' + ticker + '/transcripts/' + str(page)
        webpage = requests.get(location, headers={'User-Agent': 'Mozilla/5.0'}).text.encode('utf8')
        if(not check_if_page_exists(webpage)): break
        article = '<a href="/article/'
        webpage_list = webpage.split(article)[1:]
        end = '" sasource="qp_transcripts">'
        for t in webpage_list:
            print(ticker + str(index))
            # Only search for specific transcript if it isn't already saved
            filename = ticker + str(index) + '.html'
            path = '/Users/albertcai/Box Sync/Work/PURM/transcripts/'
            if not os.path.isfile(path + filename) or os.stat(path + filename).st_size is 0:
                #Find each transcript online
                transcript_url = 'http://seekingalpha.com/article/' + t[:t.find(end)]
                transcript = requests.get(transcript_url, headers={'User-Agent': 'Mozilla/5.0'}).text.encode('utf8')
                if not os.path.exists(path):
                    os.makedirs(path)
                # Save transcript
                with open(os.path.join(path, filename), 'wb') as temp_file:
                    temp_file.write(transcript)
            # Increment the filename number
            index += 1
        #Increment the page number for searching
        page += 1


