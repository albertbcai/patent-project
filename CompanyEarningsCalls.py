from bs4 import BeautifulSoup
from os import listdir
from os.path import isfile, join
import re

location = "C:\Users\TTH\Dropbox\Data Science Team\Top 100 Transcripts"


def make_new_soup(file_name):
    beautiful_soup = BeautifulSoup(open(location + "\\" + file_name), "html.parser")
    return beautiful_soup


def get_company_name(file_name):
    soup = make_new_soup(file_name)
    mydivs = soup.findAll("div", {"class": "c5"})
    header_text = str(mydivs[0].text)
    split_text_by_year = re.split("\d{4}", header_text)
    title = re.split("Earnings", split_text_by_year[1])
    return title[0].strip()


file_name_list = [f for f in listdir(location) if isfile(join(location, f))]

company_names_list = []

for file_name in file_name_list:
    company_names_list.append(get_company_name(file_name))

print company_names_list