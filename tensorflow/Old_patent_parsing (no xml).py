import zipfile
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
import os
import sys

# Dictionary of all tags
tags = {'PATNWKU': '',
        'PATNSRC': '',
        'PATNAPN': '',
        'PATNAPT': '',
        'PATNART': '',
        'PATNAPD': '',
        'PATNTTL': '',
        'PATNISD': '',
        'PATNNCL': '',
        'PATNECL': '',
        'PATNEXP': '',
        'PATNEXA': '',
        'PATNNDR': '',
        'PATNNFG': '',
        'PATNTRM': '',
        'PATNPBL': '',
        'INVTNAM': '',
        'INVTSTR': '',
        'INVTCTY': '',
        'INVTSTA': '',
        'INVTZIP': '',
        'ASSGNAM': '',
        'ASSGCTY': '',
        'ASSGSTA': '',
        'ASSGCOD': '',
        'REISCOD': '',
        'REISAPN': '',
        'REISAPD': '',
        'REISPNO': '',
        'REISISD': '',
        'CLASOCL': '',
        'CLASXCL': '',
        'RLAPCOD': '',
        'RLAPAPN': '',
        'RLAPAPD': '',
        'RLAPPSC': '',
        'RLAPPNO': '',
        'RLAPISD': '',
        'CLASEDF': '',
        'CLASICL': '',
        'CLASFSC': '',
        'CLASFSS': '',
        'UREFPNO': '',
        'UREFISD': '',
        'UREFNAM': '',
        'UREFOCL': '',
        'UREFXCL': '',
        'LREPFRM': '',
        'ABSTPAL': '',
        'BSUMPAC': '',
        'BSUMPAR': '',
        'BSUMTBL': '',
        'DRWDPAC': '',
        'DRWDPAR': '',
        'DETDPAC': '',
        'DETDPAR': '',
        'CLMSSTM': '',
        'CLMSNUM': '',
        'CLMSPAR': '',
        'DCLMPAR': '',
        'FREFPNO': '',
        'FREFISD': '',
        'FREFCNT': '',
        'FREFOCL': '',
        'PARNPAC': '',
        'PARNPAR': '',
        'CLMSPA1': '',
        }

# Initialize the SQL tables
table_name = 'patent_info_old_patents'
db = create_engine('mysql://root:patentproject@localhost:3306/patent_parsing', echo=False)
Session = sessionmaker(bind=db)
session = Session()
db.echo = False  # Try changing this to True and see what happens
metadata = MetaData(db)
print('Creating table.')

patent_info = \
    Table(table_name, metadata,
          Column('PATNWKU', String(50), primary_key=True),
          Column('PATNSRC', TEXT),
          Column('PATNAPN', TEXT),
          Column('PATNAPT', TEXT),
          Column('PATNART', TEXT),
          Column('PATNAPD', TEXT),
          Column('PATNTTL', TEXT),
          Column('PATNISD', TEXT),
          Column('PATNNCL', TEXT),
          Column('PATNECL', TEXT),
          Column('PATNEXP', TEXT),
          Column('PATNNDR', TEXT),
          Column('PATNNFG', TEXT),
          Column('PATNTRM', TEXT),
          Column('PATNPBL', TEXT),
          Column('PATNEXA', TEXT),
          Column('INVTNAM', TEXT),
          Column('INVTSTR', TEXT),
          Column('INVTCTY', TEXT),
          Column('INVTSTA', TEXT),
          Column('INVTZIP', TEXT),
          Column('ASSGNAM', TEXT),
          Column('ASSGCTY', TEXT),
          Column('ASSGSTA', TEXT),
          Column('ASSGCOD', TEXT),
          Column('REISCOD', TEXT),
          Column('REISAPN', TEXT),
          Column('REISAPD', TEXT),
          Column('REISPNO', TEXT),
          Column('REISISD', TEXT),
          Column('CLASXCL', TEXT),
          Column('RLAPCOD', TEXT),
          Column('RLAPAPN', TEXT),
          Column('RLAPAPD', TEXT),
          Column('RLAPPSC', TEXT),
          Column('RLAPPNO', TEXT),
          Column('RLAPISD', TEXT),
          Column('CLASOCL', TEXT),
          Column('CLASEDF', TEXT),
          Column('CLASICL', TEXT),
          Column('CLASFSC', TEXT),
          Column('CLASFSS', TEXT),
          Column('UREFPNO', TEXT),
          Column('UREFISD', TEXT),
          Column('UREFNAM', TEXT),
          Column('UREFOCL', TEXT),
          Column('UREFXCL', TEXT),
          Column('LREPFRM', TEXT),
          Column('ABSTPAL', TEXT),
          Column('BSUMPAC', TEXT),
          Column('BSUMPAR', TEXT),
          Column('BSUMTBL', TEXT),
          Column('DRWDPAC', TEXT),
          Column('DRWDPAR', TEXT),
          Column('DETDPAC', TEXT),
          Column('DETDPAR', TEXT),
          Column('CLMSSTM', TEXT),
          Column('CLMSNUM', TEXT),
          Column('CLMSPAR', TEXT),
          Column('DCLMPAR', TEXT),
          Column('FREFPNO', TEXT),
          Column('FREFISD', TEXT),
          Column('FREFCNT', TEXT),
          Column('FREFOCL', TEXT),
          Column('PARNPAC', TEXT),
          Column('PARNPAR', TEXT),
          Column('CLMSPA1', TEXT),
          )
try:
    patent_info.create()
except:
    print(sys.exc_info())
patent_info = Table(table_name, metadata, autoload=True)

delimiter = "|"
curr_patent = 1


path = os.path.expanduser("~/Desktop/patent_parsing/")
year_list = range(1976, 2001)
year_list = [1976, 1977, 1978]
for year in year_list:
    year = str(year)
    os.chdir(path)
    if not os.path.exists(year):
        os.mkdir(year)
    os.chdir(year)
    curr_week = 0
    for f in os.listdir(path+year):
        if f == '.DS_Store':
            continue
        curr_week += 1
        archive = zipfile.ZipFile(f, 'r')
        filelist = archive.namelist()
        for file in filelist:
            f = archive.open(file)
            line = f.readline()
            line = f.readline()
            header_tag = 'PATN'
            # Iterate through every line in the file
            line_number = 1
            while line != '':
                line_number += 1
                tag = line[0:4].strip()
                # See new PATN, write to SQL table and reset dictionary
                if tag == "PATN" and line_number > 5:
                    for tag_dict in tags:
                        if tags[tag_dict].startswith(delimiter):
                            tags[tag_dict] = tags[tag_dict][len(delimiter):]
                    try:
                        patent_info.insert().execute(
                            PATNWKU=tags['PATNWKU'],
                            PATNSRC=tags['PATNSRC'],
                            PATNAPN=tags['PATNAPN'],
                            PATNAPT=tags['PATNAPT'],
                            PATNART=tags['PATNART'],
                            PATNAPD=tags['PATNAPD'],
                            PATNTTL=tags['PATNTTL'],
                            PATNISD=tags['PATNISD'],
                            PATNNCL=tags['PATNNCL'],
                            PATNECL=tags['PATNECL'],
                            PATNEXP=tags['PATNEXP'],
                            PATNNDR=tags['PATNNDR'],
                            PATNNFG=tags['PATNNFG'],
                            PATNTRM=tags['PATNTRM'],
                            PATNPBL=tags['PATNPBL'],
                            PATNEXA=tags['PATNEXA'],
                            INVTNAM=tags['INVTNAM'],
                            INVTSTR=tags['INVTSTR'],
                            INVTCTY=tags['INVTCTY'],
                            INVTSTA=tags['INVTSTA'],
                            INVTZIP=tags['INVTZIP'],
                            ASSGNAM=tags['ASSGNAM'],
                            ASSGCTY=tags['ASSGCTY'],
                            ASSGSTA=tags['ASSGSTA'],
                            ASSGCOD=tags['ASSGCOD'],
                            REISCOD=tags['REISCOD'],
                            REISAPN=tags['REISAPN'],
                            REISAPD=tags['REISAPD'],
                            REISPNO=tags['REISPNO'],
                            REISISD=tags['REISISD'],
                            CLASOCL=tags['CLASOCL'],
                            CLASXCL=tags['CLASXCL'],
                            RLAPCOD=tags['RLAPCOD'],
                            RLAPAPN=tags['RLAPAPN'],
                            RLAPAPD=tags['RLAPAPD'],
                            RLAPPSC=tags['RLAPPSC'],
                            RLAPPNO=tags['RLAPPNO'],
                            RLAPISD=tags['RLAPISD'],
                            CLASEDF=tags['CLASEDF'],
                            CLASICL=tags['CLASICL'],
                            CLASFSC=tags['CLASFSC'],
                            CLASFSS=tags['CLASFSS'],
                            UREFPNO=tags['UREFPNO'],
                            UREFISD=tags['UREFISD'],
                            UREFNAM=tags['UREFNAM'],
                            UREFOCL=tags['UREFOCL'],
                            UREFXCL=tags['UREFXCL'],
                            LREPFRM=tags['LREPFRM'],
                            ABSTPAL=tags['ABSTPAL'],
                            BSUMPAC=tags['BSUMPAC'],
                            BSUMPAR=tags['BSUMPAR'],
                            BSUMTBL=tags['BSUMTBL'],
                            DRWDPAC=tags['DRWDPAC'],
                            DRWDPAR=tags['DRWDPAR'],
                            DETDPAC=tags['DETDPAC'],
                            DETDPAR=tags['DETDPAR'],
                            CLMSSTM=tags['CLMSSTM'],
                            CLMSNUM=tags['CLMSNUM'],
                            CLMSPAR=tags['CLMSPAR'],
                            DCLMPAR=tags['DCLMPAR'],
                            FREFISD=tags['FREFISD'],
                            FREFPNO=tags['FREFPNO'],
                            FREFCNT=tags['FREFCNT'],
                            FREFOCL=tags['FREFOCL'],
                            PARNPAC=tags['PARNPAC'],
                            PARNPAR=tags['PARNPAR'],
                            CLMSPA1=tags['CLMSPA1'],
                        )
                    except:
                        print(sys.exc_info())
                        print('error when entering patent_id: %s' % tags['PATNWKU'])
                    # Reset the dictionary
                    tags = {key: '' for key in tags}
                # Found a title tag
                if len(tag) == 4:
                    header_tag = tag
                # Found a sub tag
                elif len(tag) == 3:
                    combined_tag = header_tag + tag
                    if combined_tag not in tags:
                        print ("Missing tag %s in tags dictionary" % combined_tag)
                        tags[combined_tag] = ''
                    tags[combined_tag] += delimiter + line[4:-1]
                # Found a wrapped line from before
                elif len(tag) == 0:
                    tags[combined_tag] += line[4:-1]
                else:
                    raise ValueError('Did not find either a title_tag or a sub_tag.')
                line = f.readline()
            # Print progress
            print('Completed parsing from year %s and week %s' % (year, str(curr_week)))

