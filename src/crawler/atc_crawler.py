import requests
from bs4 import BeautifulSoup
import pandas as pd
import string
from tqdm import tqdm

ATC_DDD_URL = "https://www.whocc.no/atc_ddd_index/?code="

def getATC_DDD_Data(targetList, level):
    tempList = []
    for n in tqdm(targetList):
        req = requests.get(ATC_DDD_URL+n)
        soup = BeautifulSoup(req.text, 'html.parser')
        rowDataList = [i.find('a') for i in soup.findAll('b')]
        if level == 1:
            rowDataList =rowDataList[:level]
        else:
            rowDataList =rowDataList[level-1:]
        if rowDataList:
            for i in rowDataList:
                tempList.append((i['href'].split('&')[0].split('=')[1], i.text, ))
    return pd.DataFrame(tempList, columns=['href', 'text'])

ATC_L1 = getATC_DDD_Data(string.ascii_uppercase, 1)
ATC_L1.to_csv("processed/ATC_L1.csv", index=False)

ATC_L2 = getATC_DDD_Data(string.ascii_uppercase, 2)
ATC_L2.to_csv("processed/ATC_L2.csv", index=False)

ATC_L3 = getATC_DDD_Data(list(ATC_L2['href']), 3)
ATC_L3.to_csv("processed/ATC_L3.csv", index=False)

ATC_L4 = getATC_DDD_Data(list(ATC_L3['href']), 4)
ATC_L4.to_csv("processed/ATC_L4.csv", index=False)

ATC_L4 = pd.read_csv("processed/ATC_L4.csv", dtype=str).fillna("")

ATC_L4List = []
for i in tqdm(list(ATC_L4['href'])):
    req = requests.get(ATC_DDD_URL+i)
    soup = BeautifulSoup(req.text, 'html.parser')
    try:
        Row4DF = pd.read_html(str(soup.select('ul table')), header=0)[0].fillna('')
        for n in Row4DF.index:
            if Row4DF.at[n, 'ATC code'] == '':
                try:
                    Row4DF.at[n, 'ATC code'] = Row4DF.at[n-1, 'ATC code']
                except:
                    Row4DF.at[n, 'ATC code'] = i

            if Row4DF.at[n, 'Name'] == '':
                try:
                    Row4DF.at[n, 'Name'] = Row4DF.at[n-1, 'Name']
                except:
                    Row4DF.at[n, 'Name'] = i

        ATC_L4List.extend(Row4DF.to_dict('records'))
    except:
        ATC_L4List.extend([{'ATC code': i}])


ATC_L5 = pd.DataFrame(ATC_L4List)
ATC_L5.to_csv("processed/ATC_L5.csv", index=False)
