import requests
import string
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
from collections import defaultdict

drug_list = "https://www.yaopinnet.com/tools/yibao2020/xiyao.htm"
req = requests.get(drug_list)
if req.status_code == 200:
    content = req.content
    soup = BeautifulSoup(content, 'html.parser')
    l1_text_list = [[x.text.split(" ")[0].strip(), x.text.split(" ")[1].strip()]
                    for x in soup.findAll(name='div', attrs={"class": "c0"})]
    l2_text_list = []

    for y in soup.findAll(name='div', attrs={"class": "c1"}):
        for z in y.findAll("a"):
            l2_text_list.append([z['href'], z.text.split(" ")[0].strip(), z.text.split(" ")[1].strip()])

    df_l1 = pd.DataFrame(l1_text_list, columns=["code", "text"])
    df_l1.to_csv("processed/drug_insurance_L1.csv", index=False)

    df_l2 = pd.DataFrame(l2_text_list, columns=["url", "code", "text"])
    df_l2.to_csv("processed/drug_insurance_L2.csv", index=False)


df_l2 = pd.read_csv("processed/drug_insurance_L2.csv", dtype=str).fillna("")
drug_detail_template = "https://www.yaopinnet.com/tools/yibao2020/list.asp?k={}"
df_l3 = pd.DataFrame(columns=["编号", "甲乙", "药品名称", "剂型", "备注", "招商信息"])

for index, row in tqdm(df_l2.iterrows()):
    url = drug_detail_template.format(row['code'])
    try:
        req = requests.get(url, timeout=10)
    except:
        continue

    soup = BeautifulSoup(req.content, 'html.parser')
    try:
        df_table = pd.read_html(str(soup.select('table')), header=0)[0].fillna("").astype(str)
        df_l3 = pd.concat([df_l3, df_table], ignore_index=True, axis=0)
    except:
        continue

df_l3.to_csv("processed/drug_insurance_L3.csv", index=False)
