import requests
import string
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
from collections import defaultdict

# list_url_template = "https://www.yaopinnet.com/huayao1/{}.htm"
#
# menu_list = []
# for alphabet in tqdm(string.ascii_lowercase):
#     for num in range(1, 50):
#         req = requests.get(list_url_template.format("{}{}".format(alphabet, num)))
#         if req.status_code != 200:
#             break
#
#         content = req.content
#         soup = BeautifulSoup(content, 'html.parser')
#         row_list = [i.find('a') for i in soup.findAll('li')]
#         if row_list:
#             for i in row_list:
#                 menu_list.append((i['href'], i.text))
#
# df_menu = pd.DataFrame(menu_list, columns=["url", "drug_name"])
# df_menu.to_csv("processed/drug_description_list.csv", index=False)

df_menu = pd.read_csv("processed/drug_description_list.csv", dtype=str).fillna("")

drug_description_dict = {}
column_set = []
for index, row in tqdm(df_menu.iterrows()):
    url = row["url"]
    drug_name = row["drug_name"]
    try:
        req = requests.get("https://www.yaopinnet.com{}".format(url), timeout=10)
    except:
        continue

    if req.status_code != 200:
        continue
    soup = BeautifulSoup(req.content, 'html.parser')
    row_list = [x.text for x in soup.findAll('li')]
    drug_dict = {}
    try:
        for x in row_list:
            x_split = x.split("】")
            drug_dict[x_split[0].replace("【", "").strip()] = x_split[1].replace("\r", " ").strip()
            if x_split[0].replace("【", "").strip() not in column_set:
                column_set.append(x_split[0].replace("【", "").strip())
        drug_description_dict[drug_name] = drug_dict
    except:
        continue

    if index != 0 and index % 1000 == 0:
        description_dict = defaultdict(list)
        for drug_name, values in drug_description_dict.items():
            for col in column_set:
                if col == "药品名称":
                    description_dict[col].append(drug_name)
                    continue
                description_dict[col].append(values.get(col, ""))

        df_detail = pd.DataFrame(description_dict)
        df_detail.to_csv("processed/drug_description_detail_{}.csv".format(index), index=False)
        drug_description_dict = {}


if len(drug_description_dict.items()) > 0:
    description_dict = defaultdict(list)
    for drug_name, values in drug_description_dict.items():
        for col in column_set:
            if col == "药品名称":
                description_dict[col].append(drug_name)
                continue
            description_dict[col].append(values.get(col, ""))

    df_detail = pd.DataFrame(description_dict)
    df_detail.to_csv("processed/drug_description_detail_{}.csv".format(len(df_menu)), index=False)
