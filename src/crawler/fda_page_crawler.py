import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from collections import defaultdict

def fda_guideline():
    guideline_url = "https://www.fda.gov/medical-devices/precision-medicine/table-pharmacogenetic-associations"
    req = requests.get(guideline_url)
    soup = BeautifulSoup(req.text, 'html.parser')
    tables = soup.find_all("div", {"class": "table-responsive"})
    h2 = soup.find_all("h2")
    sec_h2 = list(filter(lambda x: "section" in x.text.lower(), h2))
    df_all_table = pd.DataFrame(columns=["drug", "gene", "subgroup", "interaction"])
    for i, table in enumerate(tables):
        title = sec_h2[i].text
        df_table = pd.read_html(str(table.select('table')), header=0)[0].fillna("").astype(str)
        for column in df_table.columns:
            if "drug" == column.lower():
                df_table["drug"] = df_table[column].str.lower().str.strip()
            if "gene" == column.lower():
                df_table[["gene"]] = df_table[[column]]
            if "subgroup" in column.lower():
                df_table[["subgroup"]] = df_table[[column]]
            if "interaction" in column.lower():
                df_table[["interaction"]] = df_table[[column]]

        df_table["title"] = [title] * len(df_table)
        df_table = df_table[["drug", "gene", "subgroup", "interaction", "title"]]
        df_all_table = pd.concat([df_all_table, df_table], axis=0, ignore_index=True)

    table_dict = defaultdict(list)
    for index, row in df_all_table.iterrows():
        gene_list = list(filter(lambda x: x!= "", [x.strip() for x in re.split(r"and|or|/", row["gene"])]))
        for gene in gene_list:
            table_dict["gene"].append(gene)
            for col in df_all_table.columns:
                if col != "gene":
                    table_dict[col].append(row[col])

    df_all_table = pd.DataFrame(table_dict)
    update_h2 = list(filter(lambda x: "updates" in x.text.lower(), h2))[0]
    update_date = update_h2.find_next_sibling().find_all("strong")[0].text.replace(":", "")

    df_all_table["link"] = [guideline_url] * len(df_all_table)
    df_all_table["update_date"] = [update_date] * len(df_all_table)
    df_all_table.to_csv("processed/fda_guideline_table.csv", index=False)


def fda_label():
    brac_regex = re.compile(r"\([^)]*\)")
    brac_extract_regex = re.compile(r"\((.*?)\)")
    label_url = "https://www.fda.gov/drugs/science-and-research-drugs/table-pharmacogenomic-biomarkers-drug-labeling"
    req = requests.get(label_url)
    soup = BeautifulSoup(req.text, 'html.parser')
    table = soup.find_all("table", {"class": "table-responsive"})[0]
    df_table = pd.read_html(str(table), header=0)[0].fillna("").astype(str)
    for column in df_table.columns:
        if "drug" in column.lower():
            df_table["drug"] = [re.sub(brac_regex, "", x).lower().strip() for x in df_table[column].values]
        if "therapeutic" in column.lower():
            df_table[["therapeutic"]] = df_table[[column]]
        if "biomarker" in column.lower():
            df_table[["gene"]] = df_table[[column]]
        if "label" in column.lower():
            df_table[["label"]] = df_table[[column]]

    df_table = df_table[["drug", "therapeutic", "gene", "label"]]

    table_dict = defaultdict(list)
    for index, row in df_table.iterrows():
        extra_data = "".join(re.findall(brac_extract_regex, row["gene"]))
        gene = re.sub(brac_regex, "", row["gene"]).strip()
        gene_list = list(filter(lambda x: x != "", [x.strip() for x in re.split(r"and|or|/|,", gene)]))
        drug_list = list(filter(lambda x: x != "", [x.strip() for x in re.split(r"and|/|,", row["drug"].replace("\xa0", ""))]))
        for ge in gene_list:
            for drug in drug_list:
                if len(drug) < 2:
                    continue
                table_dict["drug"].append(drug)
                table_dict["gene"].append(ge)
                table_dict["therapeutic"].append("{} {}".format(row["therapeutic"], extra_data).strip())
                table_dict["label"].append(row["label"])

    df_table = pd.DataFrame(table_dict)
    df_table["url"] = [label_url] * len(df_table)

    df_table.to_csv("processed/fda_label_table.csv", index=False)


if __name__ == "__main__":
    fda_guideline()
    fda_label()
