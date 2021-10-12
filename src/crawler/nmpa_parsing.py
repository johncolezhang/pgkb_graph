import json
import os
import pandas as pd

def parsing_drug_file():
    # imported
    with open("nmpa_data/imported_drugs.json", "r", encoding="utf-8") as f:
        imported_drug_list = json.load(f)

        for d in imported_drug_list:
            if "" in d.keys():
                del [d[""]]
            if "注" in d.keys():
                del [d["注"]]
            d["数据源"] = "进口"

    demostic_path = "nmpa_data/demostic_drugs"
    demostic_drug_list = []

    for p in [os.path.join(demostic_path, x) for x in os.listdir(demostic_path)]:
        with open(p, "r", encoding="utf-8") as f:
            demostic_drug_list.extend(json.load(f))

        for d in demostic_drug_list:
            if "" in d.keys():
                del [d[""]]
            if "注" in d.keys():
                del [d["注"]]
            d["数据源"] = "国产"

    df_chemical = pd.read_csv(
        "chemicals/chemicals.tsv",
        sep='\t',
        error_bad_lines=False,
        dtype=str
    ).fillna("")[["PharmGKB Accession Id", "Name"]]

    chemical_dict = dict(zip(df_chemical["PharmGKB Accession Id"].values, df_chemical["Name"].values))
    drug_chemical_relation_list = []

    for drug in demostic_drug_list:
        d_split = drug["英文名称"].lower().split(" ")
        for key, value in chemical_dict.items():
            name = value.lower()
            for d in d_split:
                if name == d:
                    drug_chemical_relation_list.append((key, value, drug))
                    break

    for drug in imported_drug_list:
        d_split = drug["产品名称（英文）"].lower().split(" ")
        for key, value in chemical_dict.items():
            name = value.lower()
            for d in d_split:
                if name == d:
                    drug_chemical_relation_list.append((key, value, drug))
                    break

    pa_list = []
    chemical_list = []
    primary_key_list = []
    chn_name_list = []
    eng_name_list = []
    code_list = []
    source_list = []
    country_list = []
    company_list = []
    dose_list = []
    type_list = []
    chn_business_name_list = []
    eng_business_name_list = []
    for relation in drug_chemical_relation_list:
        pa_list.append(relation[0])
        chemical_list.append(relation[1])
        if relation[2]["数据源"] == "国产":
            primary_key_list.append(relation[2]["批准文号"])
            chn_name_list.append(relation[2]["产品名称"])
            eng_name_list.append(relation[2]["英文名称"])
            code_list.append(relation[2]["药品本位码"])
            source_list.append("国产")
            country_list.append("中国")
            company_list.append(relation[2]["生产单位"])
            dose_list.append(relation[2]["规格"])
            type_list.append(relation[2]["剂型"])
            chn_business_name_list.append(relation[2]["商品名"])
            eng_business_name_list.append("")
        else:
            primary_key_list.append(relation[2]["注册证号"])
            chn_name_list.append(relation[2]["产品名称（中文）"])
            eng_name_list.append(relation[2]["产品名称（英文）"])
            code_list.append(relation[2]["药品本位码"])
            source_list.append("进口")
            country = relation[2]["国家/地区（中文）"]
            country_list.append(country if country != "" else relation[2]["厂商国家/地区（中文）"])
            company = relation[2]["生产厂商（中文）"]
            company_list.append(company if company != "" else relation[2]["生产厂商（英文）"])
            dose_list.append(relation[2]["规格（中文）"])
            type_list.append(relation[2]["剂型（中文）"])
            chn_business_name_list.append(relation[2]["商品名（中文）"])
            eng_business_name_list.append(relation[2]["商品名（英文）"])

    pd.DataFrame({
        "PAID": pa_list,
        "chemical": chemical_list,
        "license": primary_key_list,
        "chn_name": chn_name_list,
        "eng_name": eng_name_list,
        "code": code_list,
        "source": source_list,
        "country": country_list,
        "company": company_list,
        "drug_type": type_list,
        "dose": dose_list,
        "chn_business_name": chn_business_name_list,
        "eng_business_name": eng_business_name_list
    }).to_csv("processed/nmpa_drug_chemical.csv", index=False)

if __name__ == "__main__":
    parsing_drug_file()