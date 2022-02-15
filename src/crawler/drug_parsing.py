import json
import os
import pandas as pd
from fnmatch import fnmatch
from collections import defaultdict

# 化合物及药品名的映射
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

    imp_drug_dict = defaultdict(list)

    for imp_drug in imported_drug_list:
        drug_name = imp_drug["产品名称（中文）"]
        business_name = imp_drug["商品名（中文）"]
        license_no = imp_drug["注册证号"]
        drug_no = imp_drug["药品本位码"]
        company = imp_drug["生产厂商（英文）"]

        imp_drug_dict["drug_name"].append(drug_name)
        imp_drug_dict["business_name"].append(business_name)
        imp_drug_dict["license_no"].append(license_no)
        imp_drug_dict["drug_no"].append(drug_no)
        imp_drug_dict["company"].append(company)

    print(len(set(imp_drug_dict["drug_name"])))

    pd.DataFrame(imp_drug_dict).to_csv("processed/imported_drug.csv", index=False)

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

    dd_columns = ["药品名称", "成份", "性状", "适应症", "用法用量", "不良反应", "禁忌", "注意事项",
                  "孕妇及哺乳期妇女用药", "儿童用药", "老年用药", "贮藏", "规格", "药物相互作用",
                  "药理毒理", "药代动力学", "药物过量", "有效期", "包装", "执行标准"]
    df_drug_description = pd.DataFrame(
        columns=dd_columns)

    for fn in os.listdir("processed"):
        if fnmatch(fn, "drug_description_detail_*.csv"):
            df_drug_description = pd.concat(
                [df_drug_description,
                 pd.read_csv(os.path.join("processed", fn), dtype=str).fillna("")],
                ignore_index=True,
                axis=0
            )

    drug_description_dict = {}
    for index, row in df_drug_description.iterrows():
        drug_description_dict[row["药品名称"]] = {}
        for col in dd_columns:
            if col != "药品名称":
                drug_description_dict[row["药品名称"]][col] = row[col]
    for key in drug_description_dict.keys():
        drug_description_dict[key] = str(drug_description_dict[key]).replace("\"", "'")

    for drug in demostic_drug_list:
        d_split = drug["英文名称"].lower().split(" ")
        d_split = list(filter(lambda x: x not in ["", "tablets", "injection", "compound", "enteric-coated"
                                                  "for", "and", "capsules", "c", "granules", "sodium",
                                                  "glucose", "acid"], d_split))
        for key, value in chemical_dict.items():
            name = value.lower()
            for d in d_split:
                if name == d:
                    drug_chemical_relation_list.append((key, value, drug))
                    break

    for drug in imported_drug_list:
        d_split = drug["产品名称（英文）"].lower().split(" ")
        d_split = list(filter(lambda x: x not in ["", "tablets", "injection", "compound", "enteric-coated"
                                                  "for", "and", "capsules", "c",
                                                  "granules", "sodium",
                                                  "glucose", "acid"], d_split))
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
    description_list = []
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
            if relation[2]["产品名称"] in drug_description_dict.keys():
                description_list.append(drug_description_dict[relation[2]["产品名称"]])
            else:
                description_list.append("")

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
            if relation[2]["产品名称（中文）"] in drug_description_dict.keys():
                description_list.append(drug_description_dict[relation[2]["产品名称（中文）"]])
            else:
                description_list.append("")

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
        "eng_business_name": eng_business_name_list,
        "description": description_list
    }).to_csv("processed/nmpa_drug_chemical.csv", index=False)

if __name__ == "__main__":
    parsing_drug_file()