import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from collections import defaultdict


brac_extract_regex = re.compile(r"\((.*?)\)")
brac_regex = re.compile(r"\([^)]*\)")


def gen_nat1_df():
    nat1_url = "http://nat.mbg.duth.gr/Human%20NAT1%20alleles_2013.htm"
    req = requests.get(nat1_url)
    text = req.text.replace("<br>", "$nl$").replace("&nbsp;", "$nl$").replace("&#916;", "$nl$&#916;") \
        .replace("1065-1090", "1065-1090$nl$").replace("190C&gt;T", "$nl$190C&gt;T").replace("\x93", "") \
        .replace("\x94", "")
    soup = BeautifulSoup(text, 'html.parser')
    table = soup.find_all("table", {"class": "MsoNormalTable"})[1]
    # parse_table(table)
    df_table = pd.read_html(str(table), header=0)[0].fillna("").astype(str)
    haplotype_column = ""
    nucleotide_rs_column = ""
    protein_column = ""
    phenotype_column = ""
    for col in df_table.columns:
        if "haplotype" in col.lower():
            haplotype_column = col
            continue
        if "nucleotide" in col.lower():
            nucleotide_rs_column = col
            continue
        if "amino acid" in col.lower():
            protein_column = col
            continue
        if "phenotype" in col.lower():
            phenotype_column = col
            continue

    data_dict = defaultdict(list)

    for index, row in df_table.iterrows():
        haplotype = row[haplotype_column]
        nucleotide_rs = row[nucleotide_rs_column] # $nl$ 用于分行
        if nucleotide_rs == "$nl$":
            nucleotide_rs_list = []
        else:
            nucleotide_rs_list = list(filter(lambda x: x != "", [x.strip() for x in nucleotide_rs.split("$nl$")]))
        protein = row[protein_column].replace("; ", "/").replace("(synonymous)", "") # $nl$ 用于分行及填空
        if protein == "$nl$":
            protein_list = []
        else:
            p_list = [x.strip() for x in protein.split("$nl$")]
            protein_list = []
            for pl in p_list:
                if " " in pl:
                    protein_list.extend(list(filter(lambda x: x!= "", [x.strip() for x in pl.split(" ")])))
                else:
                    protein_list.append(pl)

        phenotype = row[phenotype_column]
        if phenotype == "$nl$":
            phenotype = ""
        else:
            phenotype = "".join(phenotype.split("$nl$"))

        nuc_len = len(nucleotide_rs_list)
        if len(protein_list) < nuc_len:
            protein_list.extend([""] * (nuc_len - len(protein_list)))


        data_dict["haplotype"].extend([haplotype] * nuc_len)
        data_dict["phenotype"].extend([phenotype] * nuc_len)
        data_dict["protein"].extend(protein_list)

        rsID_list = ["".join(re.findall(brac_extract_regex, x)).strip() for x in nucleotide_rs_list]
        nucleotide_list = [re.sub(brac_regex, "", x).strip() for x in nucleotide_rs_list]

        data_dict["rsID"].extend(rsID_list)
        data_dict["nucleotide"].extend(nucleotide_list)

    df = pd.DataFrame(data_dict)
    return df

def gen_nat2_df():
    nat2_url = "http://nat.mbg.duth.gr/Human%20NAT2%20alleles_2013.htm"
    req = requests.get(nat2_url)
    text = req.text.replace("<br>", "$nl$").replace("&nbsp;", "$nl$").replace("&#916;", "$nl$&#916;") \
        .replace("1065-1090", "1065-1090$nl$").replace("190C&gt;T", "$nl$190C&gt;T").replace("\x93", "") \
        .replace("\x94", "")
    soup = BeautifulSoup(text, 'html.parser')
    table = soup.find_all("table", {"class": "MsoTable3DFx3"})
    df_table = pd.read_html(str(table), header=0)[0].fillna("").astype(str)
    haplotype_column = ""
    nucleotide_rs_column = ""
    protein_column = ""
    phenotype_column = ""
    for col in df_table.columns:
        if "haplotype" in col.lower():
            haplotype_column = col
            continue
        if "nucleotide" in col.lower():
            nucleotide_rs_column = col
            continue
        if "amino acid" in col.lower():
            protein_column = col
            continue
        if "phenotype" in col.lower():
            phenotype_column = col
            continue

    data_dict = defaultdict(list)

    for index, row in df_table.iterrows():
        haplotype = row[haplotype_column]
        if haplotype == "$nl$":
            continue
        nucleotide_rs = row[nucleotide_rs_column]  # $nl$ 用于分行
        if nucleotide_rs == "$nl$":
            nucleotide_rs_list = []
        else:
            nucleotide_rs_list = list(filter(lambda x: x != "", [x.strip() for x in nucleotide_rs.split("$nl$")]))
        protein = row[protein_column].replace("; ", "/").replace("(synonymous)", "")  # $nl$ 用于分行及填空
        if protein == "$nl$":
            protein_list = []
        else:
            p_list = [x.strip() for x in protein.split("$nl$")]
            protein_list = []
            for pl in p_list:
                if " " in pl:
                    protein_list.extend(list(filter(lambda x: x != "", [x.strip() for x in pl.split(" ")])))
                else:
                    protein_list.append(pl)

        phenotype = row[phenotype_column]
        if phenotype == "$nl$":
            phenotype = ""
        else:
            phenotype = "".join(phenotype.split("$nl$"))

        nuc_len = len(nucleotide_rs_list)
        if len(protein_list) < nuc_len:
            protein_list.extend([""] * (nuc_len - len(protein_list)))

        data_dict["haplotype"].extend([haplotype] * nuc_len)
        data_dict["phenotype"].extend([phenotype] * nuc_len)
        data_dict["protein"].extend(protein_list)

        rsID_list = ["".join(re.findall(brac_extract_regex, x)).strip() for x in nucleotide_rs_list]
        nucleotide_list = [re.sub(brac_regex, "", x).strip() for x in nucleotide_rs_list]

        data_dict["rsID"].extend(rsID_list)
        data_dict["nucleotide"].extend(nucleotide_list)

    df = pd.DataFrame(data_dict)
    return df


if __name__ == "__main__":
    gen_nat2_df()