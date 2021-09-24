#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import os
import json
import re

def generate_clinical_file():
    # parse clinical update date
    clinical_update_date = ""
    for x in os.listdir("clinical_annotation"):
        if "CREATED" in x:
            clinical_update_date = x.split("_")[1].split(".")[0]

    df_clinical_annotation = pd.read_csv('clinical_annotation/clinical_annotations.tsv', sep='\t').fillna("")
    df_clinical_annotation.index = range(len(df_clinical_annotation))
    # use to fix wrong comma split
    special_haplotype_name_list = ["G6PD Mediterranean, Dallas, Panama, Sassari, Cagliari, Birmingham"]
    variant_drug_list = []
    all_variant_list = []

    for index, row in df_clinical_annotation.iterrows():
        variant = row["Variant/Haplotypes"]
        variant_list = []
        for shn in special_haplotype_name_list:
            if shn in variant:
                variant_list.append(shn)
                variant = variant.replace(shn, "")
        variant_list.extend([x.strip() for x in variant.split(",")])
        variant_list = list(filter(lambda x: x != "", variant_list))
        all_variant_list.extend(variant_list)

        level = row["Level of Evidence"]
        phenotype_category = row["Phenotype Category"]
        phenotype = row["Phenotype(s)"]
        update_date = row["Latest History Date (YYYY-MM-DD)"]

        drug = row["Drug(s)"]
        drug_list = [x.strip() for x in re.split(r";|,|/", drug)]

        for v in variant_list:
            for d in drug_list:
                variant_drug_list.append((v, level, phenotype_category, phenotype, update_date, d))

    variant_drug_list = list(set(variant_drug_list))
    df_clinical_drug_variant_annotation = pd.DataFrame(
        variant_drug_list,
        columns=["variant", "evidence_level", "phenotype_category", "phenotype", "update_date", "drug"])

    df_clinical_drug_variant_annotation = df_clinical_drug_variant_annotation.assign(
        data_source=["clinical_annotation"] * len(variant_drug_list)).assign(
        update_date=[clinical_update_date] * len(variant_drug_list)
    )

    df_clinical_drug_variant_annotation.to_csv("processed/clinical_drug_variant_annotation.csv", index=False)


def generate_drug_label_file():
    # both variant and gene
    df_drug_label = pd.read_csv('drug_label/drugLabels.tsv', sep='\t').fillna("")

    # parse drug label update date
    drug_label_update_date = ""
    for x in os.listdir("drug_label"):
        if "CREATED" in x:
            drug_label_update_date = x.split("_")[1].split(".")[0]

    drug_gene_label_list = []
    drug_variant_label_list = []

    for index, row in df_drug_label.iterrows():
        genes = row["Genes"]
        gene_list = [x.strip() for x in genes.split(";")]
        variant = row["Variants/Haplotypes"]
        variant_list = filter(lambda x: x != "", [x.strip() for x in variant.split(";")])
        chemicals = row["Chemicals"]
        chemical_list = filter(lambda x: x != "", [x.strip() for x in re.split(r"and|/", chemicals)])
        source = row["Source"]
        level = row["Testing Level"]
        name = row["Name"]
        update_date = row["Latest History Date (YYYY-MM-DD)"]

        for c in chemical_list:
            for g in gene_list:
                drug_gene_label_list.append((c, level, source, name, update_date, g))

            for v in variant_list:
                drug_variant_label_list.append((c, level, source, name, update_date, v))

    df_dl_gene = pd.DataFrame(
        drug_gene_label_list,
        columns=["drug", "label", "organization", "name", "update_date", "gene"]).assign(
        data_source=["drug_label"] * len(drug_gene_label_list)).assign(
        update_date=[drug_label_update_date] * len(drug_gene_label_list)
    )

    df_dl_variant = pd.DataFrame(
        drug_variant_label_list,
        columns=["drug", "label", "organization", "name", "update_date", "variant"]).assign(
        data_source=["drug_label"] * len(drug_variant_label_list)).assign(
        update_date=[drug_label_update_date] * len(drug_variant_label_list)
    )

    df_dl_gene.to_csv("processed/drug_gene_label.csv", index=False)
    df_dl_variant.to_csv("processed/drug_variant_label.csv", index=False)


def generate_guideline_file():
    folder = "guideline_annotation"
    guideline_list = []
    haplotype_list = []
    term_list = []
    drug_list = []
    gene_list = []
    guideline_name_list = []

    # parse guideline update date
    guideline_update_date = ""
    for x in os.listdir(folder):
        if "CREATED" in x:
            guideline_update_date = x.split("_")[1].split(".")[0]

    for path in os.listdir(folder):
        if path.endswith(".json") and "Annotation_of_" in path:
            with open(os.path.join(folder, path), "r", encoding="utf-8") as f:
                guideline_dict = json.load(f)

            path = path.replace("Annotation_of_", "").replace(".json", "")
            try:
                guideline, drug_gene = path.split("_for_")
            except:
                continue
            drugs, genes = drug_gene.split("_and_")
            drugs = list(filter(lambda x: x != "", [x.strip() for x in drugs.split("_")]))

            tmp_haplotype_list = []
            tmp_term_list = []
            for guideline_gene in guideline_dict["guideline"]["guidelineGenes"]:
                for allele in guideline_gene["alleles"]:
                    if "haplotype" in allele.keys():
                        tmp_haplotype_list.append(allele["haplotype"]["symbol"])
                        tmp_term_list.append(allele["function"]["term"])

            guideline_name = guideline_dict["guideline"]["name"]

            guideline_name_list.extend([guideline_name] * len(tmp_haplotype_list) * len(drugs))
            guideline_list.extend([guideline] * len(tmp_haplotype_list) * len(drugs))
            gene_list.extend([genes] * len(tmp_haplotype_list) * len(drugs))
            # data alignment
            for drug in drugs:
                drug_list.extend([drug] * len(tmp_haplotype_list))
                haplotype_list.extend(tmp_haplotype_list)
                term_list.extend(tmp_term_list)

    df_guideline = pd.DataFrame({
        "guideline_institute": guideline_list,
        "guideline_name": guideline_name_list,
        "drug": drug_list,
        "gene": gene_list,
        "haplotype": haplotype_list,
        "term": term_list
    })
    df_guideline.assign(
        data_source=["guideline"] * len(df_guideline)
    ).assign(
        update_date=[guideline_update_date] * len(df_guideline)
    ).to_csv("processed/guideline_drug_variant_annotation.csv", index=False)


def generate_research_file():
    # both variant and diplotype
    df_variant_annotation = pd.read_csv(
        'variant_annotation/var_drug_ann.tsv',
        sep='\t',
        error_bad_lines=False).fillna("")

    # parse research update date
    research_update_date = ""
    for x in os.listdir("variant_annotation"):
        if "CREATED" in x:
            research_update_date = x.split("_")[1].split(".")[0]

    df_variant_annotation = df_variant_annotation[df_variant_annotation["Significance"] == "yes"]

    df_variant_param = pd.read_csv('variant_annotation/study_parameters.tsv', sep='\t', error_bad_lines=False).fillna(
        "")
    df_variant_param = df_variant_param[
        ["Variant Annotation ID", "Study Type", "Study Cases", "Study Controls", "Characteristics",
         "Characteristics Type", "Frequency In Cases", "Frequency In Controls", "P Value", "Biogeographical Groups"]]
    df_variant_merge = pd.merge(df_variant_annotation, df_variant_param, how="left", left_on="Variant Annotation ID",
                                right_on="Variant Annotation ID")

    # set 0.01 as p_value threshold to filter
    p_value_list = df_variant_merge["P Value"].values
    p_value_list = list(
        map(lambda x: "1" if ">" in str(x) else str(x).replace("<", "").replace("=", "").strip(), p_value_list))
    p_value_bool_list = []
    for x in p_value_list:
        try:
            if float(x) <= 0.01:
                p_value_bool_list.append(True)
            else:
                p_value_bool_list.append(False)
        except:
            p_value_bool_list.append(False)
    df_variant_merge = df_variant_merge.assign(p_value_effect=p_value_bool_list)
    df_variant_merge = df_variant_merge[df_variant_merge["p_value_effect"] == True]

    variant_drug_research_list = []
    diplotype_drug_research_list = []

    for index, row in df_variant_merge.iterrows():
        variant = row["Variant/Haplotypes"]
        if "genotype" in variant:
            continue
        variant_list = filter(lambda x: x != "", [x.strip() for x in variant.split(",")])

        drugs = row["Drug(s)"]
        drug_list = list(filter(lambda x: x != "", [x.replace("\"", "").strip() for x in re.split("/|,", drugs)]))
        p_value = "P value {}".format(row["P Value"]).replace("=", "").replace("<", "")
        phenotype = row["Phenotype Category"]
        phenotype_list = set([x.replace("\"", "").strip() for x in phenotype.split(",")])
        PMID = row["PMID"]
        PMID_link = "https://pubmed.ncbi.nlm.nih.gov/{}/".format(PMID)
        note = row["Notes"]
        sentence = row["Sentence"]
        biogeo_group = row["Biogeographical Groups"].replace("\"", "'")

        if row["Alleles"] != "" and "/" in row["Alleles"]:
            diplotype_list = [x.strip() for x in row["Alleles"].split("+")]
            for d in drug_list:
                for dip in diplotype_list:
                    for p in phenotype_list:
                        if "/" in dip:
                            diplotype = "{} {}".format(row["Gene"], dip)
                            diplotype_drug_research_list.append(
                                (d, p, p_value, biogeo_group, PMID, PMID_link, note, sentence, diplotype))
        else:
            for d in drug_list:
                for v in variant_list:
                    for p in phenotype_list:
                        variant_drug_research_list.append((d, p, p_value, biogeo_group, PMID, PMID_link, note, sentence, v))

    pd.DataFrame(
        variant_drug_research_list,
        columns=["drug", "phenotype_category", "p_value",
                 "bio_geo_group", "PMID", "PMID_link", "note", "sentence", "variant"]
    ).assign(
        data_source=["research"] * len(variant_drug_research_list)
    ).assign(
        update_date=[research_update_date] * len(variant_drug_research_list)
    ).to_csv("processed/research_drug_variant_annotation.csv", index=False)

    pd.DataFrame(
        diplotype_drug_research_list,
        columns=["drug", "phenotype_category", "p_value",
                 "bio_geo_group", "PMID", "PMID_link", "note", "sentence",
                 "diplotype"]
    ).assign(
        data_source=["research"] * len(diplotype_drug_research_list)
    ).assign(
        update_date=[research_update_date] * len(diplotype_drug_research_list)
    ).to_csv("processed/research_drug_diplotype_annotation.csv", index=False)


def step2_generate_file():
    generate_clinical_file()
    generate_drug_label_file()
    generate_guideline_file()
    generate_research_file()

if __name__ == "__main__":
    step2_generate_file()
