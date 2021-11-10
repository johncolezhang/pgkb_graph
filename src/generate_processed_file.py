#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import os
import json
import re
from bs4 import BeautifulSoup
from collections import defaultdict

def generate_clinical_file():
    # parse clinical update date
    clinical_update_date = ""
    for x in os.listdir("clinical_annotation"):
        if "CREATED" in x:
            clinical_update_date = x.split("_")[1].split(".")[0]

    df_clinical_annotation = pd.read_csv('clinical_annotation/clinical_annotations.tsv', sep='\t', dtype=str).fillna("")
    df_clinical_annotation.index = range(len(df_clinical_annotation))
    # use to fix wrong comma split
    special_haplotype_name_list = ["G6PD Mediterranean, Dallas, Panama, Sassari, Cagliari, Birmingham"]
    variant_drug_list = []

    df_clinical_phenotype = pd.read_csv("clinical_annotation/clinical_ann_alleles.tsv", sep="\t", dtype=str).fillna("")
    clinical_pheno_dict = {}
    for index, row in df_clinical_phenotype.iterrows():
        clinical_pheno_dict["{}|{}".format(row["Clinical Annotation ID"],
                                           row["Genotype/Allele"])] = row["Annotation Text"]

    df_clinical_annotation = pd.merge(df_clinical_phenotype, df_clinical_annotation, on=["Clinical Annotation ID"], how="left")

    for index, row in df_clinical_annotation.iterrows():
        if "rs" in row["Variant/Haplotypes"]:
            variant = row["Variant/Haplotypes"]
        else:
            if row["Genotype/Allele"] in special_haplotype_name_list or "/" in row["Genotype/Allele"]:
                variant = "{} {}".format(row["Gene"], row["Genotype/Allele"])
            else:
                variant = "{}{}".format(row["Gene"], row["Genotype/Allele"])

        level = row["Level of Evidence"]
        phenotype_category = row["Phenotype Category"]
        phenotype = "{}; {}".format(row["Annotation Text"], row["Phenotype(s)"])
        update_date = row["Latest History Date (YYYY-MM-DD)"]
        link = row["URL"]
        level_modifier = row["Level Modifiers"]
        score = row["Score"]
        drug = row["Drug(s)"]
        drug_list = [x.strip() for x in re.split(r";|,|/", drug)]

        for d in drug_list:
            variant_drug_list.append((variant, level, phenotype_category,
                                      phenotype, update_date, d, link,
                                      level_modifier, score))

    variant_drug_list = list(set(variant_drug_list))
    df_clinical_drug_variant_annotation = pd.DataFrame(
        variant_drug_list,
        columns=["variant", "evidence_level", "phenotype_category", "phenotype",
                 "update_date", "drug", "link", "level_modifier", "score"])

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
        link = "https://www.pharmgkb.org/labelAnnotation/{}".format(row["PharmGKB ID"])

        for c in chemical_list:
            for g in gene_list:
                drug_gene_label_list.append((c, level, source, name, update_date, g, link))

            for v in variant_list:
                drug_variant_label_list.append((c, level, source, name, update_date, v, link))

    df_dl_gene = pd.DataFrame(
        drug_gene_label_list,
        columns=["drug", "label", "organization", "name", "update_date", "gene", "link"]).assign(
        data_source=["drug_label"] * len(drug_gene_label_list)).assign(
        update_date=[drug_label_update_date] * len(drug_gene_label_list)
    )

    df_dl_variant = pd.DataFrame(
        drug_variant_label_list,
        columns=["drug", "label", "organization", "name", "update_date", "variant", "link"]).assign(
        data_source=["drug_label"] * len(drug_variant_label_list)).assign(
        update_date=[drug_label_update_date] * len(drug_variant_label_list)
    )

    df_dl_gene.to_csv("processed/drug_gene_label.csv", index=False)
    df_dl_variant.to_csv("processed/drug_variant_label.csv", index=False)


def parse_tables(html, gene, drug, organization, link):
    # save columns which contain these pharses
    column_template = ["phenotype", "diplotype", "genotype", "implication", "description", "recommendation", "variant"]
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    titles = soup.find_all("h3")
    df_list = []
    for i in range(len(tables)):
        try:
            table = tables[i]
            title = titles[i].get_text()
        except Exception as e:
            print("missing title, gene {}, drug {}, organization {}, link: {}".format(gene, drug, organization, link))
            continue
        list_header = []
        header = table.find("tr")
        for items in header:
            try:
                t = items.get_text()
                if t == "\n":
                    continue
                list_header.append(t)
            except:
                continue

        HTML_data = table.find_all("tr")[1:]
        data = []
        for element in HTML_data:
            sub_data = []
            for sub_element in element:
                try:
                    t = sub_element.get_text()
                    if t == "\n":
                        continue
                    sub_data.append(t)
                except:
                    continue
            data.append(sub_data)

        try:
            df_tmp = pd.DataFrame(data=data, columns=list_header)
            save_column = []
            for column in df_tmp.columns:
                if any([True if x in column.lower() else False for x in column_template]):
                    save_column.append(column)
            df_tmp = df_tmp[save_column]

            df_tmp = df_tmp.assign(
                gene=[gene] * len(df_tmp)
            ).assign(
                drug=[drug] * len(df_tmp)
            ).assign(
                organization=[organization] * len(df_tmp)
            ).assign(
                title=[title] * len(df_tmp)
            ).assign(
                link=[link] * len(df_tmp)
            )
            df_list.append(df_tmp)
        except Exception as e:
            print("parsing html failed, gene {}, drug {}, organization {}".format(gene, drug, organization))
            continue
    return df_list


def handle_pheno_drug_tables(table_list, guideline_update_date):
    phenotype_category_list = []
    phenotype_list = []
    diplotype_list = []
    genotype_list = []
    implication_list = []
    description_list = []
    recommendation_list = []
    dosing_list = []
    gene_list = []
    drug_list = []
    organization_list = []
    title_list = []
    link_list = []
    variant_list = []
    for table in table_list:
        phenotype_column = ""
        diplotype_column = ""
        genotype_column = ""
        implication_column = ""
        description_column = ""
        recommendation_column = ""
        dosing_column = ""
        variant_column = ""
        for col in table.columns:
            if "variant" in col.lower():
                variant_column = col
            if "phenotype" in col.lower():
                phenotype_column = col
            if "diplotype" in col.lower():
                diplotype_column = col
            if "genotype" in col.lower():
                genotype_column = col
            if "implication" in col.lower():
                implication_column = col
            if "description" in col.lower():
                description_column = col
            if "recommendation" in col.lower() and "classification" not in col.lower():
                recommendation_column = col
            if "dosing" in col.lower():
                dosing_column = col

        for index, row in table.iterrows():
            if phenotype_column != "":
                phenotype = row[phenotype_column]
            else:
                continue

            if diplotype_column != "":
                diplotype = row[diplotype_column]
            else:
                diplotype = ""

            if genotype_column != "":
                genotype = row[genotype_column]
            else:
                genotype = ""

            if implication_column != "":
                implication = row[implication_column]
            else:
                implication = ""

            if description_column != "":
                description = row[description_column]
            else:
                description = ""

            if recommendation_column != "":
                recommendation = row[recommendation_column]
            else:
                recommendation = ""

            if dosing_column != "":
                dosing = row[dosing_column]
            else:
                dosing = ""

            if variant_column != "":
                variant = row[variant_column]
            else:
                variant = ""

            gene = row["gene"]
            drug = row["drug"]
            organization = row["organization"]
            title = row["title"]

            if any([True if x in phenotype.lower() and "meta" in phenotype.lower() else False \
                    for x in ["intermediate", "normal", "poor", "rapid", "ultrarapid", "extensive"]]) or \
                    any(True if x in phenotype else False \
                        for x in ["IM", "PM", "UM", "NM", "RM", "EM"]) or \
                    any(True if x in phenotype.lower() else False \
                        for x in ["suscept", "hypersensitivity", "scar",
                                  "homozygous", "hla-b", "hla-a"]): # special phenotype for HLA-A, HLA-B and RYR1
                pass
            else:
                continue

            if genotype == "" and implication == "" and description == "" and \
               recommendation == "" and dosing == "" and variant == "":
                continue

            if "_" in gene:
                genes = gene.split("_")
                filtered_gene = list(filter(lambda x: x in phenotype, genes))
                if len(filtered_gene) > 0:
                    gene = filtered_gene[0]

            if "Drug" in table.columns and row["Drug"] != "":
                drug = row["Drug"]
            elif "_" in drug:
                drugs = drug.split("_")
                filtered_drug = list(filter(lambda x: x in phenotype or x in recommendation or x in implication, drugs))
                if len(filtered_drug) > 0:
                    drug = filtered_drug[0]

            if type(drug) == list and len(drug) > 0:
                drug = drug[0]

            phenotype_category = ""
            if "poor" in phenotype.lower() or "PM" in phenotype:
                phenotype_category = "Poor Metabolizer"
            elif "intermediate" in phenotype.lower() or "IM" in phenotype:
                phenotype_category = "Intermediate Metabolizer"
            elif "normal" in phenotype.lower() or "NM" in phenotype:
                phenotype_category = "Normal Metabolizer"
            elif ("rapid" in phenotype.lower() and "ultra" not in phenotype.lower()) or "RM" in phenotype:
                phenotype_category = "Rapid Metabolizer"
            elif ("rapid" in phenotype.lower() and "ultra" in phenotype.lower()) or "UM" in phenotype:
                phenotype_category = "Ultrarapid Metabolizer"
            elif "extensive" in phenotype.lower() or "EM" in phenotype:
                phenotype_category = "Extensive Metabolizer"

            phenotype_category_list.append(phenotype_category)
            phenotype_list.append(phenotype)
            diplotype_list.append(diplotype)
            genotype_list.append(genotype)
            implication_list.append(implication)
            description_list.append(description)
            recommendation_list.append(recommendation)
            dosing_list.append(dosing)
            gene_list.append(gene)
            drug_list.append(drug)
            organization_list.append(organization)
            title_list.append(title)
            link_list.append(row["link"])
            variant_list.append(variant)

    df_pheno_drug = pd.DataFrame({
        "phenotype_category": phenotype_category_list,
        "phenotype": phenotype_list,
        "diplotype": diplotype_list,
        "genotype": genotype_list,
        "implication": implication_list,
        "description": description_list,
        "recommendation": recommendation_list,
        "dosing": dosing_list,
        "gene": gene_list,
        "drug": drug_list,
        "organization": organization_list,
        "title": title_list,
        "link": link_list,
        "data_source": ["guildeline"] * len(link_list),
        "update_date": [guideline_update_date] * len(link_list),
        "variant": variant_list
    })

    df_pheno_drug[
        df_pheno_drug["phenotype_category"] == ""
    ].to_csv("processed/phenotype_drug_relation_no_metabolizer.csv",
             index=False)

    df_pheno_drug = df_pheno_drug[df_pheno_drug["phenotype_category"] != ""]
    df_pheno_drug.to_csv("processed/phenotype_drug_relation.csv", index=False)

    phenotype_category_list = []
    phenotype_list = []
    diplotype_list = []
    genotype_list = []
    implication_list = []
    description_list = []
    recommendation_list = []
    gene_list = []
    drug_list = []
    organization_list = []
    title_list = []
    link_list = []

    for index, row in df_pheno_drug.iterrows():
        diplotype = row["diplotype"]
        diplotypes = re.split(",|;| ", diplotype)
        diplotypes = list(filter(lambda x: x != "" and x.count("*") == 2, diplotypes))
        if len(diplotypes) == 0:
            continue

        for dip in diplotypes:
            dip_name = "{} {}".format(row["gene"], dip)
            recommendation = row["dosing"] if row["dosing"] != "" else row["recommendation"]
            phenotype_category_list.append(row["phenotype_category"])
            phenotype_list.append(row["phenotype"])
            diplotype_list.append(dip_name)
            genotype_list.append(row["genotype"])
            implication_list.append(row["implication"])
            description_list.append(row["description"])
            recommendation_list.append(row["recommendation"])
            gene_list.append(row["gene"])
            drug_list.append(row["drug"])
            organization_list.append(row["organization"])
            title_list.append(row["title"])
            link_list.append(row["link"])

    pd.DataFrame({
        "phenotype_category": phenotype_category_list,
        "phenotype": phenotype_list,
        "diplotype": diplotype_list,
        "genotype": genotype_list,
        "implication": implication_list,
        "description": description_list,
        "recommendation": recommendation_list,
        "gene": gene_list,
        "drug": drug_list,
        "organization": organization_list,
        "title": title_list,
        "link": link_list,
        "data_source": ["guildeline"] * len(link_list),
        "update_date": [guideline_update_date] * len(link_list)
    }).to_csv("processed/diplotype_drug_relation.csv", index=False)


def generate_no_metabolizer_data():
    df_pheno = pd.read_csv("processed/phenotype_drug_relation_no_metabolizer.csv").fillna("")
    haplotype_record_list = []
    diplotype_record_list = []

    for index, row in df_pheno.iterrows():
        diplotype_list = row["diplotype"].replace("a", "").replace("b", "").replace("c", "").replace("d", "").split(" ")
        diplotype_list = list(filter(lambda x: x != "" and "/" in x and x != "/",
                                     [x.replace(",", "").replace(";", "").strip() for x in diplotype_list]))
        variant_list = list(filter(lambda x: x != "", [x.split(";")[0].strip() for x in row["variant"].split(",")]))
        gene = row["gene"].replace("_", "-")
        for diplo in diplotype_list:
            diplotype_record_list.append(
                ("{} {}".format(gene, diplo), row["phenotype_category"], row["phenotype"], row["genotype"],
                 row["implication"], row["description"], row["recommendation"], row["dosing"], row["drug"],
                 row["organization"], row["title"], row["link"], row["data_source"], row["update_date"])
            )
        for var in variant_list:
            haplotype_record_list.append(
                (var, row["phenotype_category"], row["phenotype"], row["genotype"],
                 row["implication"], row["description"], row["recommendation"], row["dosing"], row["drug"],
                 row["organization"], row["title"], row["link"], row["data_source"], row["update_date"])
            )

        df_haplotype_guideline = pd.DataFrame(
            haplotype_record_list,
            columns=["haplotype", "phenotype_category", "phenotype", "genotype",
                     "implication", "description", "recommendation", "dosing", "drug",
                     "organization", "title", "link", "data_source", "update_date"])

        df_haplotype_guideline.to_csv("processed/haplotype_record_guideline.csv", index=False)

        df_diplotype_guideline = pd.DataFrame(
            diplotype_record_list,
            columns=["diplotype", "phenotype_category", "phenotype", "genotype",
                     "implication", "description", "recommendation", "dosing", "drug",
                     "organization", "title", "link", "data_source", "update_date"])

        df_diplotype_guideline.to_csv("processed/diplotype_record_guideline.csv", index=False)


def generate_guideline_file():
    folder = "guideline_annotation"
    guideline_list = []
    haplotype_list = []
    term_list = []
    drug_list = []
    gene_list = []
    guideline_name_list = []
    guideline_link_list = []
    guideline_cancer_genome_list = []
    guideline_literature_list = []
    guideline_source_list = []
    df_table_list = []

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
            guideline_link = guideline_dict["guideline"]["@id"]
            guideline_cancer_genome = str(guideline_dict["guideline"]["cancerGenome"])
            literature_list = []
            for liter in guideline_dict["guideline"]["literature"]:
                literature_list.append({
                    "title": liter["title"],
                    "link": liter["@id"]
                })
            guideline_literature = str(literature_list)
            guideline_source = guideline_dict["guideline"]["source"]

            guideline_name_list.extend([guideline_name] * len(tmp_haplotype_list) * len(drugs))
            guideline_list.extend([guideline] * len(tmp_haplotype_list) * len(drugs))
            gene_list.extend([genes] * len(tmp_haplotype_list) * len(drugs))
            guideline_link_list.extend([guideline_link] * len(tmp_haplotype_list) * len(drugs))
            guideline_cancer_genome_list.extend([guideline_cancer_genome] * len(tmp_haplotype_list) * len(drugs))
            guideline_literature_list.extend([guideline_literature] * len(tmp_haplotype_list) * len(drugs))
            guideline_source_list.extend([guideline_source] * len(tmp_haplotype_list) * len(drugs))

            # data alignment
            for drug in drugs:
                drug_list.extend([drug] * len(tmp_haplotype_list))
                haplotype_list.extend(tmp_haplotype_list)
                term_list.extend(tmp_term_list)

            df_table_list.extend(
                parse_tables(
                    guideline_dict["guideline"]["textMarkdown"]["html"],
                    gene=genes,
                    drug=drugs,
                    organization=guideline,
                    link=guideline_link
                )
            )

    df_guideline = pd.DataFrame({
        "guideline_institute": guideline_list,
        "guideline_name": guideline_name_list,
        "drug": drug_list,
        "gene": gene_list,
        "haplotype": haplotype_list,
        "term": term_list,
        "guideline_link": guideline_link_list,
        "cancer_genome": guideline_cancer_genome_list,
        "literature": guideline_literature_list,
        "source": guideline_source_list
    })

    df_guideline.assign(
        data_source=["guideline"] * len(df_guideline)
    ).assign(
        update_date=[guideline_update_date] * len(df_guideline)
    ).to_csv("processed/guideline_drug_variant_annotation.csv", index=False)

    # generate (gene pheno <-> drug) relation data
    handle_pheno_drug_tables(df_table_list, guideline_update_date)
    # generate no metabolizer guideline data
    generate_no_metabolizer_data()


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
        link = "https://www.pharmgkb.org/variantAnnotation/{}".format(row["Variant Annotation ID"])

        if row["Alleles"] != "" and "/" in row["Alleles"]:
            diplotype_list = [x.strip() for x in row["Alleles"].split("+")]
            for d in drug_list:
                for dip in diplotype_list:
                    for p in phenotype_list:
                        if "/" in dip:
                            diplotype = "{} {}".format(row["Gene"], dip)
                            diplotype_drug_research_list.append(
                                (d, p, p_value, biogeo_group, PMID,
                                 PMID_link, note, sentence, diplotype, link))
        else:
            for d in drug_list:
                for v in variant_list:
                    for p in phenotype_list:
                        variant_drug_research_list.append((d, p, p_value, biogeo_group,
                                                           PMID, PMID_link, note, sentence, v, link))

    pd.DataFrame(
        variant_drug_research_list,
        columns=["drug", "phenotype_category", "p_value",
                 "bio_geo_group", "PMID", "PMID_link", "note", "sentence", "variant", "link"]
    ).assign(
        data_source=["research"] * len(variant_drug_research_list)
    ).assign(
        update_date=[research_update_date] * len(variant_drug_research_list)
    ).to_csv("processed/research_drug_variant_annotation.csv", index=False)

    pd.DataFrame(
        diplotype_drug_research_list,
        columns=["drug", "phenotype_category", "p_value",
                 "bio_geo_group", "PMID", "PMID_link", "note", "sentence",
                 "diplotype", "link"]
    ).assign(
        data_source=["research"] * len(diplotype_drug_research_list)
    ).assign(
        update_date=[research_update_date] * len(diplotype_drug_research_list)
    ).to_csv("processed/research_drug_diplotype_annotation.csv", index=False)


def generate_cpic_guideline_file():
    xl = pd.ExcelFile("cpic/cpic_gene-drug_pairs.xlsx")
    df_cpic_guideline = pd.read_excel(xl, sheet_name="CPIC Gene-Drug Pairs", dtype=str).fillna("")
    df_change = pd.read_excel(xl, sheet_name="Change log", dtype=str).fillna("")
    update_date = list(df_change["Date"].values)[-1]
    df_cpic_guideline = df_cpic_guideline.assign(update_date=[update_date] * len(df_cpic_guideline))
    pmid_list = list(df_cpic_guideline["CPIC Publications (PMID)"].values)
    pmid_link_list = []
    PMID_link_template = "https://pubmed.ncbi.nlm.nih.gov/{}/"
    for pmid in pmid_list:
        if ";" not in pmid:
            if pmid.strip() != "":
                pmid_link_list.append(PMID_link_template.format(pmid.strip()))
            else:
                pmid_link_list.append("")
        else:
            pmid_link_list.append(" ; ".join([PMID_link_template.format(x.strip()) for x in pmid.split(";")]))

    df_cpic_guideline = df_cpic_guideline.assign(PMID_link=pmid_link_list)
    df_cpic_guideline.to_csv("processed/cpic_gene_drug.csv", index=False)


def position_map_util(NC_change_code):
        chr_name, pos = NC_change_code.replace("g.", "").replace("m.", "").split(":")
        chr_name = "chr" + str(int("".join(re.findall(r"[\d]+.[\d]+", chr_name)).split(".")[0]))
        position = "".join(
                ["{}:{}".format(pos.replace(y, ""), y)
                 for y in re.findall(r"[\D]+", pos)]
            ) if "del" not in pos else pos

        return "{}:{}".format(chr_name, position)


def generate_rsID_position():
    df_variants = pd.read_csv(
        'variants/variants.tsv',
        sep='\t',
        error_bad_lines=False,
        dtype=str
    ).fillna("")

    variant_location_matched_synonym_dict = defaultdict(list)
    for index, row in df_variants.iterrows():
        location = row["Location"]
        variant = row["Variant Name"]
        synonym = row["Synonyms"]
        if variant != "" and synonym != "":
            synonym_list = [x.strip() for x in synonym.split(",")]
            for s in synonym_list:
                if location != "" and location.split(":")[1] in s and "=" not in s:
                    s = position_map_util(s)
                    variant_location_matched_synonym_dict[variant].append(s)

    rsID_location_dict = {key: ",".join(value) for key, value in variant_location_matched_synonym_dict.items()}

    pd.DataFrame({
        "rsID": list(rsID_location_dict.keys()),
        "position": list(rsID_location_dict.values())
    }).to_csv("processed/rsID_position.csv", index=False)


def step2_generate_file():
    generate_clinical_file()
    generate_drug_label_file()
    generate_guideline_file()
    generate_research_file()
    generate_cpic_guideline_file()

if __name__ == "__main__":
    # step2_generate_file()
    generate_rsID_position()
