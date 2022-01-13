#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from util.variant_mapping_util import variantMappingUtil
import pandas as pd
import json
from collections import defaultdict
import os
from fnmatch import fnmatch
from datetime import datetime
import re

df_translate = pd.read_csv("d:/disease-kb/processed/chemical_translation.csv",
                           dtype=str).fillna("")

translate_dict = dict(zip(
    list(df_translate["chemical_name"].str.lower().str.strip().values),
    list(df_translate["cn_chemical_name"].str.lower().str.strip().values),
))

v_mapping = variantMappingUtil()

def clinical_evidence():
    df = pd.read_csv(
        "processed/clinical_drug_variant_annotation.csv",
        encoding="utf-8",
        dtype=str
    ).fillna("")

    drug_chn_list = [translate_dict.get(x, "")
                     for x in df["drug"].str.lower().str.strip().values]

    df["drug_chn"] = drug_chn_list
    df[["pgkb_update_date"]] = df[["update_date"]]
    df = df[["drug", "drug_chn", "variant", "evidence_level",
             "phenotype_category", "phenotype", "link",
             "pgkb_update_date"]]

    df.to_csv("d:/knowledge-base/static/clinical_evidence.csv", index=False)


def drug_label():
    df = pd.read_csv(
        "processed/drug_variant_label.csv",
        encoding="utf-8",
        dtype=str
    ).fillna("")

    drug_chn_list = [translate_dict.get(x, "")
                     for x in df["drug"].str.lower().str.strip().values]

    df["drug_chn"] = drug_chn_list
    df[["pgkb_update_date"]] = df[["update_date"]]
    df[["title"]] = df[["name"]]
    df = df[["drug", "drug_chn", "variant", "label",
             "title", "organization", "link",
             "pgkb_update_date"]]

    df.to_csv("d:/knowledge-base/static/drug_variant_label.csv", index=False)

    df = pd.read_csv(
        "processed/drug_gene_label.csv",
        encoding="utf-8",
        dtype=str
    ).fillna("")

    drug_chn_list = [translate_dict.get(x, "")
                     for x in df["drug"].str.lower().str.strip().values]

    df["drug_chn"] = drug_chn_list
    df[["pgkb_update_date"]] = df[["update_date"]]
    df[["title"]] = df[["name"]]
    df[["variant"]] = df[["gene"]]
    df = df[["drug", "drug_chn", "variant", "label",
             "title", "organization", "link",
             "pgkb_update_date"]]

    df.to_csv("d:/knowledge-base/static/drug_gene_label.csv", index=False)

    df = pd.read_csv(
        "d:/disease-kb/processed/cn_drug_label.csv",
        encoding="utf-8",
        dtype=str
    ).fillna("")

    df[["drug_chn"]] = df[["drug"]]
    df[["drug"]] = df[["en_drug"]]
    df[["variant"]] = df[["gene"]]
    df["title"] = ["药物代谢酶和药物作用靶点基因检测技术指南(试行)"] * len(df)
    df[["label"]] = df[["remark"]]
    df["organization"] = ["NMPA"] * len(df)
    df["link"] = ["http://www.nhc.gov.cn/yzygj/s3594/201507/d7df42ae50b14d01b2eda7f1610bd295.shtml"] * len(df)
    df["pgkb_update_date"] = ["2015-07-31"] * len(df)
    df = df[["drug", "drug_chn", "variant", "label",
             "title", "organization", "link",
             "pgkb_update_date"]]

    df.to_csv("d:/knowledge-base/static/cn_drug_label.csv", index=False)


def cpic_evidence():
    df = pd.read_csv(
        "processed/cpic_gene_drug.csv",
        encoding="utf-8",
        dtype=str
    ).fillna("")

    drug_chn_list = [translate_dict.get(x, "")
                     for x in df["Drug"].str.lower().str.strip().values]
    df["drug_chn"] = drug_chn_list
    df[["drug"]] = df[["Drug"]]
    df[["gene"]] = df[["Gene"]]
    df[["cpic_level"]] = df[["CPIC Level"]]
    df[["cpic_level_status"]] = df[["CPIC Level Status"]]
    df[["pgkb_level"]] = df[["PharmGKB Level of Evidence"]]
    df[["fda_pgx_label"]] = df[["PGx on FDA Label"]]
    df[["cpic_update_date"]] = df[["update_date"]]
    df[["link"]] = df[["Guideline"]]

    df = df[["gene", "drug", "drug_chn", "cpic_level", "cpic_level_status",
             "pgkb_level", "fda_pgx_label", "cpic_update_date", "link"]]

    df.to_csv("d:/knowledge-base/static/cpic_evidence.csv", index=False)


def pgkb_guideline():
    df = pd.read_csv(
        "processed/phenotype_drug_relation.csv",
        encoding="utf-8",
        dtype=str
    ).fillna("")

    drug_chn_list = [translate_dict.get(x, "")
                     for x in df["drug"].str.lower().str.strip().values]
    df["drug_chn"] = drug_chn_list
    df[["metabolizer"]] = df[["phenotype_category"]]
    df[["pgkb_update_date"]] = df[["update_date"]]
    df[["remark"]] = df[["title"]]

    df = df[["drug", "drug_chn", "metabolizer", "phenotype", "genotype", "gene", "recommendation",
             "dosing", "implication", "description", "organization", "link", "remark", "pgkb_update_date"]]

    df.to_csv("d:/knowledge-base/static/pgkb_guideline.csv", index=False)

    df = pd.read_csv(
        "processed/phenotype_drug_relation_no_metabolizer.csv",
        encoding="utf-8",
        dtype=str
    ).fillna("")

    drug_chn_list = [translate_dict.get(x, "")
                     for x in df["drug"].str.lower().str.strip().values]
    df["drug_chn"] = drug_chn_list
    df[["metabolizer"]] = df[["phenotype_category"]]
    df[["pgkb_update_date"]] = df[["update_date"]]
    df[["remark"]] = df[["title"]]

    df = df[["drug", "drug_chn", "metabolizer", "phenotype", "genotype", "gene", "recommendation",
             "dosing", "implication", "description", "organization", "link", "remark", "pgkb_update_date"]]

    df.to_csv("d:/knowledge-base/static/pgkb_guideline_no_pheno.csv", index=False)


def metabolizer_variant():
    # data from v_mapping, updated.
    gene_list = []
    metabolizer_list = []
    variant_list = []

    for gene, value in v_mapping.gene_diplotype_dict.items():
        for dip, pheno in value.items():
            dip = "{} {}".format(gene, dip)
            pheno = pheno["phenotype"]
            if gene not in pheno:
                pheno = "{} {}".format(gene, pheno)
            gene_list.append(gene)
            metabolizer_list.append(pheno)
            variant_list.append(dip)

    pd.DataFrame({
        "gene": gene_list,
        "metabolizer": metabolizer_list,
        "variant": variant_list
    }).to_csv("d:/knowledge-base/static/metabolizer_variant.csv", index=False)


def generate_pharmvar_file():
    # data from pharmvar downloading, updated.
    folders = os.listdir("pharmvar")
    folder = [x[1] for x in list(filter(lambda x: x[0],
                                         [[fnmatch(x, "pharmvar-*") and
                                           os.path.isdir(
                                               os.path.join("pharmvar", x)
                                           ), x] for x in folders]))][0]
    version = folder.split("-")[-1]
    folder = os.path.join("pharmvar", folder)
    gene_folders = list(filter(lambda x: os.path.isdir(x), [os.path.join(folder, x) for x in os.listdir(folder)]))

    df_GRCh38_all = pd.DataFrame(
        columns=["Haplotype Name", "Gene", "rsID", "ReferenceSequence",
                 "Variant Start", "Variant Stop", "Reference Allele",
                 "Variant Allele", "Type"]
    )

    df_RefSeq_all = pd.DataFrame(
        columns=["Haplotype Name", "Gene", "rsID", "ReferenceSequence",
                 "Variant Start", "Variant Stop", "Reference Allele",
                 "Variant Allele", "Type"]
    )

    for gf in gene_folders:
        GRCh38_hap_file = list(filter(lambda x: x.endswith("haplotypes.tsv"),
                                      os.listdir(os.path.join(gf, "GRCh38"))))[0]
        GRCh38_hap_file = os.path.join(gf, "GRCh38", GRCh38_hap_file)
        df_GRCh38 = pd.read_csv(GRCh38_hap_file, sep="\t", skiprows=1, dtype=str).fillna("")
        df_GRCh38_all = pd.concat([df_GRCh38_all, df_GRCh38], axis=0, ignore_index=True)

        RefSeq_hap_file = list(filter(lambda x: x.endswith("haplotypes.tsv"),
                                      os.listdir(os.path.join(gf, "RefSeqGene"))))[0]
        RefSeq_hap_file = os.path.join(gf, "RefSeqGene", RefSeq_hap_file)
        df_RefSeq = pd.read_csv(RefSeq_hap_file, sep="\t", skiprows=1, dtype=str).fillna("")
        df_RefSeq_all = pd.concat([df_RefSeq_all, df_RefSeq], axis=0, ignore_index=True)

    df_GRCh38_all["version"] = [version] * len(df_GRCh38_all)
    df_RefSeq_all["version"] = [version] * len(df_RefSeq_all)

    df_GRCh38_all[["haplotype_name", "gene", "rsID", "reference_sequence",
                   "variant_start", "variant_end", "reference_allele",
                   "variant_allele", "type"]] = \
    df_GRCh38_all[["Haplotype Name", "Gene", "rsID", "ReferenceSequence",
                   "Variant Start", "Variant Stop", "Reference Allele",
                   "Variant Allele", "Type"]]
    df_GRCh38_all = df_GRCh38_all[["haplotype_name", "gene", "rsID", "reference_sequence",
                                   "variant_start", "variant_end", "reference_allele",
                                   "variant_allele", "type", "version"]]

    df_RefSeq_all[["haplotype_name", "gene", "rsID", "reference_sequence",
                   "variant_start", "variant_end", "reference_allele",
                   "variant_allele", "type"]] = \
    df_RefSeq_all[["Haplotype Name", "Gene", "rsID", "ReferenceSequence",
                   "Variant Start", "Variant Stop", "Reference Allele",
                   "Variant Allele", "Type"]]
    df_RefSeq_all = df_RefSeq_all[["haplotype_name", "gene", "rsID", "reference_sequence",
                                   "variant_start", "variant_end", "reference_allele",
                                   "variant_allele", "type", "version"]]

    df_GRCh38_all.to_csv("d:/knowledge-base/static/pharmvar_GRCh38_all.csv", index=False)
    df_RefSeq_all.to_csv("d:/knowledge-base/static/pharmvar_RefSeq_all.csv", index=False)


def get_NC_position(NC_column, NC_list):
    chr_name = "".join(re.findall(r"NC_[\d]+.[\d]+", NC_column))
    position_list = ["{}:{}".format(chr_name, x.strip()) if x != "" else "" for x in NC_list]
    return position_list


def get_NM_code(nucleotide_column):
    return "".join(re.findall(r"NM_[\d]+.[\d]+", nucleotide_column))


def align_variant_list(protein_list, nucleotide_list, rsID_list, NC_list):
    max_size = max(len(protein_list), len(nucleotide_list), len(rsID_list), len(NC_list))
    if len(protein_list) != max_size:
        protein_list = [""] * max_size
    if len(nucleotide_list) != max_size:
        nucleotide_list = [""] * max_size
    if len(rsID_list) != max_size:
        rsID_list = [""] * max_size
    if len(NC_list) != max_size:
        NC_list = [""] * max_size

    new_protein_list = []
    new_nucleotide_list = []
    new_rsID_list = []
    new_NC_list = []

    for i in range(max_size):
        npro_list = [x.strip() for x in protein_list[i].split(";")]
        nnuc_list = [x.strip() for x in nucleotide_list[i].split(";")]
        nrs_list = [x.strip() for x in rsID_list[i].split(";")]
        nnc_list = [x.strip() for x in NC_list[i].split(";")]
        m_size = max(len(npro_list), len(nnuc_list), len(nrs_list), len(nnc_list))
        if len(npro_list) == m_size:
            new_protein_list.extend(npro_list)
        else:
            new_protein_list.extend(npro_list * m_size)

        if len(nnuc_list) == m_size:
            new_nucleotide_list.extend(nnuc_list)
        else:
            new_nucleotide_list.extend(nnuc_list * m_size)

        if len(nrs_list) == m_size:
            new_rsID_list.extend(nrs_list)
        else:
            new_rsID_list.extend(nrs_list * m_size)

        if len(nnc_list) == m_size:
            new_NC_list.extend(nnc_list)
        else:
            new_NC_list.extend(nnc_list * m_size)

    return new_protein_list, new_nucleotide_list, new_rsID_list, new_NC_list


def generate_pgkb_variant_file():
    # data from v_mapping, updated.
    brac_regex = re.compile(r"\[[^)]*\]")
    position_map_dict = defaultdict(list)
    # generate all haplotype rsID position mapping
    for gene, hap_list in v_mapping.gene_hap_list_dict.items():
        for hap in hap_list:
            if "*" in hap:
                hap_name = "{}{}".format(gene, hap)
            else:
                hap_name = "{} {}".format(gene, hap)
            if hap == "":
                continue
            hap_mapping_dict = v_mapping.haplotype_mapping(hap_name)
            protein_list = hap_mapping_dict.get("protein_list", [])
            nucleotide_column = hap_mapping_dict.get("nucleotide_column", "")
            mRNA = get_NM_code(nucleotide_column)
            nucleotide_list = hap_mapping_dict.get("nucleotide_list", [])
            rsID_list = hap_mapping_dict.get("rsID", [])
            NC_column = hap_mapping_dict.get("NC_column", "")
            NC_list = hap_mapping_dict.get("NC_list", [])
            NC_list = [re.sub(brac_regex, "", x) for x in NC_list]
            is_reference = str(hap_mapping_dict.get("is_reference", ""))

            protein_list, nucleotide_list, rsID_list, NC_list = align_variant_list(protein_list, nucleotide_list, rsID_list, NC_list)
            position_list = get_NC_position(NC_column, NC_list)

            if is_reference == "True":
                position_map_dict["gene"].append(gene)
                position_map_dict["is_reference"].append(is_reference)
                position_map_dict["haplotype_name"].append(hap_name)
                position_map_dict["mRNA"].append("")
                position_map_dict["rsID"].append("")
                position_map_dict["position"].append("")
                position_map_dict["nucleotide"].append("")
                position_map_dict["protein"].append("")
            else:
                for i in range(len(protein_list)):
                    position_map_dict["gene"].append(gene)
                    position_map_dict["is_reference"].append(is_reference)
                    position_map_dict["haplotype_name"].append(hap_name)
                    position_map_dict["mRNA"].append(mRNA)
                    position_map_dict["rsID"].append(rsID_list[i])
                    position_map_dict["position"].append(position_list[i])
                    position_map_dict["nucleotide"].append(nucleotide_list[i])
                    position_map_dict["protein"].append(protein_list[i])

    df_position_map = pd.DataFrame(position_map_dict)
    df_position_map.to_csv("processed/new_position_map.csv", index=False)

    df_convert = pd.read_csv("processed/convert_position.csv", dtype=str).fillna("")
    position_dict = dict(zip(
        list(df_convert["position"].values),
        list(df_convert["convert_position"].values)
    ))

    convert_position_list = [position_dict.get(x, "") for x in df_position_map["position"].values]

    df_position_map["convert_position"] = convert_position_list
    df_position_map["update_time"] = [datetime.now().strftime("%Y-%m-%d")] * len(df_position_map)
    df_position_map.to_csv("d:/knowledge-base/static/pgkb_variant_all.csv", index=False)


def generate_fda_guideline():
    # data from fda crawling, updated.
    df= pd.read_csv("processed/fda_guideline_table.csv", dtype=str).fillna("")

    drug_chn_list = [translate_dict.get(x, "")
                     for x in df["drug"].str.lower().str.strip().values]

    df["drug_chn"] = drug_chn_list
    df[["metabolizer"]] = df[["subgroup"]]
    df[["recommendation"]] = df[["interaction"]]
    df[["description"]] = df[["title"]]
    df[["pgkb_update_date"]] = df[["update_date"]]
    df[["remark"]] = df[["update_date"]]
    df["phenotype"] = [""] * len(df)
    df["dosing"] = [""] * len(df)
    df["implication"] = [""] * len(df)
    df["organization"] = ["FDA_Guideline"] * len(df)
    df[["genotype"]] = df[["subgroup"]]

    df = df[["drug", "drug_chn", "metabolizer", "phenotype", "genotype", "gene", "recommendation",
             "dosing", "implication", "description", "organization", "link", "remark", "pgkb_update_date"]]

    df.to_csv("d:/knowledge-base/static/fda_guideline.csv", index=False)


def generate_fda_label():
    # data from fda label crawling, updated.
    df = pd.read_csv("processed/fda_label_table.csv", dtype=str).fillna("")
    drug_chn_list = [translate_dict.get(x, "")
                     for x in df["drug"].str.lower().str.strip().values]
    df["drug_chn"] = drug_chn_list
    df[["variant"]] = df[["gene"]]
    df["organization"] = ["FDA"] * len(df)
    df["pgkb_update_date"] = ["2021-01-01"] * len(df)
    df[["title"]] = df[["therapeutic"]]
    df[["link"]] = df[["url"]]

    df = df[["drug", "drug_chn", "variant", "label",
             "title", "organization", "link",
             "pgkb_update_date"]]

    df.to_csv("d:/knowledge-base/static/fda_label.csv", index=False)



if __name__ == "__main__":
    clinical_evidence()
    drug_label()
    cpic_evidence()
    pgkb_guideline()
    metabolizer_variant()
    generate_pharmvar_file()
    generate_pgkb_variant_file()
    generate_fda_guideline()
    generate_fda_label()