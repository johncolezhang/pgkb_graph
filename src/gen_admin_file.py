#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from util.variant_mapping_util import variantMappingUtil
import pandas as pd
import json
from collections import defaultdict
import os
from fnmatch import fnmatch
from datetime import datetime

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


def generate_pgkb_variant_file():
    df_convert = pd.read_csv("processed/convert_position.csv", dtype=str).fillna("")
    df_position_map = pd.read_csv("processed/position_map.csv", dtype=str).fillna("")

    position_map_dict = {}
    for index, row in df_position_map.iterrows():
        position_map_dict[row["variant_name"]] = {}
        positions = row["position"].split(",")
        rsIDs = row["rsID"].split(",")
        for i in range(len(positions)):
            try:
                position_map_dict[row["variant_name"]][positions[i]] = rsIDs[i]
            except:
                pass

    rs_list = []
    gene_list = []
    for index, row in df_convert.iterrows():
        rsID = position_map_dict.get(row["haplotype_name"], {}).get(row["position"], "")
        rs_list.append(rsID)
        if "*" in row["haplotype_name"]:
            gene = row["haplotype_name"].split("*")[0]
        else:
            gene = row["haplotype_name"].split(" ")[0]
        gene_list.append(gene)

    df_convert["rsID"] = rs_list
    df_convert["gene"] = gene_list
    df_convert["update_time"] = [datetime.now().strftime("%Y-%m-%d")] * len(df_convert)
    df_convert.to_csv("d:/knowledge-base/static/pgkb_variant_all.csv", index=False)


if __name__ == "__main__":
    clinical_evidence()
    drug_label()
    cpic_evidence()
    pgkb_guideline()
    metabolizer_variant()
    generate_pharmvar_file()
    generate_pgkb_variant_file()