#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import os
import json
from collections import defaultdict
import re

class variantMappingUtil:
    def __init__(self):
        ############################### handle gene ####################################
        df_gene = pd.read_csv(
            "D:\\drug KG\\pgkb\\genes\\genes.tsv",
            sep='\t',
            error_bad_lines=False
        ).fillna("")[["Symbol", "Chromosome"]]
        self.gene_chromosome_dict = dict(zip(list(df_gene["Symbol"].values),
                                             list(df_gene["Chromosome"].values)))

        ############################### handle rsID ####################################
        df_variants = pd.read_csv(
            'D:\\drug KG\\pgkb\\variants\\variants.tsv',
            sep='\t',
            error_bad_lines=False
        ).fillna("")
        df_variants = df_variants[["Variant ID", "Variant Name", "Gene Symbols",
                                   "Location", "Synonyms"]]

        self.variant_gene_dict = defaultdict(list)
        self.gene_variant_dict = defaultdict(list)
        self.variant_synonym_dict = defaultdict(list)
        self.synonym_variant_dict = {}
        self.variant_location_dict = {}
        self.variant_location_matched_synonym_dict = defaultdict(list)
        self.variant_NG_synonym_dict = defaultdict(list)
        self.variant_NC_synonym_dict = defaultdict(list)
        self.variant_rs_synonym_dict = defaultdict(list)
        for index, row in df_variants.iterrows():
            variant = row["Variant Name"]
            gene = row["Gene Symbols"]
            synonym = row["Synonyms"]
            location = row["Location"]
            if location != "" and variant != "":
                self.variant_location_dict[variant] = location

            if gene != "" and variant != "":
                gene_list = [x.strip() for x in gene.split(",")]
                self.variant_gene_dict[variant] = gene_list
                for g in gene_list:
                    self.gene_variant_dict[g].append(variant)

            if variant != "" and synonym != "":
                synonym_list = [x.strip() for x in synonym.split(",")]
                self.variant_synonym_dict[variant] = synonym_list
                for s in synonym_list:
                    self.synonym_variant_dict[s] = variant
                    if location != "" and location.split(":")[1] in s:
                        self.variant_location_matched_synonym_dict[variant].append(s)
                    if "NG" in s:
                        self.variant_NG_synonym_dict[variant].append(s)
                    if "NC" in s:
                        self.variant_NC_synonym_dict[variant].append(s)
                    if "rs" in s:
                        self.variant_rs_synonym_dict[variant].append(s)

        ############################### handle haplotype ###############################
        clinical_genes = [
            'TPMT', 'NAT2', 'G6PD', 'CYP3A5', 'CYP2A6', 'CYP3A4', 'CYP2C19',
            'UGT1A1', 'CYP2D6', 'NUDT15', 'CYP2B6', 'CYP2C9'
        ]  # and 'HLA-B', 'HLA-A'

        guideline_genes = [
            'CACNA1S', 'CFTR', 'CYP2C9', 'CYP2B6', 'CYP2D6', 'SLCO1B1',
            'UGT1A1', 'DPYD', 'NUDT15', 'MT-RNR1', 'CYP3A5', 'TPMT',
            'RYR1', 'CYP2C19', 'G6PD', 'IFNL3'
        ]  # and 'HLA-B', 'HLA-A'

        self.genes = set(clinical_genes + guideline_genes)

        haplotype_folder = 'D:\\drug KG\\pgkb\\haplotype'
        haplotype_path_dict = {g: "" for g in self.genes}
        for path in os.listdir(haplotype_folder):
            for gene in self.genes:
                if "{}_haplotypes".format(gene) in path:
                    haplotype_path_dict[gene] = os.path.join(haplotype_folder, path)

        self.gene_df_T_dict = {}
        for gene, path in haplotype_path_dict.items():
            df_tmp = pd.read_excel(
                path,
                engine="openpyxl",
                sheet_name="modified"
            ).fillna("")
            df_tmp_T = self.df_T_convert(df_tmp)
            self.gene_df_T_dict[gene] = df_tmp_T

        self.standard_haplotype_dict = {
            "CACNA1S": "Reference",
            "CFTR": "standard",
            "CYP2A6": "*1A",
            "CYP2B6": "*1",
            "CYP2C19": "*38",
            "CYP2C9": "*1",
            "CYP2D6": "*1",
            "CYP3A4": "*1",
            "CYP3A5": "*1",
            "DPYD": "Reference",
            "G6PD": "B (wildtype)",
            "IFNL3": "",
            "MT-RNR1": "Reference",
            "NAT2": "*4",
            "NUDT15": "*1",
            "RYR1": "Reference",
            "SLCO1B1": "*1A",
            "TPMT": "*1",
            "UGT1A1": "*1",
        }

    @staticmethod
    def df_T_convert(df):
        index = df.T.index
        values = df.T.values
        df_T = pd.DataFrame(values[1:, :], index=range(len(values) - 1), columns=values[0, :])
        df_index = pd.DataFrame({index[0]: index[1:]})
        df_T = pd.concat([df_index, df_T], axis=1)
        return df_T

    # input haplotype, return mapping dict
    def haplotype_mapping(self, haplotype):
        if "*" in haplotype:
            gene = haplotype.split("*")[0]
            h_type = "*" + haplotype.split("*")[1]
        else:
            gene = haplotype.split(" ")[0]
            h_type = " ".join(haplotype.split(" ")[1:])

        if gene not in self.genes:
            return {}

        # standard type
        if self.standard_haplotype_dict[gene] == h_type:
            return {}

        # check header
        if h_type not in self.gene_df_T_dict[gene].columns:
            return {}

        h_type_list = list(self.gene_df_T_dict[gene][h_type].values)

        mapping_dict = {}
        # rsID mapping
        # try:
        rsID_list = list(self.gene_df_T_dict[gene]["rsID"].values)
        # except:
        #     print(self.gene_df_T_dict[gene])
        #     print(gene)
        #     raise Exception()
        filter_rs_ID_list = list(filter(lambda x: x[1] != "" and "rs" in x[0], zip(rsID_list, h_type_list)))
        if len(filter_rs_ID_list) != 0:
            rsID_mapping_list = [x[0].strip() for x in filter_rs_ID_list]
            mapping_dict["rsID"] = rsID_mapping_list

        for column in self.gene_df_T_dict[gene].columns:
            if "nucleotide" in column.lower():
                nucleotide_list = self.gene_df_T_dict[gene][column]
                filter_nucleotide_list = list(
                    filter(lambda x: x[1] != "" and x[0] != "", zip(nucleotide_list, h_type_list)))
                if len(filter_nucleotide_list) != 0:
                    nucleotide_mapping_list = [x[0].strip() for x in filter_nucleotide_list]
                    mapping_dict["nucleotide"] = nucleotide_mapping_list

            if "NG" in column and "position" in column.lower():
                NG_position_list = self.gene_df_T_dict[gene][column]
                filter_NG_position_list = list(
                    filter(lambda x: x[1] != "" and x[0] != "", zip(NG_position_list, h_type_list)))
                if len(filter_NG_position_list) != 0:
                    NG_position_mapping_list = [x[0].strip() for x in filter_NG_position_list]
                    mapping_dict["NG"] = "{}; {}".format(column, NG_position_mapping_list)

            if "NC" in column and "position" in column.lower():
                NC_position_list = self.gene_df_T_dict[gene][column]
                filter_NC_position_list = list(
                    filter(lambda x: x[1] != "" and x[0] != "", zip(NC_position_list, h_type_list)))
                if len(filter_NC_position_list) != 0:
                    NC_position_mapping_list = [x[0].strip() for x in filter_NC_position_list]
                    mapping_dict["NC"] = "{}; {}".format(column, NC_position_mapping_list)

            if "protein" in column.lower():
                protein_list = self.gene_df_T_dict[gene][column]
                filter_protein_list = list(filter(lambda x: x[1] != "" and x[0] != "", zip(protein_list, h_type_list)))
                if len(filter_protein_list) != 0:
                    protein_mapping_list = [x[0].strip() for x in filter_protein_list]
                    mapping_dict["protein"] = "{}; {}".format(column, protein_mapping_list)

        return mapping_dict

    def rsID_mapping(self, rsID):
        mapping_dict = {}
        if rsID in self.variant_NC_synonym_dict.keys():
            mapping_dict["NC"] = self.variant_NC_synonym_dict[rsID]
        if rsID in self.variant_NG_synonym_dict.keys():
            mapping_dict["NG"] = self.variant_NG_synonym_dict[rsID]
        if rsID in self.variant_rs_synonym_dict.keys():
            mapping_dict["rsID"] = self.variant_rs_synonym_dict[rsID]

        return mapping_dict

if __name__ == "__main__":
    v_mapping = variantMappingUtil()
    print(v_mapping.haplotype_mapping("NUDT15*16"))
    print(v_mapping.haplotype_mapping("TPMT*2"))
    print(v_mapping.haplotype_mapping("TPMT*21"))
    print(v_mapping.haplotype_mapping("CYP2B6*17"))
    print(v_mapping.haplotype_mapping("CYP3A4*1D"))
    print(v_mapping.haplotype_mapping("G6PD Mediterranean, Dallas, Panama‚ Sassari, Cagliari, Birmingham"))
    print(v_mapping.rsID_mapping("rs6600880"))
    print(v_mapping.rsID_mapping("rs1000940"))
    print(v_mapping.rsID_mapping("rs10008257"))
    print(v_mapping.rsID_mapping("rs11931604"))
    print(v_mapping.rsID_mapping("rs62296959"))
    print(v_mapping.rsID_mapping("rs1001179"))
