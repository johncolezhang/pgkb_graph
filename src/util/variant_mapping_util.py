#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import os
import json
from collections import defaultdict
import re
import fnmatch

class variantMappingUtil:
    def __init__(self):
        ############################### handle gene ####################################
        df_gene = pd.read_csv(
            "genes/genes.tsv",
            sep='\t',
            error_bad_lines=False
        ).fillna("")[["Symbol", "Chromosome"]]
        self.gene_chromosome_dict = dict(zip(list(df_gene["Symbol"].values),
                                             list(df_gene["Chromosome"].values)))

        # parse gene data update date
        self.gene_update_date = ""
        for x in os.listdir("genes"):
            if "CREATED" in x:
                self.gene_update_date = x.split("_")[1].split(".")[0]

        ############################### handle rsID ####################################
        # TODO file creation date is from variants folder
        df_variants = pd.read_csv(
            'variants/variants.tsv',
            sep='\t',
            error_bad_lines=False
        ).fillna("")
        df_variants = df_variants[["Variant ID", "Variant Name", "Gene Symbols",
                                   "Location", "Synonyms"]]

        # parse rsID update date
        self.rsID_update_date = ""
        for x in os.listdir("variants"):
            if "CREATED" in x:
                self.rsID_update_date = x.split("_")[1].split(".")[0]

        self.data_change_dict = defaultdict(dict)
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
        # clinical_genes = [
        #     'TPMT', 'NAT2', 'G6PD', 'CYP3A5', 'CYP2A6', 'CYP3A4', 'CYP2C19',
        #     'UGT1A1', 'CYP2D6', 'NUDT15', 'CYP2B6', 'CYP2C9'
        # ]  # and 'HLA-B', 'HLA-A'
        #
        # guideline_genes = [
        #     'CACNA1S', 'CFTR', 'CYP2C9', 'CYP2B6', 'CYP2D6', 'SLCO1B1',
        #     'UGT1A1', 'DPYD', 'NUDT15', 'MT-RNR1', 'CYP3A5', 'TPMT',
        #     'RYR1', 'CYP2C19', 'G6PD', 'IFNL3'
        # ]  # and 'HLA-B', 'HLA-A'
        #
        self.genes = []
        #
        # haplotype_folder = 'D:\\pgkb_graph\\haplotype'
        # haplotype_path_dict = {g: "" for g in self.genes}
        # for path in os.listdir(haplotype_folder):
        #     for gene in self.genes:
        #         if "{}_haplotypes".format(gene) in path:
        #             haplotype_path_dict[gene] = os.path.join(haplotype_folder, path)

        self.gene_df_T_dict = {}
        self.standard_haplotype_dict = {}
        # for gene, path in haplotype_path_dict.items():
        #     df_tmp = pd.read_excel(
        #         path,
        #         engine="openpyxl",
        #         sheet_name="modified",
        #         dtype=str
        #     ).fillna("")
        #     df_tmp_T = self.df_T_convert(df_tmp)
        #     self.gene_df_T_dict[gene] = df_tmp_T

        # self.standard_haplotype_dict = {
        #     "CACNA1S": "Reference",
        #     "CFTR": "standard",
        #     "CYP2A6": "*1A",
        #     "CYP2B6": "*1",
        #     "CYP2C19": "*38",
        #     "CYP2C9": "*1",
        #     "CYP2D6": "*1",
        #     "CYP3A4": "*1",
        #     "CYP3A5": "*1",
        #     "DPYD": "Reference",
        #     "G6PD": "B (wildtype)",
        #     "IFNL3": "",
        #     "MT-RNR1": "Reference",
        #     "NAT2": "*4",
        #     "NUDT15": "*1",
        #     "RYR1": "Reference",
        #     "SLCO1B1": "*1A",
        #     "TPMT": "*1",
        #     "UGT1A1": "*1",
        # }

        ############################# handle allele definition ##############
        allele_definition_folder = "allele_definition"
        allele_definition_path_dict = {}
        for path in os.listdir(allele_definition_folder):
            gene = path.split("_")[0]
            allele_definition_path_dict[gene] = os.path.join(allele_definition_folder, path)

        self.allele_definition_dict = {}
        for key, value in sorted(allele_definition_path_dict.items(), key=lambda x: x[0]):
            # generate gene_T dict and standard type dict
            self.parse_allele_definition(key, value)
            self.data_change_dict[key]["allele_definition"] = self.parse_latest_update_date(value)
            self.genes.append(key)

        ############################# handle diplotype ######################
        diplotype_folder = "diplotype"
        self.diplotype_genes = ["CYP2B6", "CYP2C9", "CYP2C19", "CYP2D6", "CYP3A5",
                                "DPYD", "NUDT15", "SLCO1B1", "TPMT", "UGT1A1"]

        diplotype_path_dict = {g: "" for g in self.diplotype_genes}
        for path in os.listdir(diplotype_folder):
            for gene in self.diplotype_genes:
                if "{}_Diplotype".format(gene) in path:
                    diplotype_path_dict[gene] = os.path.join(diplotype_folder, path)

        self.gene_diplotype_dict = {}
        self.gene_phenotype_dict = {}

        for key, value in sorted(diplotype_path_dict.items(), key=lambda x: x[0]):
            diplo_dict, pheno_dict = self.parse_diplotype(value)
            self.gene_diplotype_dict[key] = diplo_dict
            self.gene_phenotype_dict[key] = pheno_dict
            self.data_change_dict[key]["diplotype"] = self.parse_latest_update_date(value)

        ############################# handle frequency #####################
        frequency_folder = "frequency"

        self.frequency_genes = ["CACNA1S", "CYP2B6", "CYP2C9", "CYP2C19", "CYP2D6", "CYP3A5", "CYP4F2",
                                "DPYD", "HLA-A", "HLA-B", "MT-RNR1", "NUDT15", "RYR1", "SLCO1B1", "TPMT",
                                "UGT1A1", "VKORC1"]

        frequency_path_dict = {g: "" for g in self.frequency_genes}
        for path in os.listdir(frequency_folder):
            for gene in self.frequency_genes:
                if "{}_frequency_table".format(gene) in path:
                    frequency_path_dict[gene] = os.path.join(frequency_folder, path)

        self.gene_freq_dict = {}
        for key, value in sorted(frequency_path_dict.items(), key=lambda x: x[0]):
            freq_dict = self.parse_frequency(value)
            self.gene_freq_dict[key] = freq_dict
            self.data_change_dict[key]["frequency"] = self.parse_latest_update_date(value)

        ############################### handle functionality #################
        functionality_folder = "functionality"

        self.functionality_genes = ["CACNA1S", "CYP2B6", "CYP2C9", "CYP2C19", "CYP2D6", "CYP3A5",
                                    "DPYD", "MT-RNR1", "NUDT15", "RYR1", "SLCO1B1", "TPMT",
                                    "UGT1A1"]

        functionality_path_dict = {g: "" for g in self.functionality_genes}
        for path in os.listdir(functionality_folder):
            for gene in self.functionality_genes:
                if "{}_allele_functionality".format(gene) in path:
                    functionality_path_dict[gene] = os.path.join(functionality_folder, path)

        self.gene_functionality_dict = {}
        for key, value in sorted(functionality_path_dict.items(), key=lambda x: x[0]):
            func_dict = self.parse_functionality(value)
            self.gene_functionality_dict[key] = func_dict
            self.data_change_dict[key]["functionality"] = self.parse_latest_update_date(value)

    @staticmethod
    def check_first_row(xl_object, sheet_name):
        dd = list(pd.read_excel(xl_object, sheet_name=sheet_name, nrows=1).fillna("").columns)
        # only read first row
        first_row = list(pd.read_excel(xl_object, sheet_name=sheet_name, nrows=1).fillna("").columns)
        count = 0
        for x in first_row:
            if "unnamed" in str(x).lower():
                count += 1

            if count >= 2:
                return True
        return False

    @staticmethod
    def parse_latest_update_date(path):
        xl = pd.ExcelFile(path)
        for sheet in xl.sheet_names:
            if "change" in sheet.lower():
                df_change = pd.read_excel(xl, sheet_name=sheet, dtype=str).fillna("")

                for col in df_change.columns:
                    if "date" in col.lower():
                        return list(df_change[col].values)[-1]
        return ""


    def parse_allele_definition(self, gene, path):
        xl = pd.ExcelFile(path)

        for sheet in xl.sheet_names:
            if "allele" in sheet.lower():
                if self.check_first_row(xl, sheet):
                    df_definition = pd.read_excel(xl, sheet_name=sheet, skiprows=1, dtype=str).fillna("")
                else:
                    df_definition = pd.read_excel(xl, sheet_name=sheet, dtype=str).fillna("")

                df_T = self.df_T_convert(df_definition)
                self.gene_df_T_dict[gene] = df_T

                # find reference type
                reference_flag = False
                meet_rsid_flag = False
                for col in df_T.columns:
                    if col != "rsID" and not meet_rsid_flag:
                        continue
                    else:
                        meet_rsid_flag = True

                    if reference_flag and meet_rsid_flag:
                        self.standard_haplotype_dict[gene] = col
                        break
                    if "".join(list(df_T[col].values)) == "" and meet_rsid_flag:
                        reference_flag = True


    def parse_functionality(self, path):
        xl = pd.ExcelFile(path)
        allele_function_dict = {}

        for sheet in xl.sheet_names:
            if "function" in sheet.lower():
                ### read and handle functionalities
                if self.check_first_row(xl, sheet):
                    df_functionality = pd.read_excel(xl, sheet_name=sheet, skiprows=1, dtype=str).fillna("")
                else:
                    df_functionality = pd.read_excel(xl, sheet_name=sheet, dtype=str).fillna("")
                allele_column = ""
                function_status_column = ""
                activity_score_column = ""
                pmid_column = ""
                evidence_column = ""
                finding_column = ""
                nucleotide_column = ""
                protein_column = ""

                for column in df_functionality.columns:
                    if "rsid" in column.lower() or "allele/" in column.lower():
                        allele_column = column
                    elif "functional status" in column.lower() or "clinical function status" in column.lower():
                        function_status_column = column
                    elif "activity score" in column.lower():
                        activity_score_column = column
                    elif "pmid" in column.lower() or "reference" in column.lower():
                        pmid_column = column
                    elif "evidence" in column.lower():
                        evidence_column = column
                    elif "finding" in column.lower():
                        finding_column = column
                    elif "nucleotide" in column.lower():
                        nucleotide_column = column
                    elif "protein" in column.lower():
                        protein_column = column

                for index, row in df_functionality.iterrows():
                    if allele_column != "":
                        allele = row[allele_column]
                    else:
                        allele_column = list(df_functionality.columns)[0]
                        allele = row[allele_column]

                    function_status = row[function_status_column] if function_status_column != "" else ""
                    activity_score = row[activity_score_column] if activity_score_column != "" else ""
                    pmid = row[pmid_column] if pmid_column != "" else ""
                    evidence = row[evidence_column] if evidence_column != "" else ""
                    finding = row[finding_column] if finding_column != "" else ""
                    nucleotide = row[nucleotide_column] if nucleotide_column != "" else ""
                    protein = row[protein_column] if protein_column != "" else ""

                    if "{}{}{}{}{}{}{}".format(function_status, activity_score, pmid, evidence, finding, nucleotide,
                                               protein) != "":
                        allele_function_dict[allele] = {
                            "function_status": function_status,
                            "activity_score": activity_score,
                            "pmid": pmid.strip(),
                            "evidence": evidence,
                            "finding": finding,
                            "nucleotide": nucleotide,
                            "protein": protein
                        }
        return allele_function_dict


    def parse_frequency(self, path):
        xl = pd.ExcelFile(path)

        frequency_df_dict = {}
        for sheet in xl.sheet_names:
            if "frequency" in sheet.lower():
                if self.check_first_row(xl, sheet):
                    # skip first row
                    frequency_df_dict[sheet] = pd.read_excel(xl, sheet_name=sheet, skiprows=1, dtype=str).fillna("")
                else:
                    frequency_df_dict[sheet] = pd.read_excel(xl, sheet_name=sheet, dtype=str).fillna("")

        frequency_dict = {}
        for frequency_name, df in frequency_df_dict.items():
            columns = list(df.columns)
            allele_column = columns[0]
            bio_group_columns = columns[1:]
            allele_dict = {}
            for index, row in df.iterrows():
                allele = row[allele_column]
                bio_group_dict = {}
                for bio in bio_group_columns:
                    bio_group_dict[bio] = row[bio]
                allele_dict[allele] = bio_group_dict
            frequency_dict[frequency_name] = allele_dict
        return frequency_dict

    @staticmethod
    def parse_diplotype(path):
        xl = pd.ExcelFile(path)

        diplotype_dict = {}
        phenotype_dict = {}
        for sheet in xl.sheet_names:
            if "diplotype" in sheet.lower():
                ### read and handle diplotype
                df_diplotype = pd.read_excel(xl, sheet_name=sheet, dtype=str).fillna("")
                diplotype_column = ""
                phenotype_column = ""
                ehr_notation_column = ""
                score_column = ""
                variant_1_column = ""
                variant_2_column = ""

                for column in df_diplotype.columns:
                    if "diplotype" in column.lower() and diplotype_column == "":
                        diplotype_column = column
                    elif "phenotype" in column.lower() or "metabolizer" in column.lower() and phenotype_column == "":
                        phenotype_column = column
                    elif "notation" in column.lower():
                        ehr_notation_column = column
                    elif "score" in column.lower():
                        score_column = column
                    elif "variant" in column.lower() and "1" in column:
                        variant_1_column = column
                    elif "variant" in column.lower() and "2" in column:
                        variant_2_column = column

                for index, row in df_diplotype.iterrows():
                    if diplotype_column != "":
                        diplotype = row[diplotype_column]
                    elif variant_1_column != "" and variant_2_column != "":
                        diplotype = "{}/{}".format(row[variant_1_column], row[variant_2_column])
                    else:
                        continue

                    phenotype = row[phenotype_column].strip() if phenotype_column != "" else ""
                    ehr_notation = row[ehr_notation_column].strip() if ehr_notation_column != "" else ""
                    score = row[score_column] if score_column != "" else ""

                    if "{}{}{}".format(phenotype, ehr_notation, score) != "":
                        diplotype_dict[diplotype] = {
                            "phenotype": phenotype,
                            "ehr_notation": ehr_notation,
                            "score": score
                        }

            elif "consult note" in sheet.lower():
                ### read and handle consult note
                df_consult = pd.read_excel(xl, sheet_name=sheet, dtype=str).fillna("")
                phenotype_column = ""
                ehr_notation_column = ""
                consultation_column = ""
                score_column = ""
                for column in df_consult.columns:
                    if "phenotype" in column.lower() or "metabolizer" in column.lower():
                        phenotype_column = column
                    elif "notation" in column.lower():
                        ehr_notation_column = column
                    elif "consultation" in column.lower():
                        consultation_column = column
                    elif "score" in column.lower():
                        score_column = column

                for index, row in df_consult.iterrows():
                    phenotype = row[phenotype_column].strip()

                    consultation = row[consultation_column].strip() if consultation_column != "" else ""
                    ehr_notation = row[ehr_notation_column].strip() if ehr_notation_column != "" else ""
                    score = row[score_column] if score_column != "" else ""

                    if "{}{}{}".format(consultation, ehr_notation, score) != "":
                        phenotype_dict[phenotype] = {
                            "ehr_notation": ehr_notation,
                            "consultation": consultation,
                            "score": score
                        }
        return diplotype_dict, phenotype_dict

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
        gene, h_type = self.get_gene_h_type(haplotype)

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

        rsID_list = list(self.gene_df_T_dict[gene]["rsID"].values)

        filter_rs_ID_list = list(filter(lambda x: x[1] != "" and "rs" in x[0], zip(rsID_list, h_type_list)))
        if len(filter_rs_ID_list) != 0:
            rsID_mapping_list = [x[0].strip() for x in filter_rs_ID_list]
            mapping_dict["rsID"] = rsID_mapping_list

        for column in self.gene_df_T_dict[gene].columns:
            if "nucleotide" in column.lower() or "NM_" in column:
                nucleotide_list = self.gene_df_T_dict[gene][column]
                filter_nucleotide_list = list(
                    filter(lambda x: x[1] != "" and x[0] != "", zip(nucleotide_list, h_type_list)))
                if len(filter_nucleotide_list) != 0:
                    nucleotide_mapping_list = [x[0].strip() for x in filter_nucleotide_list]
                    mapping_dict["nucleotide"] = "{}; {}".format(column, nucleotide_mapping_list)
                    continue

            if "NG" in column and "position" in column.lower():
                NG_position_list = self.gene_df_T_dict[gene][column]
                filter_NG_position_list = list(
                    filter(lambda x: x[1] != "" and x[0] != "", zip(NG_position_list, h_type_list)))
                if len(filter_NG_position_list) != 0:
                    # one type may contain multi position, so list to store
                    NG_position_mapping_list = [x[0].strip() for x in filter_NG_position_list]
                    mapping_dict["NG"] = "{}; {}".format(column, NG_position_mapping_list)
                    continue

            if "NC" in column and "position" in column.lower():
                NC_position_list = self.gene_df_T_dict[gene][column]
                filter_NC_position_list = list(
                    filter(lambda x: x[1] != "" and x[0] != "", zip(NC_position_list, h_type_list)))
                if len(filter_NC_position_list) != 0:
                    NC_position_mapping_list = [x[0].strip() for x in filter_NC_position_list]
                    mapping_dict["NC"] = "{}; {}".format(column, NC_position_mapping_list)
                    continue

            if "protein" in column.lower():
                protein_list = self.gene_df_T_dict[gene][column]
                filter_protein_list = list(filter(lambda x: x[1] != "" and x[0] != "", zip(protein_list, h_type_list)))
                if len(filter_protein_list) != 0:
                    protein_mapping_list = [x[0].strip() for x in filter_protein_list]
                    mapping_dict["protein"] = "{}; {}".format(column, protein_mapping_list)
                    continue
        try:
            update_date = self.data_change_dict[gene]["allele_definition"]
        except:
            update_date = ""
        mapping_dict["update_date"] = update_date

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

    def diplotype_mapping(self, diplotype):
        d_list = diplotype.split(" ")
        gene = d_list[0]
        d_type = "".join(d_list[1:])

        if gene not in self.gene_diplotype_dict.keys():
            return {}
        if d_type not in self.gene_diplotype_dict[gene].keys():
            return {}

        try:
            update_date = self.data_change_dict[gene]["diplotype"]
        except:
            update_date = ""

        return {
            "phenotype": self.gene_diplotype_dict[gene][d_type]["phenotype"].strip(),
            "ehr_notation": self.gene_diplotype_dict[gene][d_type]["ehr_notation"].strip(),
            "activity_score": self.gene_diplotype_dict[gene][d_type]["score"],
            "consultation": self.gene_phenotype_dict[gene].get(
                self.gene_diplotype_dict[gene][d_type]["phenotype"].strip(),
                {}).get("consultation", ""),
            "update_date": update_date
        }

    def generate_diplotype_relation(self, output_path):
        gene_list = []
        diplotype_list = []
        variant_1_list = []
        variant_2_list = []
        phenotype_list = []
        ehr_notation_list = []
        activity_score_list = []
        consultation_list = []

        for gene, diplotype_dict in self.gene_diplotype_dict.items():
            for diplotype, result in diplotype_dict.items():
                [variant_1, variant_2] = diplotype.split("/")

                variant_1 = "{}{}".format(gene, variant_1.strip()) \
                    if "*" in variant_1 else "{} {}".format(gene, variant_1.strip())

                variant_2 = "{}{}".format(gene, variant_2.strip()) \
                    if "*" in variant_2 else "{} {}".format(gene, variant_2.strip())

                gene_list.append(gene)
                diplotype_list.append(diplotype)
                variant_1_list.append(variant_1)
                variant_2_list.append(variant_2)
                phenotype_list.append(result["phenotype"].strip())
                ehr_notation_list.append(result["ehr_notation"].strip())
                activity_score_list.append(result["score"])
                consultation_list.append(self.gene_phenotype_dict[gene].get(result["phenotype"].strip(),
                                                                            {}).get("consultation", ""))

        return pd.DataFrame({
            "gene": gene_list,
            "diplotype": diplotype_list,
            "variant1": variant_1_list,
            "variant2": variant_2_list,
            "phenotype": phenotype_list,
            "ehr_notation": ehr_notation_list,
            "activity_score": activity_score_list,
            "consultation": consultation_list
        }).to_csv(output_path, index=False)


    def haplotype_frequency_mapping(self, haplotype):
        gene, h_type = self.get_gene_h_type(haplotype)
        if gene not in self.gene_freq_dict.keys():
            return {}

        haplotype_freq_dict = {}
        for x in self.gene_freq_dict[gene].keys():
            if "allele" in x.lower():
                haplotype_freq_dict = self.gene_freq_dict[gene][x]

        if h_type in haplotype_freq_dict.keys():
            haplotype_freq_dict = haplotype_freq_dict[h_type]
        else:
            haplotype_freq_dict = {}

        haplotype_freq_dict = dict(list(map(lambda x: (x[0], str(x[1])), haplotype_freq_dict.items())))
        try:
            update_date = self.data_change_dict[gene]["frequency"]
        except:
            update_date = ""
        haplotype_freq_dict["update_date"] = update_date

        return haplotype_freq_dict


    def diplotype_frequency_mapping(self, diplotype):
        d_list = diplotype.split(" ")
        gene = d_list[0]
        d_type = "".join(d_list[1:])

        if gene not in self.gene_freq_dict.keys():
            return {}

        diplotype_freq_dict = {}
        for x in self.gene_freq_dict[gene].keys():
            if "diplotype" in x.lower():
                diplotype_freq_dict = self.gene_freq_dict[gene][x]

        if d_type in diplotype_freq_dict.keys():
            diplotype_freq_dict = diplotype_freq_dict[d_type]
        else:
            diplotype_freq_dict = {}

        diplotype_freq_dict = dict(list(map(lambda x: (x[0], str(x[1])), diplotype_freq_dict.items())))
        try:
            update_date = self.data_change_dict[gene]["frequency"]
        except:
            update_date = ""
        diplotype_freq_dict["update_date"] = update_date
        return diplotype_freq_dict


    def phenotype_frequency_mapping(self, phenotype):
        p_list = phenotype.split(" ")
        gene = p_list[0]
        pheno = " ".join(p_list[1:])

        if gene not in self.gene_freq_dict.keys():
            return {}

        phenotype_freq_dict = {}
        for x in self.gene_freq_dict[gene].keys():
            if "phenotype" in x.lower():
                phenotype_freq_dict = self.gene_freq_dict[gene][x]

        if pheno in phenotype_freq_dict.keys():
            phenotype_freq_dict = phenotype_freq_dict[pheno]
        else:
            phenotype_freq_dict = {}

        phenotype_freq_dict = dict(list(map(lambda x: (x[0], str(x[1])), phenotype_freq_dict.items())))
        return phenotype_freq_dict

    def haplotype_functionality_mapping(self, haplotype):
        gene, h_type = self.get_gene_h_type(haplotype)

        if gene not in self.gene_functionality_dict.keys():
            return {}

        if h_type not in self.gene_functionality_dict[gene].keys():
            return {}
        else:
            try:
                update_date = self.data_change_dict[gene]["frequency"]
            except:
                update_date = ""

            func_dict = self.gene_functionality_dict[gene][h_type]
            func_dict["update_date"] = update_date
            return func_dict

    @staticmethod
    def get_gene_h_type(haplotype):
        if "*" in haplotype:
            gene = haplotype.split("*")[0]
            h_type = "*" + haplotype.split("*")[1]
        else:
            gene = haplotype.split(" ")[0]
            h_type = " ".join(haplotype.split(" ")[1:])

        return gene, h_type


def test_haplotype():
    v_mapping = variantMappingUtil()
    print("############### haplotype info mapping #############")
    print(v_mapping.haplotype_mapping("NUDT15*16"))
    print(v_mapping.haplotype_mapping("TPMT*2"))
    print(v_mapping.haplotype_mapping("TPMT*21"))
    print(v_mapping.haplotype_mapping("CYP2B6*17"))
    # should be no result
    print(v_mapping.haplotype_mapping("CYP3A4*1D"))
    print(v_mapping.haplotype_mapping("G6PD Mediterranean, Dallas, Panama‚ Sassari, Cagliari, Birmingham"))
    print(v_mapping.rsID_mapping("rs6600880"))
    print(v_mapping.rsID_mapping("rs1000940"))
    print(v_mapping.rsID_mapping("rs10008257"))
    print(v_mapping.rsID_mapping("rs11931604"))
    print(v_mapping.rsID_mapping("rs62296959"))
    print(v_mapping.rsID_mapping("rs1001179"))
    print(v_mapping.haplotype_mapping("DPYD c.62G>A"))
    print("################# haplotype frequency mapping ###########")
    print(v_mapping.haplotype_frequency_mapping("CYP2B6*17"))
    print(v_mapping.haplotype_frequency_mapping("DPYD c.62G>A"))
    print(v_mapping.haplotype_frequency_mapping("CYP2D6*3"))
    print("################# haplotype functionality mapping ###########")
    print(v_mapping.haplotype_functionality_mapping("CYP2B6*17"))
    print(v_mapping.haplotype_functionality_mapping("DPYD c.62G>A"))
    print(v_mapping.haplotype_functionality_mapping("CYP2D6*3"))
    print(v_mapping.haplotype_functionality_mapping("TPMT*2"))

def test_diplotype():
    v_mapping = variantMappingUtil()
    print("################ diplotype info mapping ###################")
    print(v_mapping.diplotype_frequency_mapping("CYP2B6 *1/*4"))
    print(v_mapping.diplotype_frequency_mapping("CYP2C9 *1/*2"))
    print(v_mapping.diplotype_frequency_mapping("CYP2D6 *1/*10"))
    # should no result
    print(v_mapping.diplotype_frequency_mapping("CYP2B6 *1/*50"))

    print(v_mapping.gene_diplotype_dict["CYP2B6"]["*1/*4"])


def generate_diplotype():
    v_mapping = variantMappingUtil()
    v_mapping.generate_diplotype_relation(output_path="processed/diplotype_annotation.csv")

if __name__ == "__main__":
    test_haplotype()
    # generate_diplotype()
    # test_diplotype()
