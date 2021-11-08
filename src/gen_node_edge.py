#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import json
from copy import deepcopy
from util.variant_mapping_util import variantMappingUtil
import shutil
import os
from datetime import datetime
from collections import defaultdict

v_mapping = variantMappingUtil()

df_translate = pd.read_csv("processed/translate.csv", dtype=str, encoding="utf-8").fillna("")
translate_dict = dict(zip(
    list(df_translate["en_str"].values),
    list(df_translate["cn_str"].values)
))

def check_type(variant_name):
    if "rs" in variant_name:
        return "rsID"
    elif "/" in variant_name:
        return "diplotype"
    else:
        return "haplotype"

def edge_node(node, remain_label_list, remain_property_list):
    new_node = dict()
    new_node["node_ID"] = node["node_ID"]
    property_dict = deepcopy(node["property"])
    pk = list(property_dict.keys())
    for x in pk:
        if x not in remain_property_list:
            del property_dict[x]
    new_node["property"] = property_dict
    
    label_list = deepcopy(node["label"])
    for x in label_list:
        if x not in remain_label_list:
            label_list.remove(x)
    new_node["label"] = label_list
    return new_node

gene_variant_edge_list = []


def handle_clinical_csv():
    node_list = []
    edge_list = []

    df = pd.read_csv(
        "processed/clinical_drug_variant_annotation.csv",
        encoding="utf-8",
        dtype=str
    ).fillna("")

    for index, row in df.iterrows():
        ########################### Add node ######################
        variant_type = check_type(row["variant"])
        if variant_type == "rsID":
            mapping_dict = v_mapping.rsID_mapping(row["variant"])
            mapping_dict["update_date"] = v_mapping.rsID_update_date
            gene_list = v_mapping.variant_gene_dict.get(row["variant"], [])
            chromosome_list = [v_mapping.gene_chromosome_dict.get(x, "") for x in gene_list]

        elif variant_type == "haplotype":
            mapping_dict = v_mapping.haplotype_mapping(row["variant"])
            frequency_dict = v_mapping.haplotype_frequency_mapping(row["variant"])
            function_dict = v_mapping.haplotype_functionality_mapping(row["variant"])
            mapping_dict.update({"frequency": frequency_dict})
            mapping_dict.update({"functionality": function_dict})
            if "*" in row["variant"] and " " in row["variant"]:
                gene_list = [row["variant"].split(" ")[0].strip()]
            elif "*" in row["variant"]:
                gene_list = [row["variant"].split("*")[0].strip()]
            else:
                gene_list = [row["variant"].split(" ")[0].strip()]
            chromosome_list = [v_mapping.gene_chromosome_dict.get(x, "") for x in gene_list]

        else:
            # handle diplotype <-> drug clinical annotation
            mapping_dict = v_mapping.diplotype_mapping(row["variant"])
            mapping_dict.update({"frequency": v_mapping.diplotype_frequency_mapping(row["variant"])})

            diplotype_node = {
                "label": ["diplotype"],
                "node_ID": "diplotype_name",
                "property": {
                    "diplotype_name": row["variant"],
                    "display": row["variant"],
                    "phenotype": mapping_dict.get("phenotype", ""),
                    "ehr_notation": mapping_dict.get("ehr_notation", ""),
                    "activity_score": mapping_dict.get("activity_score", ""),
                    "consultation": mapping_dict.get("consultation", ""),
                    "update_date": mapping_dict.get("update_date", ""),
                    "frequency": str(mapping_dict.get("frequency", ""))
                }
            }
            node_list.append(diplotype_node)

            chemical_node = {
                "label": ["chemical"],
                "node_ID": "chemical_name",
                "property": {
                    "chemical_name": row["drug"],
                    "display": row["drug"],
                    # TODO map meshID
                    "meshID": "",
                    "link": v_mapping.chemical_link_dict.get(row["drug"], "")
                }
            }
            # add ATC node
            if row["drug"] in v_mapping.ATC_dict.keys():
                for key in v_mapping.ATC_dict[row["drug"]].keys():
                    chemical_node["property"][key] = v_mapping.ATC_dict[row["drug"]][key]

            node_list.append(chemical_node)

            # diplotype to chemical.
            relation_edge = {
                "start_node": edge_node(
                    diplotype_node,
                    remain_label_list=["diplotype"],
                    remain_property_list=["diplotype_name", "type"]
                ),
                "end_node": edge_node(
                    chemical_node,
                    remain_label_list=["chemical"],
                    remain_property_list=["chemical_name"]
                ),
                "edge": {
                    "label": "clinical_annotation",
                    "property": {
                        "data_source": "clinical_annotation",
                        "evidence_level": row["evidence_level"],
                        "phenotype_category": row["phenotype_category"],
                        "phenotype": row["phenotype"],
                        "phenotype_chn": translate_dict.get(row["phenotype"], ""),
                        "level_modifier": row["level_modifier"],
                        "score": row["score"],
                        "update_date": row["update_date"],
                        "link": row["link"]
                    }
                }
            }
            edge_list.append(relation_edge)
            continue

        gene_node_list = []
        for i in range(len(gene_list)):
            gene_node_list.append({
                "label": ["gene"],
                "node_ID": "gene_name",
                "property": {
                    "gene_name": gene_list[i],
                    "display": gene_list[i],
                    "chromosome": chromosome_list[i],
                    "update_date": v_mapping.gene_update_date,
                    "link": v_mapping.gene_link_dict.get(gene_list[i], ""),
                    "is_VIP": v_mapping.gene_is_vip.get(gene_list[i], ""),
                    "has_variant_annotation": v_mapping.gene_has_variant_annotation.get(gene_list[i], ""),
                    "has_cpic_dosing_guideline": v_mapping.gene_has_cpic_dosing_guideline.get(gene_list[i], ""),
                    "chromosomal_start_GRCh38": v_mapping.gene_chromosomal_start_GRCh38.get(gene_list[i], ""),
                    "chromosomal_stop_GRCh38": v_mapping.gene_chromosomal_stop_GRCh38.get(gene_list[i], ""),
                    "refseq": v_mapping.gene_refseq_rna.get(gene_list[i], ""),
                    "OMIM": v_mapping.gene_omim.get(gene_list[i], "")
                }
            })
        node_list.extend(gene_node_list)

        if variant_type in ["rsID", "haplotype"]:
            if variant_type == "rsID":
                variant_label = ["variant", "rsID"]
            else:
                variant_label = ["variant", "haplotype"]

            variant_node = {
                "label": variant_label,
                "node_ID": "variant_name",
                "property": {
                    "variant_name": row["variant"],
                    "display": row["variant"],
                    "type": variant_type,
                    "NC_change_code": mapping_dict.get("NC", ""),
                    "NG_change_code": mapping_dict.get("NG", ""),
                    "protein_change_code": mapping_dict.get("protein", ""),
                    "nucleotide_change_code": mapping_dict.get("nucleotide", ""),
                    "mapped_rsID": ",".join(mapping_dict.get("rsID", [])),
                    "update_date": mapping_dict.get("update_date", ""),
                    "frequency": str(mapping_dict.get("frequency", "")),
                    "functionality": str(mapping_dict.get("functionality", "")),
                    "link": v_mapping.variant_link_dict.get(row["variant"], "")
                }
            }
            node_list.append(variant_node)

            chemical_node = {
                "label": ["chemical"],
                "node_ID": "chemical_name",
                "property": {
                    "chemical_name": row["drug"],
                    "display": row["drug"],
                    # TODO map meshID
                    "meshID": "",
                    "link": v_mapping.chemical_link_dict.get(row["drug"], "")
                }
            }
            # add ATC node
            if row["drug"] in v_mapping.ATC_dict.keys():
                for key in v_mapping.ATC_dict[row["drug"]].keys():
                    chemical_node["property"][key] = v_mapping.ATC_dict[row["drug"]][key]

            node_list.append(chemical_node)

            ######################## Add edge #######################
            # variant to gene.
            for gene_node in gene_node_list:
                relation_edge = {
                    "start_node": edge_node(
                        variant_node,
                        remain_label_list=["variant"],
                        remain_property_list=["variant_name", "type"]
                    ),
                    "end_node": edge_node(
                        gene_node,
                        remain_label_list=["gene"],
                        remain_property_list=["gene_name"]
                    ),
                    "edge": {
                        "label": "mutation_at",
                        "property": {}
                    }
                }
                gene_variant_edge_list.append([
                    variant_node["property"]["display"],
                    gene_node["property"]["display"],
                    relation_edge
                ])

            # variant to chemical.
            relation_edge = {
                "start_node": edge_node(
                    variant_node,
                    remain_label_list=["variant"],
                    remain_property_list=["variant_name", "type"]
                ),
                "end_node": edge_node(
                    chemical_node,
                    remain_label_list=["chemical"],
                    remain_property_list=["chemical_name"]
                ),
                "edge": {
                    "label": "clinical_annotation",
                    "property": {
                        "data_source": "clinical_annotation",
                        "evidence_level": row["evidence_level"],
                        "phenotype_category": row["phenotype_category"],
                        "phenotype": row["phenotype"].replace("\"", "'"),
                        "phenotype_chn": translate_dict.get(row["phenotype"], "").replace("\"", "'"),
                        "level_modifier": row["level_modifier"],
                        "score": row["score"],
                        "update_date": row["update_date"],
                        "link": row["link"]
                    }
                }
            }
            edge_list.append(relation_edge)
    return node_list, edge_list


def handle_variant_drug_label_csv():
    node_list = []
    edge_list = []

    df = pd.read_csv(
        "processed/drug_variant_label.csv",
        encoding="utf-8",
        dtype=str
    ).fillna("")

    for index, row in df.iterrows():
        ########################### Add node ######################
        variant_type = check_type(row["variant"])
        if variant_type == "rsID":
            mapping_dict = v_mapping.rsID_mapping(row["variant"])
            mapping_dict["update_date"] = v_mapping.rsID_update_date
            gene_list = v_mapping.variant_gene_dict.get(row["variant"], [])
            chromosome_list = [v_mapping.gene_chromosome_dict.get(x, "") for x in gene_list]

        else:
            mapping_dict = v_mapping.haplotype_mapping(row["variant"])
            frequency_dict = v_mapping.haplotype_frequency_mapping(row["variant"])
            function_dict = v_mapping.haplotype_functionality_mapping(row["variant"])
            mapping_dict.update({"frequency": frequency_dict})
            mapping_dict.update({"functionality": function_dict})

            if "*" in row["variant"] and " " in row["variant"]:
                gene_list = [row["variant"].split(" ")[0].strip()]
            elif "*" in row["variant"]:
                gene_list = [row["variant"].split("*")[0].strip()]
            else:
                gene_list = [row["variant"].split(" ")[0].strip()]
            chromosome_list = [v_mapping.gene_chromosome_dict.get(x, "") for x in gene_list]

        gene_node_list = []
        for i in range(len(gene_list)):
            gene_node_list.append({
                "label": ["gene"],
                "node_ID": "gene_name",
                "property": {
                    "gene_name": gene_list[i],
                    "display": gene_list[i],
                    "chromosome": chromosome_list[i],
                    "update_date": v_mapping.gene_update_date,
                    "link": v_mapping.gene_link_dict.get(gene_list[i], ""),
                    "is_VIP": v_mapping.gene_is_vip.get(gene_list[i], ""),
                    "has_variant_annotation": v_mapping.gene_has_variant_annotation.get(gene_list[i], ""),
                    "has_cpic_dosing_guideline": v_mapping.gene_has_cpic_dosing_guideline.get(gene_list[i], ""),
                    "chromosomal_start_GRCh38": v_mapping.gene_chromosomal_start_GRCh38.get(gene_list[i], ""),
                    "chromosomal_stop_GRCh38": v_mapping.gene_chromosomal_stop_GRCh38.get(gene_list[i], ""),
                    "refseq": v_mapping.gene_refseq_rna.get(gene_list[i], ""),
                    "OMIM": v_mapping.gene_omim.get(gene_list[i], "")
                }
            })
        node_list.extend(gene_node_list)

        if variant_type == "rsID":
            variant_label = ["variant", "rsID"]
        else:
            variant_label = ["variant", "haplotype"]

        variant_node = {
            "label": variant_label,
            "node_ID": "variant_name",
            "property": {
                "variant_name": row["variant"],
                "display": row["variant"],
                "type": variant_type,
                "NC_change_code": mapping_dict.get("NC", ""),
                "NG_change_code": mapping_dict.get("NG", ""),
                "protein_change_code": mapping_dict.get("protein", ""),
                "nucleotide_change_code": mapping_dict.get("nucleotide", ""),
                "mapped_rsID": ",".join(mapping_dict.get("rsID", [])),
                "update_date": mapping_dict.get("update_date", ""),
                "frequency": str(mapping_dict.get("frequency", "")),
                "functionality": str(mapping_dict.get("functionality", "")),
                "link": v_mapping.variant_link_dict.get(row["variant"], "")
            }
        }
        node_list.append(variant_node)

        chemical_node = {
            "label": ["chemical"],
            "node_ID": "chemical_name",
            "property": {
                "chemical_name": row["drug"],
                "display": row["drug"],
                "meshID": ""
            }
        }
        # add ATC node
        if row["drug"] in v_mapping.ATC_dict.keys():
            for key in v_mapping.ATC_dict[row["drug"]].keys():
                chemical_node["property"][key] = v_mapping.ATC_dict[row["drug"]][key]

        node_list.append(chemical_node)

        ######################## Add edge #######################
        # variant to gene.
        for gene_node in gene_node_list:
            relation_edge = {
                "start_node": edge_node(
                    variant_node,
                    remain_label_list=["variant"],
                    remain_property_list=["variant_name", "type"]
                ),
                "end_node": edge_node(
                    gene_node,
                    remain_label_list=["gene"],
                    remain_property_list=["gene_name"]
                ),
                "edge": {
                    "label": "mutation_at",
                    "property": {}
                }
            }
            gene_variant_edge_list.append([
                variant_node["property"]["display"],
                gene_node["property"]["display"],
                relation_edge
            ])

        # variant to chemical.
        relation_edge = {
            "start_node": edge_node(
                variant_node,
                remain_label_list=["variant"],
                remain_property_list=["variant_name", "type"]
            ),
            "end_node": edge_node(
                chemical_node,
                remain_label_list=["chemical"],
                remain_property_list=["chemical_name"]
            ),
            "edge": {
                "label": "drug_label",
                "property": {
                    "data_source": "drug_label",
                    "organization": row["organization"],
                    "label_name": row["name"],
                    "testing_level": row["label"],
                    "update_date": row["update_date"],
                    "link": row["link"]
                }
            }
        }
        edge_list.append(relation_edge)
    return node_list, edge_list


def handle_gene_drug_label_csv():
    node_list = []
    edge_list = []

    df = pd.read_csv(
        "processed/drug_gene_label.csv",
        encoding="utf-8",
        dtype=str
    ).fillna("")

    for index, row in df.iterrows():
        ########################### Add node ######################
        gene_node = {
            "label": ["gene"],
            "node_ID": "gene_name",
            "property": {
                "gene_name": row["gene"],
                "display": row["gene"],
                "chromosome": v_mapping.gene_chromosome_dict.get(row["gene"], ""),
                "link": v_mapping.gene_link_dict.get(row["gene"], ""),
                "is_VIP": v_mapping.gene_is_vip.get(row["gene"], ""),
                "has_variant_annotation": v_mapping.gene_has_variant_annotation.get(row["gene"], ""),
                "has_cpic_dosing_guideline": v_mapping.gene_has_cpic_dosing_guideline.get(row["gene"], ""),
                "chromosomal_start_GRCh38": v_mapping.gene_chromosomal_start_GRCh38.get(row["gene"], ""),
                "chromosomal_stop_GRCh38": v_mapping.gene_chromosomal_stop_GRCh38.get(row["gene"], ""),
                "refseq": v_mapping.gene_refseq_rna.get(row["gene"], ""),
                "OMIM": v_mapping.gene_omim.get(row["gene"], "")
            }
        }
        node_list.append(gene_node)

        chemical_node = {
            "label": ["chemical"],
            "node_ID": "chemical_name",
            "property": {
                "chemical_name": row["drug"],
                "display": row["drug"],
                "meshID": ""
            }
        }

        # add ATC node
        if row["drug"] in v_mapping.ATC_dict.keys():
            for key in v_mapping.ATC_dict[row["drug"]].keys():
                chemical_node["property"][key] = v_mapping.ATC_dict[row["drug"]][key]

        node_list.append(chemical_node)

        ######################## Add edge #######################
        # gene to chemical.
        relation_edge = {
            "start_node": edge_node(
                gene_node,
                remain_label_list=["gene"],
                remain_property_list=["gene_name"]
            ),
            "end_node": edge_node(
                chemical_node,
                remain_label_list=["chemical"],
                remain_property_list=["chemical_name"]
            ),
            "edge": {
                "label": "drug_label",
                "property": {
                    "data_source": "drug_label",
                    "organization": row["organization"],
                    "label_name": row["name"],
                    "testing_level": row["label"],
                    "update_date": row["update_date"],
                    "link": row["link"]
                }
            }
        }
        edge_list.append(relation_edge)
    return node_list, edge_list


def handle_guideline_csv():
    node_list = []
    edge_list = []

    df = pd.read_csv(
        "processed/guideline_drug_variant_annotation.csv",
        encoding="utf-8",
        dtype=str
    ).fillna("")

    for index, row in df.iterrows():
        ########################### Add node ######################
        variant_type = check_type(row["haplotype"])
        if variant_type == "rsID":
            mapping_dict = v_mapping.rsID_mapping(row["haplotype"])
            mapping_dict["update_date"] = v_mapping.rsID_update_date
            gene_list = v_mapping.variant_gene_dict.get(row["haplotype"], [])
            chromosome_list = [v_mapping.gene_chromosome_dict.get(x, "") for x in gene_list]

        else:
            mapping_dict = v_mapping.haplotype_mapping(row["haplotype"])
            frequency_dict = v_mapping.haplotype_frequency_mapping(row["haplotype"])
            function_dict = v_mapping.haplotype_functionality_mapping(row["haplotype"])
            mapping_dict.update({"frequency": frequency_dict})
            mapping_dict.update({"functionality": function_dict})

            if "*" in row["haplotype"] and " " in row["haplotype"]:
                gene_list = [row["haplotype"].split(" ")[0].strip()]
            elif "*" in row["haplotype"]:
                gene_list = [row["haplotype"].split("*")[0].strip()]
            else:
                gene_list = [row["haplotype"].split(" ")[0].strip()]
            chromosome_list = [v_mapping.gene_chromosome_dict.get(x, "") for x in gene_list]

        gene_node_list = []
        for i in range(len(gene_list)):
            gene_node_list.append({
                "label": ["gene"],
                "node_ID": "gene_name",
                "property": {
                    "gene_name": gene_list[i],
                    "display": gene_list[i],
                    "chromosome": chromosome_list[i],
                    "update_date": v_mapping.gene_update_date,
                    "link": v_mapping.gene_link_dict.get(row["gene"], ""),
                    "is_VIP": v_mapping.gene_is_vip.get(gene_list[i], ""),
                    "has_variant_annotation": v_mapping.gene_has_variant_annotation.get(gene_list[i], ""),
                    "has_cpic_dosing_guideline": v_mapping.gene_has_cpic_dosing_guideline.get(gene_list[i], ""),
                    "chromosomal_start_GRCh38": v_mapping.gene_chromosomal_start_GRCh38.get(gene_list[i], ""),
                    "chromosomal_stop_GRCh38": v_mapping.gene_chromosomal_stop_GRCh38.get(gene_list[i], ""),
                    "refseq": v_mapping.gene_refseq_rna.get(gene_list[i], ""),
                    "OMIM": v_mapping.gene_omim.get(gene_list[i], "")
                }
            })
        node_list.extend(gene_node_list)

        if variant_type == "rsID":
            variant_label = ["variant", "rsID"]
        else:
            variant_label = ["variant", "haplotype"]

        variant_node = {
            "label": variant_label,
            "node_ID": "variant_name",
            "property": {
                "variant_name": row["haplotype"],
                "display": row["haplotype"],
                "type": variant_type,
                "NC_change_code": mapping_dict.get("NC", ""),
                "NG_change_code": mapping_dict.get("NG", ""),
                "protein_change_code": mapping_dict.get("protein", ""),
                "nucleotide_change_code": mapping_dict.get("nucleotide", ""),
                "mapped_rsID": ",".join(mapping_dict.get("rsID", [])),
                "update_date": mapping_dict.get("update_date", ""),
                "frequency": str(mapping_dict.get("frequency", "")),
                "functionality": str(mapping_dict.get("functionality", "")),
                "link": v_mapping.variant_link_dict.get(row["haplotype"], ""),
                "is_reference": str(mapping_dict.get("is_reference", ""))
            }
        }
        node_list.append(variant_node)

        chemical_node = {
            "label": ["chemical"],
            "node_ID": "chemical_name",
            "property": {
                "chemical_name": row["drug"],
                "display": row["drug"],
                "meshID": ""
            }
        }
        # add ATC node
        if row["drug"] in v_mapping.ATC_dict.keys():
            for key in v_mapping.ATC_dict[row["drug"]].keys():
                chemical_node["property"][key] = v_mapping.ATC_dict[row["drug"]][key]

        node_list.append(chemical_node)

        ######################## Add edge #######################
        # variant to gene.
        for gene_node in gene_node_list:
            relation_edge = {
                "start_node": edge_node(
                    variant_node,
                    remain_label_list=["variant"],
                    remain_property_list=["variant_name", "type"]
                ),
                "end_node": edge_node(
                    gene_node,
                    remain_label_list=["gene"],
                    remain_property_list=["gene_name"]
                ),
                "edge": {
                    "label": "mutation_at",
                    "property": {}
                }
            }
            gene_variant_edge_list.append([
                variant_node["property"]["display"],
                gene_node["property"]["display"],
                relation_edge
            ])

        # variant to chemical.
        relation_edge = {
            "start_node": edge_node(
                variant_node,
                remain_label_list=["variant"],
                remain_property_list=["variant_name", "type"]
            ),
            "end_node": edge_node(
                chemical_node,
                remain_label_list=["chemical"],
                remain_property_list=["chemical_name"]
            ),
            "edge": {
                "label": "guideline_annotation",
                "property": {
                    "data_source": "guideline_annotation",
                    "guideline_institute": row["guideline_institute"],
                    "guideline_name": row["guideline_name"],
                    "guideline_name_chn": translate_dict.get(row["guideline_name"], ""),
                    "guideline_term": row["term"],
                    "guideline_link": row["guideline_link"],
                    "cancer_genome": row["cancer_genome"],
                    "literature": row["literature"],
                    "source": row["source"],
                    "update_date": row["update_date"]
                }
            }
        }
        edge_list.append(relation_edge)
    return node_list, edge_list


def handle_diplotype_drug_csv():
    node_list = []
    edge_list = []
    df = pd.read_csv(
        "processed/diplotype_drug_relation.csv",
        encoding="utf-8",
        dtype=str
    )

    check_set = []
    for index, row in df.iterrows():
        ########################### Add node ######################
        diplotype = row["diplotype"].replace("\"", "'")
        dip_mapping_dict = v_mapping.diplotype_mapping(diplotype)
        dip_mapping_dict.update({"frequency": v_mapping.diplotype_frequency_mapping(diplotype)})

        diplotype_node = {
            "label": ["diplotype"],
            "node_ID": "diplotype_name",
            "property": {
                "diplotype_name": diplotype.replace("\"", "'"),
                "display": diplotype.replace("\"", "'"),
                "phenotype": dip_mapping_dict.get("phenotype", ""),
                "ehr_notation": dip_mapping_dict.get("ehr_notation", ""),
                "activity_score": dip_mapping_dict.get("activity_score", ""),
                "consultation": dip_mapping_dict.get("consultation", ""),
                "update_date": dip_mapping_dict.get("update_date", ""),
                "frequency": str(dip_mapping_dict.get("frequency", ""))
            }
        }
        node_list.append(diplotype_node)

        chemical_node = {
            "label": ["chemical"],
            "node_ID": "chemical_name",
            "property": {
                "chemical_name": row["drug"],
                "display": row["drug"],
                "meshID": ""
            }
        }

        # add ATC node
        if row["drug"] in v_mapping.ATC_dict.keys():
            for key in v_mapping.ATC_dict[row["drug"]].keys():
                chemical_node["property"][key] = v_mapping.ATC_dict[row["drug"]][key]

        node_list.append(chemical_node)

        ################################## add edge ##############################
        if "{}{}{}".format(row["diplotype"], row["drug"], row["organization"]) in check_set:
            continue
        else:
            check_set.append("{}{}{}".format(row["diplotype"], row["drug"], row["organization"]))
            # diplotype to chemical.
            relation_edge = {
                "start_node": edge_node(
                    diplotype_node,
                    remain_label_list=["diplotype"],
                    remain_property_list=["diplotype_name"]
                ),
                "end_node": edge_node(
                    chemical_node,
                    remain_label_list=["chemical"],
                    remain_property_list=["chemical_name"]
                ),
                "edge": {
                    "label": "guideline_annotation",
                    "property": {
                        "data_source": "guideline_annotation",
                        "phenotype": row["phenotype"],
                        "phenotype_category": row["phenotype_category"],
                        "genotype": row["genotype"],
                        "genotype_chn": translate_dict.get(row["genotype"], ""),
                        "implication": str(row["implication"]).replace("\"", "'"),
                        "implication_chn": translate_dict.get(row["implication"], "").replace("\"", "'"),
                        "description": str(row["description"]).replace("\"", "'"),
                        "recommendation": str(row["recommendation"]).replace("\"", "'"),
                        "recommendation_chn": translate_dict.get(row["recommendation"], "").replace("\"", "'"),
                        "organization": str(row["organization"]).replace("\"", "'"),
                        "title": row["title"].replace("\"", "'"),
                        "link": row["link"],
                        "update_date": row["update_date"]
                    }
                }
            }
            edge_list.append(relation_edge)
    return node_list, edge_list


def handle_research_csv():
    ################### parse metabolizer drug dict ###############
    metabolizer_drug_dict = defaultdict(dict)
    df = pd.read_csv(
        "processed/phenotype_drug_relation.csv",
        encoding="utf-8",
        dtype=str
    ).fillna("")
    for pheno_gene, content in df.groupby(["phenotype_category", "gene"]):
        drug_dict = defaultdict(list)
        for drug, data in content.groupby("drug"):
            for i, r in data.iterrows():
                drug_dict[drug].append({
                    "data_source": "guideline_annotation",
                    "phenotype": r["phenotype"],
                    "phenotype_category": r["phenotype_category"],
                    "genotype": r["genotype"],
                    "implication": str(r["implication"]).replace("\"", "'"),
                    "implication_chn": translate_dict.get(r["implication"], "").replace("\"", "'"),
                    "description": str(r["description"]).replace("\"", "'"),
                    "description_chn": translate_dict.get(r["description"], "").replace("\"", "'"),
                    "recommendation": str(r["recommendation"]).replace("\"", "'"),
                    "recommendation_chn": translate_dict.get(r["recommendation"], "").replace("\"", "'"),
                    "organization": str(r["organization"]).replace("\"", "'"),
                    "title": str(r["title"]).replace("\"", "'"),
                    "link": r["link"],
                    "update_date": r["update_date"]
                })
        metabolizer_drug_dict["{} {}".format(pheno_gene[1], pheno_gene[0])] = drug_dict

    node_list = []
    edge_list = []
    meta_edge_set = []

    #################### part 1: variant to drug research #########
    df = pd.read_csv(
        "processed/research_drug_variant_annotation.csv",
        encoding="utf-8",
        dtype=str
    ).fillna("")

    for index, row in df.iterrows():
        ########################### Add node ######################
        variant_type = check_type(row["variant"])
        if variant_type == "rsID":
            mapping_dict = v_mapping.rsID_mapping(row["variant"])
            mapping_dict["update_date"] = v_mapping.rsID_update_date
            gene_list = v_mapping.variant_gene_dict.get(row["variant"], [])
            # gene_list = list(set(v_mapping.genes).intersection(set(gene_list)))
            chromosome_list = [v_mapping.gene_chromosome_dict.get(x, "") for x in gene_list]

        else:
            mapping_dict = v_mapping.haplotype_mapping(row["variant"])
            frequency_dict = v_mapping.haplotype_frequency_mapping(row["variant"])
            function_dict = v_mapping.haplotype_functionality_mapping(row["variant"])
            mapping_dict.update({"frequency": frequency_dict})
            mapping_dict.update({"functionality": function_dict})

            if "*" in row["variant"] and " " in row["variant"]:
                gene_list = [row["variant"].split(" ")[0].strip()]
            elif "*" in row["variant"]:
                gene_list = [row["variant"].split("*")[0].strip()]
            else:
                gene_list = [row["variant"].split(" ")[0].strip()]
            chromosome_list = [v_mapping.gene_chromosome_dict.get(x, "") for x in gene_list]

        gene_node_list = []
        for i in range(len(gene_list)):
            gene_node_list.append({
                "label": ["gene"],
                "node_ID": "gene_name",
                "property": {
                    "gene_name": gene_list[i],
                    "display": gene_list[i],
                    "chromosome": chromosome_list[i],
                    "update_date": v_mapping.gene_update_date,
                    "link": v_mapping.gene_link_dict.get(gene_list[i], ""),
                    "is_VIP": v_mapping.gene_is_vip.get(gene_list[i], ""),
                    "has_variant_annotation": v_mapping.gene_has_variant_annotation.get(gene_list[i], ""),
                    "has_cpic_dosing_guideline": v_mapping.gene_has_cpic_dosing_guideline.get(gene_list[i], ""),
                    "chromosomal_start_GRCh38": v_mapping.gene_chromosomal_start_GRCh38.get(gene_list[i], ""),
                    "chromosomal_stop_GRCh38": v_mapping.gene_chromosomal_stop_GRCh38.get(gene_list[i], ""),
                    "refseq": v_mapping.gene_refseq_rna.get(gene_list[i], ""),
                    "OMIM": v_mapping.gene_omim.get(gene_list[i], "")
                }
            })
        node_list.extend(gene_node_list)

        if variant_type == "rsID":
            variant_label = ["variant", "rsID"]
        else:
            variant_label = ["variant", "haplotype"]

        variant_node = {
            "label": variant_label,
            "node_ID": "variant_name",
            "property": {
                "variant_name": row["variant"],
                "display": row["variant"],
                "type": variant_type,
                "NC_change_code": mapping_dict.get("NC", ""),
                "NG_change_code": mapping_dict.get("NG", ""),
                "protein_change_code": mapping_dict.get("protein", ""),
                "nucleotide_change_code": mapping_dict.get("nucleotide", ""),
                "mapped_rsID": ",".join(mapping_dict.get("rsID", [])),
                "update_date": mapping_dict.get("update_date", ""),
                "frequency": str(mapping_dict.get("frequency", "")),
                "functionality": str(mapping_dict.get("functionality", "")),
                "link": v_mapping.variant_link_dict.get(row["variant"], ""),
                "is_reference": str(mapping_dict.get("is_reference", ""))
            }
        }
        node_list.append(variant_node)

        chemical_node = {
            "label": ["chemical"],
            "node_ID": "chemical_name",
            "property": {
                "chemical_name": row["drug"],
                "display": row["drug"],
                "meshID": ""
            }
        }

        # add ATC node
        if row["drug"] in v_mapping.ATC_dict.keys():
            for key in v_mapping.ATC_dict[row["drug"]].keys():
                chemical_node["property"][key] = v_mapping.ATC_dict[row["drug"]][key]

        node_list.append(chemical_node)

        ######################## Add edge #######################
        # variant to gene.
        for gene_node in gene_node_list:
            relation_edge = {
                "start_node": edge_node(
                    variant_node,
                    remain_label_list=["variant"],
                    remain_property_list=["variant_name", "type"]
                ),
                "end_node": edge_node(
                    gene_node,
                    remain_label_list=["gene"],
                    remain_property_list=["gene_name"]
                ),
                "edge": {
                    "label": "mutation_at",
                    "property": {}
                }
            }
            gene_variant_edge_list.append([
                variant_node["property"]["display"],
                gene_node["property"]["display"],
                relation_edge
            ])

        # variant to chemical.
        relation_edge = {
            "start_node": edge_node(
                variant_node,
                remain_label_list=["variant"],
                remain_property_list=["variant_name", "type"]
            ),
            "end_node": edge_node(
                chemical_node,
                remain_label_list=["chemical"],
                remain_property_list=["chemical_name"]
            ),
            "edge": {
                "label": "research_annotation",
                "property": {
                    "data_source": "research_annotation",
                    "p_value": row["p_value"],
                    "phenotype_category": row["phenotype_category"],
                    "bio_geo_group": row["bio_geo_group"],
                    "PMID": row["PMID"],
                    "PMID_link": row["PMID_link"],
                    "note": row["note"].replace("\"", "'"),
                    "note_chn": translate_dict.get(row["note"], "").replace("\"", "'"),
                    "sentence": row["sentence"].replace("\"", "'"),
                    "sentence_chn": translate_dict.get(row["sentence"], "").replace("\"", "'"),
                    "update_date": row["update_date"],
                    "link": row["link"]
                }
            }
        }
        edge_list.append(relation_edge)

    #################### part 2: diplotype to drug research #########
    df = pd.read_csv(
        "processed/research_drug_diplotype_annotation.csv",
        encoding="utf-8",
        dtype=str
    ).fillna("")

    for index, row in df.iterrows():
        diplotype = row["diplotype"].replace("\"", "'")
        gene = diplotype.split(" ")[0]
        variant_1 = "{}{}".format(gene, "".join(diplotype.split(" ")[1:]).split("/")[0]).replace("\"", "'")
        variant_2 = "{}{}".format(gene, "".join(diplotype.split(" ")[1:]).split("/")[1]).replace("\"", "'")

        dip_mapping_dict = v_mapping.diplotype_mapping(diplotype)
        dip_mapping_dict.update({"frequency": v_mapping.diplotype_frequency_mapping(diplotype)})

        hap1_mapping_dict = v_mapping.haplotype_mapping(variant_1)
        hap1_frequency_dict = v_mapping.haplotype_frequency_mapping(variant_1)
        hap1_function_dict = v_mapping.haplotype_functionality_mapping(variant_1)
        hap1_mapping_dict.update({"frequency": hap1_frequency_dict})
        hap1_mapping_dict.update({"functionality": hap1_function_dict})

        hap2_mapping_dict = v_mapping.haplotype_mapping(variant_2)
        hap2_frequency_dict = v_mapping.haplotype_frequency_mapping(variant_2)
        hap2_function_dict = v_mapping.haplotype_functionality_mapping(variant_2)
        hap2_mapping_dict.update({"frequency": hap2_frequency_dict})
        hap2_mapping_dict.update({"functionality": hap2_function_dict})

        diplotype_node = {
            "label": ["diplotype"],
            "node_ID": "diplotype_name",
            "property": {
                "diplotype_name": diplotype.replace("\"", "'"),
                "display": diplotype.replace("\"", "'"),
                "phenotype": dip_mapping_dict.get("phenotype", ""),
                "ehr_notation": dip_mapping_dict.get("ehr_notation", ""),
                "activity_score": dip_mapping_dict.get("activity_score", ""),
                "consultation": dip_mapping_dict.get("consultation", ""),
                "update_date": dip_mapping_dict.get("update_date", ""),
                "frequency": str(dip_mapping_dict.get("frequency", ""))
            }
        }
        node_list.append(diplotype_node)

        variant_1_node = {
            "label": ["variant", "haplotype"],
            "node_ID": "variant_name",
            "property": {
                "variant_name": variant_1,
                "display": variant_1,
                "type": "haplotype",
                "NC_change_code": hap1_mapping_dict.get("NC", ""),
                "NG_change_code": hap1_mapping_dict.get("NG", ""),
                "protein_change_code": hap1_mapping_dict.get("protein", ""),
                "nucleotide_change_code": hap1_mapping_dict.get("nucleotide", ""),
                "mapped_rsID": ",".join(hap1_mapping_dict.get("rsID", [])),
                "update_date": hap1_mapping_dict.get("update_date", ""),
                "frequency": str(hap1_mapping_dict.get("frequency", "")),
                "functionality": str(hap1_mapping_dict.get("functionality", "")),
                "is_reference": str(hap1_mapping_dict.get("is_reference", ""))
            }
        }
        node_list.append(variant_1_node)

        variant_2_node = {
            "label": ["variant", "haplotype"],
            "node_ID": "variant_name",
            "property": {
                "variant_name": variant_2,
                "display": variant_2,
                "type": "haplotype",
                "NC_change_code": hap2_mapping_dict.get("NC", ""),
                "NG_change_code": hap2_mapping_dict.get("NG", ""),
                "protein_change_code": hap2_mapping_dict.get("protein", ""),
                "nucleotide_change_code": hap2_mapping_dict.get("nucleotide", ""),
                "mapped_rsID": ",".join(hap2_mapping_dict.get("rsID", [])),
                "update_date": hap2_mapping_dict.get("update_date", ""),
                "frequency": str(hap2_mapping_dict.get("frequency", "")),
                "functionality": str(hap2_mapping_dict.get("functionality", "")),
                "is_reference": str(hap2_mapping_dict.get("is_reference", ""))
            }
        }
        node_list.append(variant_2_node)

        chemical_node = {
            "label": ["chemical"],
            "node_ID": "chemical_name",
            "property": {
                "chemical_name": row["drug"],
                "display": row["drug"],
                "meshID": ""
            }
        }

        # add ATC node
        if row["drug"] in v_mapping.ATC_dict.keys():
            for key in v_mapping.ATC_dict[row["drug"]].keys():
                chemical_node["property"][key] = v_mapping.ATC_dict[row["drug"]][key]

        node_list.append(chemical_node)

        ########################### Add edge #######################
        # diplotype to haplotype.
        hap1_edge = {
            "start_node": edge_node(
                diplotype_node,
                remain_label_list=["diplotype"],
                remain_property_list=["diplotype_name"]
            ),
            "end_node": edge_node(
                variant_1_node,
                remain_label_list=["haplotype"],
                remain_property_list=["variant_name"]
            ),
            "edge": {
                "label": "diplotype_consist_of",
                "property": {}
            }
        }
        edge_list.append(hap1_edge)

        # diplotype to haplotype.
        hap2_edge = {
            "start_node": edge_node(
                diplotype_node,
                remain_label_list=["diplotype"],
                remain_property_list=["diplotype_name"]
            ),
            "end_node": edge_node(
                variant_2_node,
                remain_label_list=["haplotype"],
                remain_property_list=["variant_name"]
            ),
            "edge": {
                "label": "diplotype_consist_of",
                "property": {}
            }
        }
        edge_list.append(hap2_edge)

        # diplotype to chemical
        dip_edge = {
            "start_node": edge_node(
                diplotype_node,
                remain_label_list=["diplotype"],
                remain_property_list=["diplotype_name"]
            ),
            "end_node": edge_node(
                chemical_node,
                remain_label_list=["chemical"],
                remain_property_list=["chemical_name"]
            ),
            "edge": {
                "label": "research_annotation",
                "property": {
                    "data_source": "research_annotation",
                    "p_value": row["p_value"],
                    "phenotype_category": row["phenotype_category"],
                    "bio_geo_group": row["bio_geo_group"],
                    "PMID": row["PMID"],
                    "PMID_link": row["PMID_link"],
                    "note": row["note"].replace("\"", "'"),
                    "note_chn": translate_dict.get(row["note"], "").replace("\"", "'"),
                    "sentence": row["sentence"].replace("\"", "'"),
                    "sentence_chn": translate_dict.get(row["sentence"], "").replace("\"", "'"),
                    "update_date": row["update_date"],
                    "link": row["link"]
                }
            }
        }
        edge_list.append(dip_edge)

        ############### Add diplotype<->drug metabolize relation ###############
        # to control duplicate in relations
        if "phenotype" in dip_mapping_dict.keys() and dip_mapping_dict["phenotype"] in metabolizer_drug_dict.keys():
            for drug in metabolizer_drug_dict[dip_mapping_dict["phenotype"]].keys():
                chemical_node = {
                    "label": ["chemical"],
                    "node_ID": "chemical_name",
                    "property": {
                        "chemical_name": drug,
                        "display": drug,
                        "meshID": ""
                    }
                }

                # add ATC node
                if drug in v_mapping.ATC_dict.keys():
                    for key in v_mapping.ATC_dict[drug].keys():
                        chemical_node["property"][key] = v_mapping.ATC_dict[drug][key]

                node_list.append(chemical_node)

                if "{}{}{}".format(diplotype, dip_mapping_dict["phenotype"], drug) in meta_edge_set:
                    continue
                else:
                    meta_edge_set.append("{}{}{}".format(diplotype, dip_mapping_dict["phenotype"], drug))

                for data in metabolizer_drug_dict[dip_mapping_dict["phenotype"]][drug]:
                    # diplotype to chemical
                    meta_edge = {
                        "start_node": edge_node(
                            diplotype_node,
                            remain_label_list=["diplotype"],
                            remain_property_list=["diplotype_name"]
                        ),
                        "end_node": edge_node(
                            chemical_node,
                            remain_label_list=["chemical"],
                            remain_property_list=["chemical_name"]
                        ),
                        "edge": {
                            "label": "diplotype_metabolizer",
                            "property": {
                                "data_source": data["data_source"],
                                "phenotype": data["phenotype"],
                                "phenotype_category": data["phenotype_category"],
                                "genotype": data["genotype"].replace("\"", "'"),
                                "implication": data["implication"].replace("\"", "'"),
                                "implication_chn": data["implication_chn"].replace("\"", "'"),
                                "description": data["description"].replace("\"", "'"),
                                "description_chn": data["description_chn"].replace("\"", "'"),
                                "recommendation": data["recommendation"].replace("\"", "'"),
                                "recommendation_chn": data["recommendation_chn"].replace("\"", "'"),
                                "organization": data["organization"].replace("\"", "'"),
                                "title": data["title"].replace("\"", "'"),
                                "link": data["link"],
                                "update_date": data["update_date"]
                            }
                        }
                    }
                    edge_list.append(meta_edge)

    return node_list, edge_list


def handle_haplotype_rsID_edge(all_node_list):
    haplotype_node_list = []
    edge_list = []

    for node in all_node_list:
        if "haplotype" in node["label"]:
            haplotype_node_list.append(node)

    edge_set = []
    for node in haplotype_node_list:
        rsID_list = [x.strip() for x in node["property"]["mapped_rsID"].split(",")]
        if len(list(filter(lambda x: x != "", rsID_list))) == 0:
            continue

        for rsID in rsID_list:
            if "{}{}".format(node["property"]["variant_name"], rsID) in edge_set:
                continue
            else:
                edge_set.append("{}{}".format(node["property"]["variant_name"], rsID))

            relation_edge = {
                "start_node": {
                    "label": ["haplotype"],
                    "node_ID": "variant_name",
                    "property": {
                        "variant_name": node["property"]["variant_name"],
                        "type": "haplotype"
                    }
                },
                "end_node": {
                    "label": ["rsID"],
                    "node_ID": "variant_name",
                    "property": {
                        "variant_name": rsID,
                        "type": "rsID"
                    }
                },
                "edge": {
                    "label": "haplotype_rsID_related",
                    "property": {}
                }
            }
            edge_list.append(relation_edge)
    return edge_list


def handle_all_haplotype_node_edge():
    node_list = []
    edge_list = []

    edge_set = []
    for gene, hap_list in v_mapping.gene_hap_list_dict.items():
        for hap in hap_list:
            if "*" in hap:
                hap_name = "{}{}".format(gene, hap)
            else:
                hap_name = "{} {}".format(gene, hap)
            if hap == "":
                continue

            hap_mapping_dict = v_mapping.haplotype_mapping(hap_name)
            hap_frequency_dict = v_mapping.haplotype_frequency_mapping(hap_name)
            hap_function_dict = v_mapping.haplotype_functionality_mapping(hap_name)
            hap_mapping_dict.update({"frequency": hap_frequency_dict})
            hap_mapping_dict.update({"functionality": hap_function_dict})
            gene_list = [gene]
            chromosome_list = [v_mapping.gene_chromosome_dict.get(x, "") for x in gene_list]

            hap_node = {
                "label": ["variant", "haplotype"],
                "node_ID": "variant_name",
                "property": {
                    "variant_name": hap_name,
                    "display": hap_name,
                    "type": "haplotype",
                    "NC_change_code": hap_mapping_dict.get("NC", ""),
                    "NG_change_code": hap_mapping_dict.get("NG", ""),
                    "protein_change_code": hap_mapping_dict.get("protein", ""),
                    "nucleotide_change_code": hap_mapping_dict.get("nucleotide", ""),
                    "mapped_rsID": ",".join(hap_mapping_dict.get("rsID", [])),
                    "update_date": hap_mapping_dict.get("update_date", ""),
                    "frequency": str(hap_mapping_dict.get("frequency", "")),
                    "functionality": str(hap_mapping_dict.get("functionality", "")),
                    "is_reference": str(hap_mapping_dict.get("is_reference", ""))
                }
            }
            node_list.append(hap_node)

            gene_node_list = []
            for i in range(len(gene_list)):
                gene_node_list.append({
                    "label": ["gene"],
                    "node_ID": "gene_name",
                    "property": {
                        "gene_name": gene_list[i],
                        "display": gene_list[i],
                        "chromosome": chromosome_list[i],
                        "update_date": v_mapping.gene_update_date,
                        "link": v_mapping.gene_link_dict.get(gene_list[i], ""),
                        "is_VIP": v_mapping.gene_is_vip.get(gene_list[i], ""),
                        "has_variant_annotation": v_mapping.gene_has_variant_annotation.get(gene_list[i], ""),
                        "has_cpic_dosing_guideline": v_mapping.gene_has_cpic_dosing_guideline.get(gene_list[i], ""),
                        "chromosomal_start_GRCh38": v_mapping.gene_chromosomal_start_GRCh38.get(gene_list[i], ""),
                        "chromosomal_stop_GRCh38": v_mapping.gene_chromosomal_stop_GRCh38.get(gene_list[i], ""),
                        "refseq": v_mapping.gene_refseq_rna.get(gene_list[i], ""),
                        "OMIM": v_mapping.gene_omim.get(gene_list[i], "")
                    }
                })
            node_list.extend(gene_node_list)

            # variant to gene.
            for gene_node in gene_node_list:
                relation_edge = {
                    "start_node": edge_node(
                        hap_node,
                        remain_label_list=["variant"],
                        remain_property_list=["variant_name", "type"]
                    ),
                    "end_node": edge_node(
                        gene_node,
                        remain_label_list=["gene"],
                        remain_property_list=["gene_name"]
                    ),
                    "edge": {
                        "label": "mutation_at",
                        "property": {}
                    }
                }
                gene_variant_edge_list.append([
                    hap_node["property"]["display"],
                    gene_node["property"]["display"],
                    relation_edge
                ])

            for rsID in filter(lambda x: x != "", hap_mapping_dict.get("rsID", [])):
                if "{}{}".format(hap_name, rsID) in edge_set:
                    continue
                else:
                    edge_set.append("{}{}".format(hap_name, rsID))

                relation_edge = {
                    "start_node": {
                        "label": ["haplotype"],
                        "node_ID": "variant_name",
                        "property": {
                            "variant_name": hap_name,
                            "type": "haplotype"
                        }
                    },
                    "end_node": {
                        "label": ["rsID"],
                        "node_ID": "variant_name",
                        "property": {
                            "variant_name": rsID,
                            "type": "rsID"
                        }
                    },
                    "edge": {
                        "label": "haplotype_rsID_related",
                        "property": {}
                    }
                }
                edge_list.append(relation_edge)
    return node_list, edge_list

def handle_all_rsID_node():
    node_list = []
    for rsID in v_mapping.variant_link_dict.keys():
        mapping_dict = v_mapping.rsID_mapping(rsID)
        mapping_dict["update_date"] = v_mapping.rsID_update_date
        gene_list = v_mapping.variant_gene_dict.get(rsID, [])
        chromosome_list = [v_mapping.gene_chromosome_dict.get(x, "") for x in gene_list]

        variant_node = {
            "label": ["variant", "rsID"],
            "node_ID": "variant_name",
            "property": {
                "variant_name": rsID,
                "display": rsID,
                "type": "rsID",
                "NC_change_code": mapping_dict.get("NC", ""),
                "NG_change_code": mapping_dict.get("NG", ""),
                "protein_change_code": mapping_dict.get("protein", ""),
                "nucleotide_change_code": mapping_dict.get("nucleotide", ""),
                "mapped_rsID": ",".join(mapping_dict.get("rsID", [])),
                "update_date": mapping_dict.get("update_date", ""),
                "frequency": str(mapping_dict.get("frequency", "")),
                "functionality": str(mapping_dict.get("functionality", "")),
                "link": v_mapping.variant_link_dict.get(rsID, "")
            }
        }
        node_list.append(variant_node)

        gene_node_list = []
        for i in range(len(gene_list)):
            gene_node_list.append({
                "label": ["gene"],
                "node_ID": "gene_name",
                "property": {
                    "gene_name": gene_list[i],
                    "display": gene_list[i],
                    "chromosome": chromosome_list[i],
                    "update_date": v_mapping.gene_update_date,
                    "link": v_mapping.gene_link_dict.get(gene_list[i], ""),
                    "is_VIP": v_mapping.gene_is_vip.get(gene_list[i], ""),
                    "has_variant_annotation": v_mapping.gene_has_variant_annotation.get(gene_list[i], ""),
                    "has_cpic_dosing_guideline": v_mapping.gene_has_cpic_dosing_guideline.get(gene_list[i], ""),
                    "chromosomal_start_GRCh38": v_mapping.gene_chromosomal_start_GRCh38.get(gene_list[i], ""),
                    "chromosomal_stop_GRCh38": v_mapping.gene_chromosomal_stop_GRCh38.get(gene_list[i], ""),
                    "refseq": v_mapping.gene_refseq_rna.get(gene_list[i], ""),
                    "OMIM": v_mapping.gene_omim.get(gene_list[i], "")
                }
            })
        node_list.extend(gene_node_list)

        # variant to gene.
        for gene_node in gene_node_list:
            relation_edge = {
                "start_node": edge_node(
                    variant_node,
                    remain_label_list=["variant"],
                    remain_property_list=["variant_name", "type"]
                ),
                "end_node": edge_node(
                    gene_node,
                    remain_label_list=["gene"],
                    remain_property_list=["gene_name"]
                ),
                "edge": {
                    "label": "mutation_at",
                    "property": {}
                }
            }
            gene_variant_edge_list.append([
                variant_node["property"]["display"],
                gene_node["property"]["display"],
                relation_edge
            ])
    return node_list


def handle_all_diplotype_drug_edge():
    node_list = []
    edge_list = []

    ################### parse metabolizer drug dict ###############
    metabolizer_drug_dict = defaultdict(dict)
    df = pd.read_csv(
        "processed/phenotype_drug_relation.csv",
        encoding="utf-8",
        dtype=str
    ).fillna("")
    for pheno_gene, content in df.groupby(["phenotype_category", "gene"]):
        drug_dict = defaultdict(list)
        for drug, data in content.groupby("drug"):
            for i, r in data.iterrows():
                drug_dict[drug].append({
                    "data_source": "guideline_annotation",
                    "phenotype": r["phenotype"],
                    "phenotype_category": r["phenotype_category"],
                    "genotype": r["genotype"],
                    "implication": str(r["implication"]).replace("\"", "'"),
                    "implication_chn": translate_dict.get(r["implication"], "").replace("\"", "'"),
                    "description": str(r["description"]).replace("\"", "'"),
                    "description_chn": translate_dict.get(r["description"], "").replace("\"", "'"),
                    "recommendation": str(r["recommendation"]).replace("\"", "'"),
                    "recommendation_chn": translate_dict.get(r["recommendation"], "").replace("\"", "'"),
                    "organization": str(r["organization"]).replace("\"", "'"),
                    "title": str(r["title"]).replace("\"", "'"),
                    "link": r["link"],
                    "update_date": r["update_date"]
                })
        metabolizer_drug_dict["{} {}".format(pheno_gene[1], pheno_gene[0])] = drug_dict

    meta_edge_set = []
    for gene, diplotype_dict in v_mapping.gene_diplotype_dict.items():
        for dip, detail in diplotype_dict.items():
            hap1_name = "{}{}".format(gene, dip.split("/")[0])
            hap2_name = "{}{}".format(gene, dip.split("/")[1])
            dip_name = "{} {}".format(gene, dip)
            metabolizer = detail["phenotype"]
            dip_mapping_dict = v_mapping.diplotype_mapping(dip_name)
            dip_mapping_dict.update({"frequency": v_mapping.diplotype_frequency_mapping(dip_name)})

            diplotype_node = {
                "label": ["diplotype"],
                "node_ID": "diplotype_name",
                "property": {
                    "diplotype_name": dip_name.replace("\"", "'"),
                    "display": dip_name.replace("\"", "'"),
                    "phenotype": dip_mapping_dict.get("phenotype", ""),
                    "ehr_notation": dip_mapping_dict.get("ehr_notation", ""),
                    "activity_score": dip_mapping_dict.get("activity_score", ""),
                    "consultation": dip_mapping_dict.get("consultation", ""),
                    "update_date": dip_mapping_dict.get("update_date", ""),
                    "frequency": str(dip_mapping_dict.get("frequency", ""))
                }
            }
            node_list.append(diplotype_node)

            hap1_mapping_dict = v_mapping.haplotype_mapping(hap1_name)
            hap1_frequency_dict = v_mapping.haplotype_frequency_mapping(hap1_name)
            hap1_function_dict = v_mapping.haplotype_functionality_mapping(hap1_name)
            hap1_mapping_dict.update({"frequency": hap1_frequency_dict})
            hap1_mapping_dict.update({"functionality": hap1_function_dict})

            hap2_mapping_dict = v_mapping.haplotype_mapping(hap2_name)
            hap2_frequency_dict = v_mapping.haplotype_frequency_mapping(hap2_name)
            hap2_function_dict = v_mapping.haplotype_functionality_mapping(hap2_name)
            hap2_mapping_dict.update({"frequency": hap2_frequency_dict})
            hap2_mapping_dict.update({"functionality": hap2_function_dict})

            hap1_node = {
                "label": ["variant", "haplotype"],
                "node_ID": "variant_name",
                "property": {
                    "variant_name": hap1_name,
                    "display": hap1_name,
                    "type": "haplotype",
                    "NC_change_code": hap1_mapping_dict.get("NC", ""),
                    "NG_change_code": hap1_mapping_dict.get("NG", ""),
                    "protein_change_code": hap1_mapping_dict.get("protein", ""),
                    "nucleotide_change_code": hap1_mapping_dict.get("nucleotide", ""),
                    "mapped_rsID": ",".join(hap1_mapping_dict.get("rsID", [])),
                    "update_date": hap1_mapping_dict.get("update_date", ""),
                    "frequency": str(hap1_mapping_dict.get("frequency", "")),
                    "functionality": str(hap1_mapping_dict.get("functionality", "")),
                    "is_reference": str(hap1_mapping_dict.get("is_reference", ""))
                }
            }
            node_list.append(hap1_node)

            hap2_node = {
                "label": ["variant", "haplotype"],
                "node_ID": "variant_name",
                "property": {
                    "variant_name": hap2_name,
                    "display": hap2_name,
                    "type": "haplotype",
                    "NC_change_code": hap2_mapping_dict.get("NC", ""),
                    "NG_change_code": hap2_mapping_dict.get("NG", ""),
                    "protein_change_code": hap2_mapping_dict.get("protein", ""),
                    "nucleotide_change_code": hap2_mapping_dict.get("nucleotide", ""),
                    "mapped_rsID": ",".join(hap2_mapping_dict.get("rsID", [])),
                    "update_date": hap2_mapping_dict.get("update_date", ""),
                    "frequency": str(hap2_mapping_dict.get("frequency", "")),
                    "functionality": str(hap2_mapping_dict.get("functionality", "")),
                    "is_reference": str(hap2_mapping_dict.get("is_reference", ""))
                }
            }
            node_list.append(hap2_node)

            # diplotype to haplotype.
            hap1_edge = {
                "start_node": edge_node(
                    diplotype_node,
                    remain_label_list=["diplotype"],
                    remain_property_list=["diplotype_name"]
                ),
                "end_node": edge_node(
                    hap1_node,
                    remain_label_list=["haplotype"],
                    remain_property_list=["variant_name"]
                ),
                "edge": {
                    "label": "diplotype_consist_of",
                    "property": {}
                }
            }
            edge_list.append(hap1_edge)

            # diplotype to haplotype.
            hap2_edge = {
                "start_node": edge_node(
                    diplotype_node,
                    remain_label_list=["diplotype"],
                    remain_property_list=["diplotype_name"]
                ),
                "end_node": edge_node(
                    hap2_node,
                    remain_label_list=["haplotype"],
                    remain_property_list=["variant_name"]
                ),
                "edge": {
                    "label": "diplotype_consist_of",
                    "property": {}
                }
            }
            edge_list.append(hap2_edge)

            if metabolizer in metabolizer_drug_dict.keys():
                # Add diplotype to drug relation
                for drug in metabolizer_drug_dict[metabolizer].keys():
                    if "{}{}{}".format(dip_name, metabolizer, drug) in meta_edge_set:
                        continue
                    else:
                        meta_edge_set.append("{}{}{}".format(dip_name, metabolizer, drug))

                    chemical_node = {
                        "label": ["chemical"],
                        "node_ID": "chemical_name",
                        "property": {
                            "chemical_name": drug,
                            "display": drug,
                            "meshID": ""
                        }
                    }

                    # add ATC node
                    if drug in v_mapping.ATC_dict.keys():
                        for key in v_mapping.ATC_dict[drug].keys():
                            chemical_node["property"][key] = v_mapping.ATC_dict[drug][key]

                    node_list.append(chemical_node)

                    for data in metabolizer_drug_dict[metabolizer][drug]:
                        # diplotype to chemical
                        meta_edge = {
                            "start_node": edge_node(
                                diplotype_node,
                                remain_label_list=["diplotype"],
                                remain_property_list=["diplotype_name"]
                            ),
                            "end_node": edge_node(
                                chemical_node,
                                remain_label_list=["chemical"],
                                remain_property_list=["chemical_name"]
                            ),
                            "edge": {
                                "label": "diplotype_metabolizer",
                                "property": {
                                    "data_source": data["data_source"],
                                    "phenotype": data["phenotype"],
                                    "phenotype_category": data["phenotype_category"],
                                    "genotype": data["genotype"].replace("\"", "'"),
                                    "implication": data["implication"].replace("\"", "'"),
                                    "implication_chn": data["implication_chn"].replace("\"", "'"),
                                    "description": data["description"].replace("\"", "'"),
                                    "description_chn": data["description_chn"].replace("\"", "'"),
                                    "recommendation": data["recommendation"].replace("\"", "'"),
                                    "recommendation_chn": data["recommendation_chn"].replace("\"", "'"),
                                    "organization": data["organization"].replace("\"", "'"),
                                    "title": data["title"].replace("\"", "'"),
                                    "link": data["link"],
                                    "update_date": data["update_date"]
                                }
                            }
                        }
                        edge_list.append(meta_edge)
    return node_list, edge_list


def handle_no_metabolizer_guideline():
    node_list = []
    edge_list = []
    df = pd.read_csv(
        "processed/diplotype_record_guideline.csv",
        encoding="utf-8",
        dtype=str
    ).fillna("")

    for index, row in df.iterrows():
        dip_name = row["diplotype"]
        gene, dip = dip_name.split(" ")
        hap1_name = "{}{}".format(gene, dip.split("/")[0])
        hap2_name = "{}{}".format(gene, dip.split("/")[1])
        dip_mapping_dict = v_mapping.diplotype_mapping(dip_name)
        dip_mapping_dict.update({"frequency": v_mapping.diplotype_frequency_mapping(dip_name)})

        diplotype_node = {
            "label": ["diplotype"],
            "node_ID": "diplotype_name",
            "property": {
                "diplotype_name": dip_name.replace("\"", "'"),
                "display": dip_name.replace("\"", "'"),
                "phenotype": dip_mapping_dict.get("phenotype", ""),
                "ehr_notation": dip_mapping_dict.get("ehr_notation", ""),
                "activity_score": dip_mapping_dict.get("activity_score", ""),
                "consultation": dip_mapping_dict.get("consultation", ""),
                "update_date": dip_mapping_dict.get("update_date", ""),
                "frequency": str(dip_mapping_dict.get("frequency", ""))
            }
        }
        node_list.append(diplotype_node)

        hap1_mapping_dict = v_mapping.haplotype_mapping(hap1_name)
        hap1_frequency_dict = v_mapping.haplotype_frequency_mapping(hap1_name)
        hap1_function_dict = v_mapping.haplotype_functionality_mapping(hap1_name)
        hap1_mapping_dict.update({"frequency": hap1_frequency_dict})
        hap1_mapping_dict.update({"functionality": hap1_function_dict})

        hap2_mapping_dict = v_mapping.haplotype_mapping(hap2_name)
        hap2_frequency_dict = v_mapping.haplotype_frequency_mapping(hap2_name)
        hap2_function_dict = v_mapping.haplotype_functionality_mapping(hap2_name)
        hap2_mapping_dict.update({"frequency": hap2_frequency_dict})
        hap2_mapping_dict.update({"functionality": hap2_function_dict})

        hap1_node = {
            "label": ["variant", "haplotype"],
            "node_ID": "variant_name",
            "property": {
                "variant_name": hap1_name,
                "display": hap1_name,
                "type": "haplotype",
                "NC_change_code": hap1_mapping_dict.get("NC", ""),
                "NG_change_code": hap1_mapping_dict.get("NG", ""),
                "protein_change_code": hap1_mapping_dict.get("protein", ""),
                "nucleotide_change_code": hap1_mapping_dict.get("nucleotide", ""),
                "mapped_rsID": ",".join(hap1_mapping_dict.get("rsID", [])),
                "update_date": hap1_mapping_dict.get("update_date", ""),
                "frequency": str(hap1_mapping_dict.get("frequency", "")),
                "functionality": str(hap1_mapping_dict.get("functionality", "")),
                "is_reference": str(hap1_mapping_dict.get("is_reference", ""))
            }
        }
        node_list.append(hap1_node)

        hap2_node = {
            "label": ["variant", "haplotype"],
            "node_ID": "variant_name",
            "property": {
                "variant_name": hap2_name,
                "display": hap2_name,
                "type": "haplotype",
                "NC_change_code": hap2_mapping_dict.get("NC", ""),
                "NG_change_code": hap2_mapping_dict.get("NG", ""),
                "protein_change_code": hap2_mapping_dict.get("protein", ""),
                "nucleotide_change_code": hap2_mapping_dict.get("nucleotide", ""),
                "mapped_rsID": ",".join(hap2_mapping_dict.get("rsID", [])),
                "update_date": hap2_mapping_dict.get("update_date", ""),
                "frequency": str(hap2_mapping_dict.get("frequency", "")),
                "functionality": str(hap2_mapping_dict.get("functionality", "")),
                "is_reference": str(hap2_mapping_dict.get("is_reference", ""))
            }
        }
        node_list.append(hap2_node)

        # diplotype to haplotype.
        hap1_edge = {
            "start_node": edge_node(
                diplotype_node,
                remain_label_list=["diplotype"],
                remain_property_list=["diplotype_name"]
            ),
            "end_node": edge_node(
                hap1_node,
                remain_label_list=["haplotype"],
                remain_property_list=["variant_name"]
            ),
            "edge": {
                "label": "diplotype_consist_of",
                "property": {}
            }
        }
        edge_list.append(hap1_edge)

        # diplotype to haplotype.
        hap2_edge = {
            "start_node": edge_node(
                diplotype_node,
                remain_label_list=["diplotype"],
                remain_property_list=["diplotype_name"]
            ),
            "end_node": edge_node(
                hap2_node,
                remain_label_list=["haplotype"],
                remain_property_list=["variant_name"]
            ),
            "edge": {
                "label": "diplotype_consist_of",
                "property": {}
            }
        }
        edge_list.append(hap2_edge)

        chemical_node = {
            "label": ["chemical"],
            "node_ID": "chemical_name",
            "property": {
                "chemical_name": row["drug"],
                "display": row["drug"],
                "meshID": ""
            }
        }

        # add ATC node
        if row["drug"] in v_mapping.ATC_dict.keys():
            for key in v_mapping.ATC_dict[row["drug"]].keys():
                chemical_node["property"][key] = v_mapping.ATC_dict[row["drug"]][key]

        node_list.append(chemical_node)

        # diplotype to chemical
        meta_edge = {
            "start_node": edge_node(
                diplotype_node,
                remain_label_list=["diplotype"],
                remain_property_list=["diplotype_name"]
            ),
            "end_node": edge_node(
                chemical_node,
                remain_label_list=["chemical"],
                remain_property_list=["chemical_name"]
            ),
            "edge": {
                "label": "variant_guideline",
                "property": {
                    "data_source": row["data_source"],
                    "phenotype": row["phenotype"],
                    "genotype": row["genotype"].replace("\"", "'"),
                    "genotype_chn": translate_dict.get(row["genotype"], "").replace("\"", "'"),
                    "implication": row["implication"].replace("\"", "'"),
                    "implication_chn": translate_dict.get(row["implication"], "").replace("\"", "'"),
                    "description": row["description"].replace("\"", "'"),
                    "recommendation": row["recommendation"].replace("\"", "'"),
                    "recommendation_chn": translate_dict.get(row["recommendation"], "").replace("\"", "'"),
                    "organization": row["organization"].replace("\"", "'"),
                    "title": row["title"].replace("\"", "'"),
                    "link": row["link"],
                    "update_date": row["update_date"]
                }
            }
        }
        edge_list.append(meta_edge)

    df = pd.read_csv(
        "processed/haplotype_record_guideline.csv",
        encoding="utf-8",
        dtype=str
    ).fillna("")

    for index, row in df.iterrows():
        hap_name = row["haplotype"]
        hap_mapping_dict = v_mapping.haplotype_mapping(hap_name)
        hap_frequency_dict = v_mapping.haplotype_frequency_mapping(hap_name)
        hap_function_dict = v_mapping.haplotype_functionality_mapping(hap_name)
        hap_mapping_dict.update({"frequency": hap_frequency_dict})
        hap_mapping_dict.update({"functionality": hap_function_dict})

        hap_node = {
            "label": ["variant", "haplotype"],
            "node_ID": "variant_name",
            "property": {
                "variant_name": hap_name,
                "display": hap_name,
                "type": "haplotype",
                "NC_change_code": hap_mapping_dict.get("NC", ""),
                "NG_change_code": hap_mapping_dict.get("NG", ""),
                "protein_change_code": hap_mapping_dict.get("protein", ""),
                "nucleotide_change_code": hap_mapping_dict.get("nucleotide", ""),
                "mapped_rsID": ",".join(hap_mapping_dict.get("rsID", [])),
                "update_date": hap_mapping_dict.get("update_date", ""),
                "frequency": str(hap_mapping_dict.get("frequency", "")),
                "functionality": str(hap_mapping_dict.get("functionality", "")),
                "is_reference": str(hap_mapping_dict.get("is_reference", ""))
            }
        }
        node_list.append(hap_node)

        chemical_node = {
            "label": ["chemical"],
            "node_ID": "chemical_name",
            "property": {
                "chemical_name": row["drug"],
                "display": row["drug"],
                "meshID": ""
            }
        }

        # add ATC node
        if row["drug"] in v_mapping.ATC_dict.keys():
            for key in v_mapping.ATC_dict[row["drug"]].keys():
                chemical_node["property"][key] = v_mapping.ATC_dict[row["drug"]][key]

        node_list.append(chemical_node)

        # haplotype to chemical
        meta_edge = {
            "start_node": edge_node(
                hap_node,
                remain_label_list=["variant"],
                remain_property_list=["variant_name"]
            ),
            "end_node": edge_node(
                chemical_node,
                remain_label_list=["chemical"],
                remain_property_list=["chemical_name"]
            ),
            "edge": {
                "label": "variant_guideline",
                "property": {
                    "data_source": row["data_source"],
                    "phenotype": row["phenotype"],
                    "genotype": row["genotype"].replace("\"", "'"),
                    "genotype_chn": translate_dict.get(row["genotype"], "").replace("\"", "'"),
                    "implication": row["implication"].replace("\"", "'"),
                    "implication_chn": translate_dict.get(row["implication"], "").replace("\"", "'"),
                    "description": row["description"].replace("\"", "'"),
                    "recommendation": row["recommendation"].replace("\"", "'"),
                    "recommendation_chn": translate_dict.get(row["recommendation"], "").replace("\"", "'"),
                    "organization": row["organization"].replace("\"", "'"),
                    "title": row["title"].replace("\"", "'"),
                    "link": row["link"],
                    "update_date": row["update_date"]
                }
            }
        }
        edge_list.append(meta_edge)


    return node_list, edge_list


def handle_cpic_guideline():
    df = pd.read_csv(
        "processed/cpic_gene_drug.csv",
        encoding="utf-8",
        dtype=str
    ).fillna("")
    node_list = []
    edge_list = []

    for index, row in df.iterrows():
        gene = row["Gene"]
        drug = row["Drug"]
        guideline_link = row["Guideline"]
        cpic_level = row["CPIC Level"]
        cpic_level_status = row["CPIC Level Status"]
        pharmGKB_evidence_level = row["PharmGKB Level of Evidence"]
        fda_PGx_label = row["PGx on FDA Label"]
        pmid = row["CPIC Publications (PMID)"]
        update_date = row["update_date"]
        pmid_link = row["PMID_link"]

        ################################ add node ###########################
        gene_node = {
            "label": ["gene"],
            "node_ID": "gene_name",
            "property": {
                "gene_name": gene,
                "display": gene,
                "chromosome": v_mapping.gene_chromosome_dict.get(gene, ""),
                "update_date": v_mapping.gene_update_date,
                "link": v_mapping.gene_link_dict.get(gene, ""),
                "is_VIP": v_mapping.gene_is_vip.get(gene, ""),
                "has_variant_annotation": v_mapping.gene_has_variant_annotation.get(gene, ""),
                "has_cpic_dosing_guideline": v_mapping.gene_has_cpic_dosing_guideline.get(gene, ""),
                "chromosomal_start_GRCh38": v_mapping.gene_chromosomal_start_GRCh38.get(gene, ""),
                "chromosomal_stop_GRCh38": v_mapping.gene_chromosomal_stop_GRCh38.get(gene, ""),
                "refseq": v_mapping.gene_refseq_rna.get(gene, ""),
                "OMIM": v_mapping.gene_omim.get(gene, "")
            }
        }
        node_list.append(gene_node)

        chemical_node = {
            "label": ["chemical"],
            "node_ID": "chemical_name",
            "property": {
                "chemical_name": drug,
                "display": drug,
                # TODO map meshID
                "meshID": "",
                "link": v_mapping.chemical_link_dict.get(drug, "")
            }
        }

        # add ATC node
        if drug in v_mapping.ATC_dict.keys():
            for key in v_mapping.ATC_dict[drug].keys():
                chemical_node["property"][key] = v_mapping.ATC_dict[drug][key]

        node_list.append(chemical_node)

        ################################## add edge ###################################
        # variant to chemical.
        relation_edge = {
            "start_node": edge_node(
                gene_node,
                remain_label_list=["gene"],
                remain_property_list=["gene_name"]
            ),
            "end_node": edge_node(
                chemical_node,
                remain_label_list=["chemical"],
                remain_property_list=["chemical_name"]
            ),
            "edge": {
                "label": "cpic_guideline",
                "property": {
                    "data_source": "cpic_guideline",
                    "guideline_link": guideline_link,
                    "CPIC_level": cpic_level,
                    "CPIC_level_status": cpic_level_status,
                    "PGKB_evidence_level": pharmGKB_evidence_level,
                    "FDA_PGx_label": fda_PGx_label,
                    "update_date": update_date,
                    "PMID": pmid,
                    "PMID_link": pmid_link
                }
            }
        }
        edge_list.append(relation_edge)
    return node_list, edge_list


def node_deduplicate(node_list):
    """
    chemical, diplotype, gene, haplotype, rsID
    """
    new_node_list = []
    set_dict = defaultdict(list)

    for node in node_list:
        if "chemical" in node["label"] and node["property"]["chemical_name"] not in set_dict["chemical"]:
            new_node_list.append(node)
            set_dict["chemical"].append(node["property"]["chemical_name"])

        if "diplotype" in node["label"] and node["property"]["diplotype_name"] not in set_dict["diplotype"]:
            new_node_list.append(node)
            set_dict["diplotype"].append(node["property"]["diplotype_name"])

        if "gene" in node["label"] and node["property"]["gene_name"] not in set_dict["gene"]:
            new_node_list.append(node)
            set_dict["gene"].append(node["property"]["gene_name"])

        if "haplotype" in node["label"] and node["property"]["variant_name"] not in set_dict["haplotype"]:
            new_node_list.append(node)
            set_dict["haplotype"].append(node["property"]["variant_name"])

        if "rsID" in node["label"] and node["property"]["variant_name"] not in set_dict["rsID"]:
            new_node_list.append(node)
            set_dict["rsID"].append(node["property"]["variant_name"])
    return new_node_list


def edge_deduplicate(edge_list):
    new_edge_list = []
    edge_set = []
    for edge in edge_list:
        if edge["edge"]["label"] in ["clinical_annotation", "guideline_annotation",
                                     "drug_label", "research_annotation", "cpic_guideline"]:
            new_edge_list.append(edge)

        else:
            edge_label = edge["edge"]["label"]

            start_node_name = "{}{}".format(edge["start_node"]["node_ID"],
                                            edge["start_node"]["property"][edge["start_node"]["node_ID"]])

            end_node_name = "{}{}".format(edge["end_node"]["node_ID"],
                                          edge["end_node"]["property"][edge["end_node"]["node_ID"]])

            if "{}{}{}".format(edge_label, start_node_name, end_node_name) not in edge_set:
                new_edge_list.append(edge)
                edge_set.append("{}{}{}".format(edge_label, start_node_name, end_node_name))
            else:
                continue

    return new_edge_list


def gen_drug_chemical_node_edge(node_list):
    df_drug_chemical = pd.read_csv("processed/nmpa_drug_chemical.csv",
                                   encoding="utf-8", dtype=str).fillna("")
    drug_node_list = []
    drug_edge_list = []
    drug_set = []

    for node in node_list:
        if "chemical" in node["label"]:
            chemical_name = node["property"]["chemical_name"]
            df_filter = df_drug_chemical[df_drug_chemical["chemical"] == chemical_name]

            for index, row in df_filter.iterrows():
                if row["license"] in drug_set or row["license"] == "":
                    continue
                else:
                    drug_set.append(row["license"])

                drug_node = {
                    "label": ["nmpa_drug"],
                    "node_ID": "license",
                    "property": {
                        "license": row["license"],
                        "drug_name_chn": row["chn_name"],
                        "drug_name_eng": row["eng_name"],
                        "drug_code": row["code"],
                        "source": row["source"],
                        "country": row["country"],
                        "company": row["company"],
                        "type": row["drug_type"],
                        "dose": row["dose"],
                        "display": row["chn_name"],
                        "chn_business_name": row["chn_business_name"],
                        "eng_business_name": row["eng_business_name"],
                        "description": row["description"]
                    }
                }
                drug_node_list.append(drug_node)

                drug_edge = {
                    "start_node": {
                        "label": ["nmpa_drug"],
                        "node_ID": "license",
                        "property": {
                            "license": row["license"],
                        }
                    },
                    "end_node": {
                        "label": ["chemical"],
                        "node_ID": "chemical_name",
                        "property": {
                            "chemical_name": row["chemical"],
                        }
                    },
                    "edge": {
                        "label": "drug_consist_of",
                        "property": {}
                    }
                }
                drug_edge_list.append(drug_edge)

    return drug_node_list, drug_edge_list


def step3_gen_node_edge():
    node_list = []
    edge_list = []

    clinical_node_list, clinical_edge_list = handle_clinical_csv()
    variant_label_node_list, variant_label_edge_list = handle_variant_drug_label_csv()
    gene_label_node_list, gene_label_edge_list = handle_gene_drug_label_csv()
    guideline_node_list, guideline_edge_list = handle_guideline_csv()
    diplo_drug_node_list, diplo_drug_edge_list = handle_diplotype_drug_csv()
    research_node_list, research_edge_list = handle_research_csv()

    node_list.extend(clinical_node_list)
    node_list.extend(variant_label_node_list)
    node_list.extend(gene_label_node_list)
    node_list.extend(guideline_node_list)
    node_list.extend(diplo_drug_node_list)
    node_list.extend(research_node_list)

    edge_list.extend(clinical_edge_list)
    edge_list.extend(variant_label_edge_list)
    edge_list.extend(gene_label_edge_list)
    edge_list.extend(guideline_edge_list)
    edge_list.extend(diplo_drug_edge_list)
    edge_list.extend(research_edge_list)

    # handle all diplotype node
    all_dip_node_list, all_dip_edge_list = handle_all_diplotype_drug_edge()

    # handle all haplotype node
    all_hap_node_list, all_hap_edge_list = handle_all_haplotype_node_edge()

    # handle all rsID node
    all_rsID_node_list = handle_all_rsID_node()

    node_list.extend(all_dip_node_list)
    node_list.extend(all_hap_node_list)
    node_list.extend(all_rsID_node_list)
    edge_list.extend(all_dip_edge_list)
    edge_list.extend(all_hap_edge_list)

    # handle haplotype to rsID edge data.
    edge_list.extend(handle_haplotype_rsID_edge(node_list))

    # deduplicate variant->gene edge data, and add to edge list
    judge_set = set()
    for x in gene_variant_edge_list:
        if x[0] + x[1] not in list(judge_set):
            judge_set.add(x[0] + x[1])
            edge_list.append(x[2])

    cpic_node_list, cpic_edge_list = handle_cpic_guideline()
    node_list.extend(cpic_node_list)
    edge_list.extend(cpic_edge_list)

    no_meta_node_list, no_meta_edge_list = handle_no_metabolizer_guideline()
    node_list.extend(no_meta_node_list)
    edge_list.extend(no_meta_edge_list)

    print(len(node_list))
    print(len(edge_list))

    # deduplicate node and edge
    node_list = node_deduplicate(node_list)
    edge_list = edge_deduplicate(edge_list)

    # add drug node and edge
    drug_node_list, drug_edge_list = gen_drug_chemical_node_edge(node_list)
    node_list.extend(drug_node_list)
    edge_list.extend(drug_edge_list)

    print(len(node_list))
    print(len(edge_list))

    # back up node and edge file with datetime string suffix
    folder = "json"
    bak_folder = os.path.join("bak", folder)
    if not os.path.isdir(folder):
        os.mkdir(folder)

    if not os.path.isdir(bak_folder):
        os.mkdir(bak_folder)

    date_str = datetime.now().strftime("%Y-%m-%d")
    for file in os.listdir(folder):
        new_file = "{}_{}.json".format(file.split(".")[0], date_str)
        shutil.move(os.path.join(folder, file), os.path.join(bak_folder, new_file))

    with open("json/nodes.json", "w") as f:
        json.dump(node_list, f)

    with open("json/edges.json", "w") as f:
        json.dump(edge_list, f)


def add_extra_data():
    # no_meta_node_list, no_meta_edge_list = handle_no_metabolizer_guideline()
    hap_node_list, hap_edge_list = handle_all_haplotype_node_edge()
    with open("json/nodes_extra.json", "w") as f:
        json.dump(hap_node_list, f)

    # with open("json/edges_extra.json", "w") as f:
    #     json.dump(no_meta_edge_list, f)


if __name__ == "__main__":
    add_extra_data()
    # step3_gen_node_edge()
    # handle_all_haplotype_node_edge()
