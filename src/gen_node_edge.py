#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import json
from copy import deepcopy
from util.variant_mapping_util import variantMappingUtil

v_mapping = variantMappingUtil()

def check_type(variant_name):
    if "rs" in variant_name:
        return "rsID"
    else:
        return "haplotype"

def edge_node(node, del_label_list, del_property_list):
    new_node = dict()
    new_node["node_ID"] = node["node_ID"]
    property_dict = deepcopy(node["property"])
    for x in del_property_list:
        if x in property_dict.keys():
            del property_dict[x]
    new_node["property"] = property_dict
    label_list = deepcopy(node["label"])
    for x in del_label_list:
        if x in label_list:
            label_list.remove(x)
    new_node["label"] = label_list
    return new_node

gene_variant_edge_list = []

def handle_clinical_csv():
    node_list = []
    edge_list = []

    df = pd.read_csv(
        "../processed/clinical_drug_variant_annotation.csv",
        encoding="utf-8"
    ).fillna("").astype(str)

    for index, row in df.iterrows():
        ########################### Add node ######################
        variant_type = check_type(row["variant"])
        if variant_type == "rsID":
            mapping_dict = v_mapping.rsID_mapping(row["variant"])
            gene_list = v_mapping.variant_gene_dict.get(row["variant"], [])
            chromosome_list = [v_mapping.gene_chromosome_dict.get(x, "") for x in gene_list]

        else:
            mapping_dict = v_mapping.haplotype_mapping(row["variant"])
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
                    "chromosome": chromosome_list[i]
                }
            })
        node_list.extend(gene_node_list)

        variant_node = {
            "label": ["variant"],
            "node_ID": "variant_name",
            "property": {
                "variant_name": row["variant"],
                "display": row["variant"],
                "type": variant_type,
                "NC_change_code": mapping_dict.get("NC", ""),
                "NG_change_code": mapping_dict.get("NG", ""),
                "protein_change_code": mapping_dict.get("protein", ""),
                "nucleotide_change_code": mapping_dict.get("nucleotide", ""),
                "mapped_rsID": ",".join(mapping_dict.get("rsID", []))
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
        node_list.append(chemical_node)

        ######################## Add edge #######################
        # variant to gene.
        for gene_node in gene_node_list:
            relation_edge = {
                "start_node": edge_node(
                    variant_node,
                    del_label_list=[],
                    del_property_list=["display", "NC_change_code", "NG_change_code",
                                       "protein_change_code", "nucleotide_change_code",
                                       "mapped_rsID"]
                ),
                "end_node": edge_node(
                    gene_node,
                    del_label_list=[],
                    del_property_list=["display", "chromosome"]
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
                del_label_list=[],
                del_property_list=["display", "NC_change_code", "NG_change_code",
                                   "protein_change_code", "nucleotide_change_code",
                                   "mapped_rsID"]
            ),
            "end_node": edge_node(
                chemical_node,
                del_label_list=[],
                del_property_list=["display", "meshID"]
            ),
            "edge": {
                "label": "clinical_annotation",
                "property": {
                    "data_source": "clinical_annotation",
                    "evidence": row["evidence"],
                    "phenotype": row["phenotype"],
                    "guideline_institute": "",
                    "guideline_name": "",
                    "guideline_term": "",
                    "testing_level": ""
                }
            }
        }
        edge_list.append(relation_edge)
    return node_list, edge_list


def handle_drug_label_csv():
    node_list = []
    edge_list = []

    df = pd.read_csv(
        "../processed/fda_drug_variant_label.csv",
        encoding="utf-8"
    ).fillna("").astype(str)

    for index, row in df.iterrows():
        ########################### Add node ######################
        variant_type = check_type(row["variant"])
        if variant_type == "rsID":
            mapping_dict = v_mapping.rsID_mapping(row["variant"])
            gene_list = v_mapping.variant_gene_dict.get(row["variant"], [])
            chromosome_list = [v_mapping.gene_chromosome_dict.get(x, "") for x in gene_list]

        else:
            mapping_dict = v_mapping.haplotype_mapping(row["variant"])
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
                    "chromosome": chromosome_list[i]
                }
            })
        node_list.extend(gene_node_list)

        variant_node = {
            "label": ["variant"],
            "node_ID": "variant_name",
            "property": {
                "variant_name": row["variant"],
                "display": row["variant"],
                "type": variant_type,
                "NC_change_code": mapping_dict.get("NC", ""),
                "NG_change_code": mapping_dict.get("NG", ""),
                "protein_change_code": mapping_dict.get("protein", ""),
                "nucleotide_change_code": mapping_dict.get("nucleotide", ""),
                "mapped_rsID": ",".join(mapping_dict.get("rsID", []))
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
        node_list.append(chemical_node)

        ######################## Add edge #######################
        # variant to gene.
        for gene_node in gene_node_list:
            relation_edge = {
                "start_node": edge_node(
                    variant_node,
                    del_label_list=[],
                    del_property_list=["display", "NC_change_code", "NG_change_code",
                                       "protein_change_code", "nucleotide_change_code",
                                       "mapped_rsID"]
                ),
                "end_node": edge_node(
                    gene_node,
                    del_label_list=[],
                    del_property_list=["display", "chromosome"]
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
                del_label_list=[],
                del_property_list=["display", "NC_change_code", "NG_change_code",
                                   "protein_change_code", "nucleotide_change_code",
                                   "mapped_rsID"]
            ),
            "end_node": edge_node(
                chemical_node,
                del_label_list=[],
                del_property_list=["display", "meshID"]
            ),
            "edge": {
                "label": "fda_drug_label",
                "property": {
                    "data_source": "fda_drug_label",
                    "evidence": "",
                    "phenotype": "",
                    "guideline_institute": "",
                    "guideline_name": "",
                    "guideline_term": "",
                    "testing_level": row["label"]
                }
            }
        }
        edge_list.append(relation_edge)
    return node_list, edge_list


def handle_guideline_csv():
    node_list = []
    edge_list = []

    df = pd.read_csv(
        "../processed/guideline_drug_variant_annotation.csv",
        encoding="utf-8"
    ).fillna("").astype(str)

    for index, row in df.iterrows():
        ########################### Add node ######################
        variant_type = check_type(row["haplotype"])
        if variant_type == "rsID":
            mapping_dict = v_mapping.rsID_mapping(row["haplotype"])
            gene_list = v_mapping.variant_gene_dict.get(row["haplotype"], [])
            chromosome_list = [v_mapping.gene_chromosome_dict.get(x, "") for x in gene_list]

        else:
            mapping_dict = v_mapping.haplotype_mapping(row["haplotype"])
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
                    "chromosome": chromosome_list[i]
                }
            })
        node_list.extend(gene_node_list)

        variant_node = {
            "label": ["variant"],
            "node_ID": "variant_name",
            "property": {
                "variant_name": row["haplotype"],
                "display": row["haplotype"],
                "type": variant_type,
                "NC_change_code": mapping_dict.get("NC", ""),
                "NG_change_code": mapping_dict.get("NG", ""),
                "protein_change_code": mapping_dict.get("protein", ""),
                "nucleotide_change_code": mapping_dict.get("nucleotide", ""),
                "mapped_rsID": ",".join(mapping_dict.get("rsID", []))
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
        node_list.append(chemical_node)

        ######################## Add edge #######################
        # variant to gene.
        for gene_node in gene_node_list:
            relation_edge = {
                "start_node": edge_node(
                    variant_node,
                    del_label_list=[],
                    del_property_list=["display", "NC_change_code", "NG_change_code",
                                       "protein_change_code", "nucleotide_change_code",
                                       "mapped_rsID"]
                ),
                "end_node": edge_node(
                    gene_node,
                    del_label_list=[],
                    del_property_list=["display", "chromosome"]
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
                del_label_list=[],
                del_property_list=["display", "NC_change_code", "NG_change_code",
                                   "protein_change_code", "nucleotide_change_code",
                                   "mapped_rsID"]
            ),
            "end_node": edge_node(
                chemical_node,
                del_label_list=[],
                del_property_list=["display", "meshID"]
            ),
            "edge": {
                "label": "guideline_annotation",
                "property": {
                    "data_source": "guideline_annotation",
                    "evidence": "",
                    "phenotype": "",
                    "guideline_institute": row["guideline_institute"],
                    "guideline_name": row["guideline_name"],
                    "guideline_term": row["term"],
                    "testing_level": ""
                }
            }
        }
        edge_list.append(relation_edge)
    return node_list, edge_list


def handle_research_csv():
    node_list = []
    edge_list = []

    df = pd.read_csv(
        "../processed/research_drug_variant_annotation.csv",
        encoding="utf-8"
    ).fillna("").astype(str)

    for index, row in df.iterrows():
        ########################### Add node ######################
        variant_type = check_type(row["variant"])
        if variant_type == "rsID":
            mapping_dict = v_mapping.rsID_mapping(row["variant"])
            gene_list = v_mapping.variant_gene_dict.get(row["variant"], [])
            chromosome_list = [v_mapping.gene_chromosome_dict.get(x, "") for x in gene_list]

        else:
            mapping_dict = v_mapping.haplotype_mapping(row["variant"])
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
                    "chromosome": chromosome_list[i]
                }
            })
        node_list.extend(gene_node_list)

        variant_node = {
            "label": ["variant"],
            "node_ID": "variant_name",
            "property": {
                "variant_name": row["variant"],
                "display": row["variant"],
                "type": variant_type,
                "NC_change_code": mapping_dict.get("NC", ""),
                "NG_change_code": mapping_dict.get("NG", ""),
                "protein_change_code": mapping_dict.get("protein", ""),
                "nucleotide_change_code": mapping_dict.get("nucleotide", ""),
                "mapped_rsID": ",".join(mapping_dict.get("rsID", []))
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
        node_list.append(chemical_node)

        ######################## Add edge #######################
        # variant to gene.
        for gene_node in gene_node_list:
            relation_edge = {
                "start_node": edge_node(
                    variant_node,
                    del_label_list=[],
                    del_property_list=["display", "NC_change_code", "NG_change_code",
                                       "protein_change_code", "nucleotide_change_code",
                                       "mapped_rsID"]
                ),
                "end_node": edge_node(
                    gene_node,
                    del_label_list=[],
                    del_property_list=["display", "chromosome"]
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
                del_label_list=[],
                del_property_list=["display", "NC_change_code", "NG_change_code",
                                   "protein_change_code", "nucleotide_change_code",
                                   "mapped_rsID"]
            ),
            "end_node": edge_node(
                chemical_node,
                del_label_list=[],
                del_property_list=["display", "meshID"]
            ),
            "edge": {
                "label": "research_annotation",
                "property": {
                    "data_source": "research_annotation",
                    "evidence": row["p_value"],
                    "phenotype": row["phenotype"],
                    "guideline_institute": "",
                    "guideline_name": "",
                    "guideline_term": "",
                    "testing_level": "",
                    "bio_geo_group": row["bio_geo_group"]
                }
            }
        }
        edge_list.append(relation_edge)
    return node_list, edge_list

if __name__ == "__main__":
    node_list = []
    edge_list = []

    clinical_node_list, clinical_edge_list = handle_clinical_csv()
    fda_label_node_list, fda_label_edge_list = handle_drug_label_csv()
    guideline_node_list, guideline_edge_list = handle_guideline_csv()
    research_node_list, research_edge_list = handle_research_csv()

    node_list.extend(clinical_node_list)
    node_list.extend(fda_label_node_list)
    node_list.extend(guideline_node_list)
    node_list.extend(research_node_list)
    print(len(node_list))

    edge_list.extend(clinical_edge_list)
    edge_list.extend(fda_label_edge_list)
    edge_list.extend(guideline_edge_list)
    edge_list.extend(research_edge_list)
    print(len(edge_list))

    judge_set = set()
    for x in gene_variant_edge_list:
        if x[0] + x[1] not in list(judge_set):
            judge_set.add(x[0] + x[1])
            edge_list.append(x[2])
    print(len(edge_list))

    with open("../json/nodes.json", "w") as f:
        json.dump(node_list, f)

    with open("../json/edges.json", "w") as f:
        json.dump(edge_list, f)


