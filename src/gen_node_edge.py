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
    # TODO file creation date is from clinical_annotation folder
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
                    "update_date": v_mapping.gene_update_date
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
                "functionality": str(mapping_dict.get("functionality", ""))
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
                    "phenotype": row["phenotype"],
                    "update_date": row["update_date"]
                }
            }
        }
        edge_list.append(relation_edge)
    return node_list, edge_list


def handle_variant_drug_label_csv():
    # TODO file creation date is from drug_label folder
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
                    "update_date": v_mapping.gene_update_date
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
                "functionality": str(mapping_dict.get("functionality", ""))
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
                    "update_date": row["update_date"]
                }
            }
        }
        edge_list.append(relation_edge)
    return node_list, edge_list


def handle_gene_drug_label_csv():
    # TODO file creation date is from drug_label folder
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
                "display": row["gene"]
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
                    "update_date": row["update_date"]
                }
            }
        }
        edge_list.append(relation_edge)
    return node_list, edge_list


def handle_guideline_csv():
    # TODO file creation date is from guideline_annotation folder
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
                    "update_date": v_mapping.gene_update_date
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
                "mapped_rsID": ",".join(mapping_dict.get("rsID", [])),
                "update_date": mapping_dict.get("update_date", ""),
                "frequency": str(mapping_dict.get("frequency", "")),
                "functionality": str(mapping_dict.get("functionality", ""))
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
                    "guideline_term": row["term"],
                    "update_date": row["update_date"]
                }
            }
        }
        edge_list.append(relation_edge)
    return node_list, edge_list


def handle_research_csv():
    # TODO file creation date is from variant_annotation folder
    node_list = []
    edge_list = []

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
                    "update_date": v_mapping.gene_update_date
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
                "functionality": str(mapping_dict.get("functionality", ""))
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
                    "sentence": row["sentence"].replace("\"", "'"),
                    "update_date": row["update_date"]
                }
            }
        }
        edge_list.append(relation_edge)

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
                "functionality": str(hap1_mapping_dict.get("functionality", ""))
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
                "functionality": str(hap2_mapping_dict.get("functionality", ""))
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
                    "sentence": row["sentence"].replace("\"", "'"),
                    "update_date": row["update_date"]
                }
            }
        }
        edge_list.append(dip_edge)

    return node_list, edge_list


def step3_gen_node_edge():
    node_list = []
    edge_list = []

    clinical_node_list, clinical_edge_list = handle_clinical_csv()
    variant_label_node_list, variant_label_edge_list = handle_variant_drug_label_csv()
    gene_label_node_list, gene_label_edge_list = handle_gene_drug_label_csv()
    guideline_node_list, guideline_edge_list = handle_guideline_csv()
    research_node_list, research_edge_list = handle_research_csv()

    node_list.extend(clinical_node_list)
    node_list.extend(variant_label_node_list)
    node_list.extend(gene_label_node_list)
    node_list.extend(guideline_node_list)
    node_list.extend(research_node_list)
    print(len(node_list))

    edge_list.extend(clinical_edge_list)
    edge_list.extend(variant_label_edge_list)
    edge_list.extend(gene_label_edge_list)
    edge_list.extend(guideline_edge_list)
    edge_list.extend(research_edge_list)
    edge_list.extend(research_edge_list)
    print(len(edge_list))

    # deduplicate variant->gene edge data, and add to edge list
    judge_set = set()
    for x in gene_variant_edge_list:
        if x[0] + x[1] not in list(judge_set):
            judge_set.add(x[0] + x[1])
            edge_list.append(x[2])
    print(len(edge_list))

    with open("json/nodes.json", "w") as f:
        json.dump(node_list, f)

    with open("json/edges.json", "w") as f:
        json.dump(edge_list, f)


if __name__ == "__main__":
    step3_gen_node_edge()
