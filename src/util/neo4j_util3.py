#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from neo4j import GraphDatabase
import json
import re
import pandas as pd
import os

def neo4j_run_cypher(tx, cypher_statement):
    tx.run(cypher_statement)

def gen_cypher_value_str(value):
    datetime_re = "datetime\(\'\d{4}\-\d{2}\-\d{2}T\d{2}\:\d{2}\:\d{2}"
    out_str = value
    if isinstance(value, str) and not re.match(datetime_re, value):
        value = value.strip()
        out_str = out_str.replace("'", '"')
        out_str = "'" + value + "'"
    return out_str

def gen_add_node_cypher(node_info):
    node_ID = node_info['node_ID']
    label_list = node_info['label']
    property_dict = node_info['property']
    node_ID_value = ""
    if node_ID in property_dict.keys():
        node_ID_value = property_dict[node_ID]
        node_ID_value = gen_cypher_value_str(node_ID_value)
    cypher_statement = 'MERGE (n'
    for label in label_list:
        cypher_statement = cypher_statement + ":" + label
    cypher_statement = cypher_statement + " {" + node_ID + ":" + str(node_ID_value)+ "})" + "\n"
    cypher_statement = cypher_statement + "SET n += {"
    for key in property_dict.keys():
        property_value = property_dict[key]
        property_value = gen_cypher_value_str(property_value)
        cypher_statement = cypher_statement + key + ":" + str(property_value) + ", "
    cypher_statement = cypher_statement[:-2]
    cypher_statement = cypher_statement + "}\n"
    return cypher_statement

def gen_add_edge_cypher(edge_info):
    start_node = edge_info['start_node']
    end_node = edge_info['end_node']
    edge = edge_info['edge']
    cypher_statement = "MATCH (s"
    if "label" in start_node.keys():
        label = start_node["label"]
        if type(label) == list:
            label = ":".join(label)
        cypher_statement = cypher_statement + ":" + label
    cypher_statement = cypher_statement + "), (e"
    if "label" in end_node.keys():
        label = end_node["label"]
        if type(label) == list:
            label = ":".join(label)
        cypher_statement = cypher_statement + ":" + label
    cypher_statement = cypher_statement + ")\nWHERE"
    start_node_property = start_node['property']
    for key in start_node_property.keys():
        value = start_node_property[key]
        value = gen_cypher_value_str(value)
        cypher_statement = cypher_statement + "\ns." + key + " = " + str(value) + " AND"
    end_node_property = end_node['property']
    for key in end_node_property.keys():
        value = end_node_property[key]
        value = gen_cypher_value_str(value)
        cypher_statement = cypher_statement + "\ne." + key + " = " + str(value) + " AND"
    cypher_statement = cypher_statement[:-3] + "\nMERGE\n(s) -[r:"
    edge_label = edge["label"]
    cypher_statement = cypher_statement + edge_label
    if ("property" in edge.keys()):
        edge_property = edge["property"]
        if len(edge_property) > 0:
            cypher_statement = cypher_statement + " {"
            for key in edge_property.keys():
                value = edge_property[key]
                value = gen_cypher_value_str(value)
                cypher_statement = cypher_statement + key + ":" + value + ","
            cypher_statement = cypher_statement[:-1] + "}"
    cypher_statement = cypher_statement + "]-> (e)"
    return cypher_statement
    
def add_node_to_neo4j(node_list, neo4j_host, neo4j_user, neo4j_pwd, neo4j_database):
    driver = GraphDatabase.driver(neo4j_host, auth=(neo4j_user, neo4j_pwd))
    with driver.session(database = neo4j_database) as session:
        for node_info in node_list:
            try:
                cypher_statement = gen_add_node_cypher(node_info)
                session.write_transaction(neo4j_run_cypher, cypher_statement)
            except:
                continue
    driver.close()   

def add_edge_to_neo4j(edge_list, neo4j_host, neo4j_user, neo4j_pwd, neo4j_database):
    driver = GraphDatabase.driver(neo4j_host, auth=(neo4j_user, neo4j_pwd))
    with driver.session(database = neo4j_database) as session:
        for i in range(0, len(edge_list)):
            edge_info = edge_list[i]
            cypher_statement = gen_add_edge_cypher(edge_info)
            if (len(cypher_statement.strip()) > 0):
                session.write_transaction(neo4j_run_cypher, cypher_statement)
    driver.close()

def delete_all_neo4j(neo4j_host, neo4j_user, neo4j_pwd, neo4j_database):
    driver = GraphDatabase.driver(neo4j_host, auth=(neo4j_user, neo4j_pwd))
    with driver.session(database = neo4j_database) as session:
        cypher_statement = "match (n) detach delete n"
        session.write_transaction(neo4j_run_cypher, cypher_statement)
    driver.close()
    