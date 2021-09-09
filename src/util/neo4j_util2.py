#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import pandas as pd
from neo4j.v1 import GraphDatabase
import ast

exec(open("neo4j_util1.py").read())

def import_relation(driver, rel_df):
    with driver.session() as session:
        for i in range(0, rel_df.shape[0]):
            print ("Importing row " + str(i+1) + " out of " + str(rel_df.shape[0]) + " rows")
            row = rel_df.iloc[i]
            entity_1 = str(row['entity_1'])
            entity_1 = entity_1.replace("'","")
            entity_1_type = row['entity_1_type']
            entity_2 = str(row['entity_2'])
            entity_2 = entity_2.replace("'","")
            entity_2_type = row['entity_2_type']      
            if entity_1 != "nan" and entity_2 != "nan":
                relation = row['relation']
                attribute_str = row['attribute']
                attribute = ast.literal_eval(attribute_str)
                session.write_transaction(neo4j_create_relation, entity_1_type, entity_1, entity_2_type, entity_2, relation, attribute)

def import_node_property(driver, node_df):
    with driver.session() as session:
        for i in range(0, node_df.shape[0]):
            row = node_df.iloc[i]
            entity = row['entity']
            entity_type = row['entity_type']
            attribute_str = row['attribute']
            attribute = ast.literal_eval(attribute_str)
            node_id  = session.write_transaction(neo4j_get_node_id_by_name, entity)
            print (node_id)
            
def mod_list_of_rel_prop_dtype(driver, rel_prop_dtype_df):
    with driver.session() as session:
        for i in range(0, rel_prop_dtype_df.shape[0]):
            row = rel_prop_dtype_df.iloc[i]
            rel_prop = row['property']
            dtype = row['dtype']
            session.write_transaction(neo4j_mod_rel_prop_dtype, rel_prop, dtype)

def create_alias_clusters(driver, alias_df):
    with driver.session() as session:
        for i in range(0, alias_df.shape[0]):
            print ("Creating " + str(i+1) + " relations out of " + str(alias_df.shape[0]))
            row = alias_df.iloc[i]
            master_node = row['master_node']
            alias_node = row['alias_node']
            session.write_transaction(neo4j_make_alias_relations, master_node, alias_node)
        cluster_list = []
        for i in range(0, alias_df.shape[0]):
            row = alias_df.iloc[i]
            temp_name = row['master_node']
            if temp_name in cluster_list:
                cluster_list.remove(temp_name)
            cluster_list.append(temp_name)
        c_num = 1
        for i in range(0,len(cluster_list)):
            master_name = cluster_list[i]
            print ("Creating cluster " + str(c_num) + "  out of " + str((len(cluster_list))))
            master_id = session.write_transaction(neo4j_get_node_id_by_name, master_name)
            if (master_id != ""):
                session.write_transaction(neo4j_arrange_alias_cluster, master_id)
            c_num = c_num + 1

def make_master_node_of_alias(driver, node_list):
    with driver.session() as session:
        for node_name in node_list:
            node_id = ""
            node_id = session.write_transaction(neo4j_get_node_id_by_name, node_name)
            if (node_id != ""):
                session.write_transaction(neo4j_arrange_alias_cluster, node_id)
                print ("arrange " + node_name + " as master node")
            else:
                print (node_name + " not found")

def add_label_to_nodes(driver, node_list, label):
    with driver.session() as session:
        temp_cnt = 1
        node_list_size = len(node_list)
        for node_name in node_list:
            print ("adding label to " + str(temp_cnt) + " nodes out of " + str(node_list_size))
            node_id = session.write_transaction(neo4j_get_node_id_by_name, node_name)
            session.write_transaction(neo4j_add_node_label, node_id, label)
            temp_cnt = temp_cnt + 1
            
def add_property_to_nodes(driver, node_df):
    with driver.session() as session:
        for i in range(0, node_df.shape[0]):
            print ('adding property for node ' + str(i) + " out of " + str(node_df.shape[0]))
            row = node_df.iloc[i]
            node_name = row['entity']
            entity_type = row['entity_type']
            attribute_str = row['attribute']
            attribute = ast.literal_eval(attribute_str)
            node_id = session.write_transaction(neo4j_get_node_id_by_name, node_name)
            session.write_transaction(neo4j_add_properties_to_node, node_id, attribute)
     
def get_entity_name_from_alias(driver, alias_name):
    with driver.session() as session:
        entity_name = session.write_transaction(neo4j_get_entity_name_from_alias, alias_name)
    return entity_name

def get_cycle_nodes(driver, in_node_name, relation):
    cycle_node_name_set = set()
    with driver.session() as session:
        in_node_id = session.write_transaction(neo4j_get_node_id_by_name, in_node_name)
        if len(str(in_node_id)) > 0:
            cycle_node_id_set = session.write_transaction(neo4j_find_cycle_nodes, in_node_id, relation)
            for node_id in cycle_node_id_set:
                node_name = session.write_transaction(neo4j_get_node_name_by_id, node_id)
                cycle_node_name_set.add(node_name)
    return cycle_node_name_set

def get_node_names_by_type(driver, node_type):
    with driver.session() as session:
        result_list = session.write_transaction(neo4j_get_node_names_by_type, node_type)
    return result_list

def get_relations_as_df(driver, rel_type, limit = 0):
    with driver.session() as session:
        rel_df = session.write_transaction(neo4j_find_relations_by_type, rel_type, limit)
    return rel_df
    
def get_self_loops_as_df(driver, limit = 0):
    with driver.session() as session:
        rel_df = session.write_transaction(neo4j_find_self_loops, limit)
    return rel_df

def remove_duplicated_relations(driver):
    with driver.session() as session:
        session.write_transaction(neo4j_remove_duplicated_relations)
         
def merge_relations_by_type(driver, merge_rel_list, new_rel):
    with driver.session() as session:
        session.write_transaction(neo4j_merge_relations_by_type, merge_rel_list, new_rel)

def remove_relations_by_type(driver, rel_type):
    with driver.session() as session:
        session.write_transaction(neo4j_remove_relations_by_type, rel_type)

def remove_orphan_nodes(driver):    
    with driver.session() as session:
        session.write_transaction(neo4j_remove_orphan_nodes)

def get_node_properties_by_label(driver, node_type):
    with driver.session() as session:
        property_list = []
        node_id_list = session.write_transaction(neo4j_get_node_id_by_type, node_type)
        for i in range(0, len(node_id_list)):
            node_id = node_id_list[i]
            node_properties = session.write_transaction(neo4j_get_node_properties_by_id, node_id)
            property_list.append(node_properties)
    return property_list