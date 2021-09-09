#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from neo4j.v1 import GraphDatabase
from fuzzywuzzy import fuzz
import json
import re

exec(open("demo_api_util1.py").read())

def deduplicate_dict_list(dict_list):
    col_list = []
    for i in range(0, len(dict_list)):
        temp_dict = dict_list[i]
        temp_col_list = list(temp_dict.keys())
        for j in range(0, len(temp_col_list)):
            col = temp_col_list[j]
            if col not in col_list:
                col_list.append(col)
#            col_set.add(col)
    
#    col_list = list(col_set)
    out_df = pd.DataFrame(columns = col_list)
    
    for i in range(0, len(dict_list)):
        temp_dict = dict_list[i]
        temp_col_keys = temp_dict.keys()
        v_list = []
        for j in range(0, len(col_list)):
            col = col_list[j]
            if col in temp_col_keys:
                val = temp_dict[col]
                v_list.append(val)
            else:
                v_list.append(None)
                
        temp_df = pd.DataFrame([v_list], columns = col_list)
        out_df = out_df.append(temp_df, ignore_index=True)
    out_df = out_df.drop_duplicates()
    
    out_dict_list = []

    for i in range(0, out_df.shape[0]):
        temp_row = out_df.iloc[i]
        temp_dict = {}
        for j in range(0, len(col_list)):
            col = col_list[j]
            temp_v = temp_row[col]
            if temp_v != None:
                temp_dict[col] = temp_v
        out_dict_list.append(temp_dict)
    return out_dict_list

def add_1st_lv_relation(nodeID, relation_dict):
    with neo4j_driver.session() as session:
        out_dict = {}
        relation_dict_key_list = list(relation_dict.keys())
        if ('industry' in relation_dict_key_list):
            relation_dict_key_list.remove('industry')
        
        for i in range(0, len(relation_dict_key_list)):
            temp_relation = relation_dict_key_list[i]
            relation_dict_list = relation_dict[temp_relation]
            temp_relation_dict_list = []
            for j in range(0, len(relation_dict_list)):
                temp_node_dict = relation_dict_list[j]
                temp_nodeID = temp_node_dict['nodeID']
                temp_relation_list = session.write_transaction(neo4j_find_1st_lv_relation_between_nodes, nodeID, temp_nodeID)
                temp_node_dict.update({'relation':temp_relation_list})
                temp_relation_dict_list.append(temp_node_dict)
            out_dict[temp_relation] = temp_relation_dict_list
    return relation_dict

def add_2nd_lv_relation(nodeID, relation_dict):
    with neo4j_driver.session() as session:
        out_dict = {}
        relation_dict_key_list = list(relation_dict.keys())
        if ('industry' in relation_dict_key_list):
            relation_dict_key_list.remove('industry')
        
        for i in range(0, len(relation_dict_key_list)):
            temp_relation = relation_dict_key_list[i]
            relation_dict_list = relation_dict[temp_relation]
            temp_relation_dict_list = []
            for j in range(0, len(relation_dict_list)):
                temp_node_dict = relation_dict_list[j]
                temp_nodeID = temp_node_dict['nodeID']
                temp_relation_list = session.write_transaction(neo4j_find_2nd_lv_relation_between_nodes, nodeID, temp_nodeID)
                temp_node_dict.update({'relation':temp_relation_list})
                temp_relation_dict_list.append(temp_node_dict)
            out_dict[temp_relation] = temp_relation_dict_list
    return relation_dict

def find_related_company_for_company(nodeID, threshold = 0.0):
    related_company_dict = {}
    with neo4j_driver.session() as session:
        node_types =session.write_transaction(neo4j_check_node_type, nodeID)
        # for listed company
        if ('listed_company' in node_types):
            shareholder_coeff = 1
            shareholding_coeff = 1
            board_member_coeff = 0.5
            shareholder_in_degree_df = session.write_transaction(neo4j_find_relation_degree, 'shareholder', 'in')
            shareholder_in_degree_df = shareholder_in_degree_df.rename(columns={'degree':'in_degree'})
            shareholder_out_degree_df = session.write_transaction(neo4j_find_relation_degree, 'shareholder', 'out')
            shareholder_out_degree_df = shareholder_out_degree_df.rename(columns={'degree':'out_degree'})
            shareholder_degree_df = shareholder_in_degree_df.merge(shareholder_out_degree_df, how = 'outer', left_on = 'node', right_on = 'node')
            shareholder_degree_df = shareholder_degree_df.fillna(0)
#            shareholder_degree_df['total_degree'] = shareholder_degree_df['in_degree'] + shareholder_degree_df['out_degree']
            shareholder_degree_df = shareholder_degree_df.rename(columns={'node':'common_node'})
#            shareholder_degree_df.to_csv(out_file, sep = '\t', index = False)
            
            board_member_degree_df = session.write_transaction(neo4j_find_relation_degree, 'board_member|company_secretary', 'out')
            board_member_degree_df = board_member_degree_df.rename(columns={'node':'common_node'})
#            board_member_degree_df.to_csv(out_file, sep = '\t', index = False)
            
            common_shareholder_node_df = session.write_transaction(neo4j_find_common_relation_node, nodeID, 'company', 'shareholder', 'in')
            common_shareholder_node_df = common_shareholder_node_df.merge(shareholder_degree_df, how = 'left', left_on = 'common_node', right_on = 'common_node')
            common_shareholder_node_df['row_weight'] = common_shareholder_node_df.apply(lambda row: shareholder_coeff/(row.out_degree - 1), axis=1)
#            common_shareholder_node_df.to_csv('out.csv', sep = '\t', index = False)
            common_shareholder_weight = common_shareholder_node_df.groupby(['node'],as_index = False)['row_weight'].agg('sum')
            common_shareholder_weight = common_shareholder_weight.rename(columns={'row_weight':'common_shareholder_weight'})
#            common_shareholder_weight.to_csv(out_file, sep = '\t', index = False)
           
            common_shareholding_node_df = session.write_transaction(neo4j_find_common_relation_node, nodeID, 'company', 'shareholder', 'out')
            common_shareholding_node_df = common_shareholding_node_df.merge(shareholder_degree_df, how = 'left', left_on = 'common_node', right_on = 'common_node')
            common_shareholding_node_df['row_weight'] = common_shareholding_node_df.apply(lambda row: shareholding_coeff/(row.in_degree - 1), axis=1)
#            common_shareholding_node_df.to_csv('out.csv', sep = '\t', index = False)
            common_shareholding_weight = common_shareholding_node_df.groupby(['node'],as_index = False)['row_weight'].agg('sum')
            common_shareholding_weight = common_shareholding_weight.rename(columns={'row_weight':'common_shareholding_weight'})
#            common_shareholding_weight.to_csv(out_file, sep = '\t', index = False)
           
            shareholder_2nd_lv_node_df = session.write_transaction(neo4j_find_2nd_lv_reach_node, nodeID, 'company', 'shareholder', 'backward')
            shareholder_2nd_lv_node_df = shareholder_2nd_lv_node_df.merge(shareholder_degree_df, how = 'left', left_on = 'common_node', right_on = 'common_node')
            shareholder_2nd_lv_node_df['row_weight'] = shareholder_2nd_lv_node_df.apply(lambda row: 0.5*(shareholder_coeff + shareholding_coeff)/(row.in_degree + row.out_degree - 1), axis=1)
#            shareholder_2nd_lv_node_df.to_csv('out.csv', sep = '\t', index = False)
            shareholder_2nd_lv_weight = shareholder_2nd_lv_node_df.groupby(['node'],as_index = False)['row_weight'].agg('sum')
            shareholder_2nd_lv_weight = shareholder_2nd_lv_weight.rename(columns={'row_weight':'shareholder_2nd_lv_weight'})
#            shareholder_2nd_lv_weight.to_csv(out_file, sep = '\t', index = False)
            
            shareholding_2nd_lv_node_df = session.write_transaction(neo4j_find_2nd_lv_reach_node, nodeID, 'company', 'shareholder', 'forward')
            shareholding_2nd_lv_node_df = shareholding_2nd_lv_node_df.merge(shareholder_degree_df, how = 'left', left_on = 'common_node', right_on = 'common_node')
            shareholding_2nd_lv_node_df['row_weight'] = shareholding_2nd_lv_node_df.apply(lambda row: 0.5*(shareholder_coeff + shareholding_coeff)/(row.in_degree + row.out_degree - 1), axis=1)
#            shareholding_2nd_lv_node_df.to_csv('out.csv', sep = '\t', index = False)
            shareholding_2nd_lv_weight = shareholding_2nd_lv_node_df.groupby(['node'],as_index = False)['row_weight'].agg('sum')
            shareholding_2nd_lv_weight = shareholding_2nd_lv_weight.rename(columns={'row_weight':'shareholding_2nd_lv_weight'})
#            shareholding_2nd_lv_weight.to_csv(out_file, sep = '\t', index = False)
#            
            common_board_member_node_df = session.write_transaction(neo4j_find_common_relation_node, nodeID, 'company', 'board_member|company_secretary', 'in')
            common_board_member_node_df = common_board_member_node_df.merge(board_member_degree_df, how = 'left', left_on = 'common_node', right_on = 'common_node')
            common_board_member_node_df['row_weight'] = common_board_member_node_df.apply(lambda row: board_member_coeff, axis=1)
#            common_board_member_node_df.to_csv('out.csv', sep = '\t', index = False)
            common_board_member_weight = common_board_member_node_df.groupby(['node'],as_index = False)['row_weight'].agg('sum')
            common_board_member_weight = common_board_member_weight.rename(columns={'row_weight':'common_board_member_weight'})
#            common_board_member_weight.to_csv(out_file, sep = '\t', index = False)            
#            
            related_node_df = common_shareholder_weight.merge(common_shareholding_weight, how = 'outer', left_on = 'node', right_on = 'node')
            related_node_df = related_node_df.merge(shareholder_2nd_lv_weight, how = 'outer', left_on = 'node', right_on = 'node')
            related_node_df = related_node_df.merge(shareholding_2nd_lv_weight, how = 'outer', left_on = 'node', right_on = 'node')
            related_node_df = related_node_df.merge(common_board_member_weight, how = 'outer', left_on = 'node', right_on = 'node')
            related_node_df = related_node_df.fillna(0)
            related_node_df['relation_eval'] = related_node_df['common_shareholder_weight'] + \
                                               related_node_df['common_shareholding_weight'] + \
                                               related_node_df['shareholder_2nd_lv_weight'] + \
                                               related_node_df['shareholding_2nd_lv_weight'] + \
                                               related_node_df['common_board_member_weight']
#            related_node_df.to_csv('out.csv', sep = '\t', index = False)
            
            for i in range(0,related_node_df.shape[0]):
                row = related_node_df.iloc[i]
                company = row['node']
                relation_eval = row['relation_eval']
                if relation_eval >= threshold:
                    related_company_dict[company] = relation_eval
                    
#        # for non_listed_company
        elif ('company' in node_types):
            shareholder_coeff = 7
            shareholding_coeff = 7
#            print ('non_listed_company')
            shareholder_in_degree_df = session.write_transaction(neo4j_find_relation_degree, 'shareholder', 'in')
            shareholder_in_degree_df = shareholder_in_degree_df.rename(columns={'degree':'in_degree'})
            shareholder_out_degree_df = session.write_transaction(neo4j_find_relation_degree, 'shareholder', 'out')
            shareholder_out_degree_df = shareholder_out_degree_df.rename(columns={'degree':'out_degree'})
            shareholder_degree_df = shareholder_in_degree_df.merge(shareholder_out_degree_df, how = 'outer', left_on = 'node', right_on = 'node')
            shareholder_degree_df = shareholder_degree_df.fillna(0)
            shareholder_degree_df['total_degree'] = shareholder_degree_df['in_degree'] + shareholder_degree_df['out_degree']
            shareholder_degree_df = shareholder_degree_df.rename(columns={'node':'common_node'})
#            shareholder_degree_df.to_csv(out_file, sep = '\t', index = False)
            
            company_info = session.write_transaction(neo4j_get_company_info_by_nodeID, nodeID)
            company = company_info['nodeName']
            
            temp_row = shareholder_degree_df.loc[shareholder_degree_df['common_node'] == company]
            company_in_degree = list(temp_row['in_degree'])[0] + 1
            
            temp_row = shareholder_degree_df.loc[shareholder_degree_df['common_node'] == company]
            company_out_degree = list(temp_row['out_degree'])[0] + 1            
            
            common_shareholder_node_df = session.write_transaction(neo4j_find_common_relation_node, nodeID, 'company', 'shareholder', 'in')
            common_shareholder_node_df = common_shareholder_node_df.merge(shareholder_degree_df, how = 'left', left_on = 'common_node', right_on = 'common_node')
            common_shareholder_node_df['row_weight'] = common_shareholder_node_df.apply(lambda row: shareholder_coeff/(company_in_degree*(row.out_degree - 1)), axis=1)
#            common_shareholder_node_df.to_csv(out_file, sep = '\t', index = False)
            common_shareholder_weight = common_shareholder_node_df.groupby(['node'],as_index = False)['row_weight'].agg('sum')
            common_shareholder_weight = common_shareholder_weight.rename(columns={'row_weight':'common_shareholder_weight'})
#            common_shareholder_weight.to_csv(out_file, sep = '\t', index = False)
            
            common_shareholding_node_df = session.write_transaction(neo4j_find_common_relation_node, nodeID, 'company', 'shareholder', 'out')
            common_shareholding_node_df = common_shareholding_node_df.merge(shareholder_degree_df, how = 'left', left_on = 'common_node', right_on = 'common_node')
            common_shareholding_node_df['row_weight'] = common_shareholding_node_df.apply(lambda row: shareholding_coeff/(company_out_degree*(row.in_degree - 1)), axis=1)
#            common_shareholding_node_df.to_csv(out_file, sep = '\t', index = False)
            common_shareholding_weight = common_shareholding_node_df.groupby(['node'],as_index = False)['row_weight'].agg('sum')
            common_shareholding_weight = common_shareholding_weight.rename(columns={'row_weight':'common_shareholding_weight'})
#            common_shareholding_weight.to_csv(out_file, sep = '\t', index = False)
            
            shareholder_2nd_lv_node_df = session.write_transaction(neo4j_find_2nd_lv_reach_node, nodeID, 'company', 'shareholder', 'backward')
            shareholder_2nd_lv_node_df = shareholder_2nd_lv_node_df.merge(shareholder_degree_df, how = 'left', left_on = 'common_node', right_on = 'common_node')
            shareholder_2nd_lv_node_df['row_weight'] = shareholder_2nd_lv_node_df.apply(lambda row: 0.5*(shareholder_coeff + shareholding_coeff)/(company_in_degree*(row.in_degree + row.out_degree - 1)), axis=1)
#            shareholder_2nd_lv_node_df.to_csv(out_file, sep = '\t', index = False)
            shareholder_2nd_lv_weight = shareholder_2nd_lv_node_df.groupby(['node'],as_index = False)['row_weight'].agg('sum')
            shareholder_2nd_lv_weight = shareholder_2nd_lv_weight.rename(columns={'row_weight':'shareholder_2nd_lv_weight'})
#            shareholder_2nd_lv_weight.to_csv(out_file, sep = '\t', index = False)
            
            shareholding_2nd_lv_node_df = session.write_transaction(neo4j_find_2nd_lv_reach_node, nodeID, 'company', 'shareholder', 'forward')
            shareholding_2nd_lv_node_df = shareholding_2nd_lv_node_df.merge(shareholder_degree_df, how = 'left', left_on = 'common_node', right_on = 'common_node')
            shareholding_2nd_lv_node_df['row_weight'] = shareholding_2nd_lv_node_df.apply(lambda row: 0.5*(shareholder_coeff + shareholding_coeff)/(company_out_degree*(row.in_degree + row.out_degree - 1)), axis=1)
#            shareholding_2nd_lv_node_df.to_csv(out_file, sep = '\t', index = False)
            shareholding_2nd_lv_weight = shareholding_2nd_lv_node_df.groupby(['node'],as_index = False)['row_weight'].agg('sum')
            shareholding_2nd_lv_weight = shareholding_2nd_lv_weight.rename(columns={'row_weight':'shareholding_2nd_lv_weight'})
#            shareholding_2nd_lv_weight.to_csv(out_file, sep = '\t', index = False)
            
            related_node_df = common_shareholder_weight.merge(common_shareholding_weight, how = 'outer', left_on = 'node', right_on = 'node')
            related_node_df = related_node_df.merge(shareholder_2nd_lv_weight, how = 'outer', left_on = 'node', right_on = 'node')
            related_node_df = related_node_df.merge(shareholding_2nd_lv_weight, how = 'outer', left_on = 'node', right_on = 'node')
            related_node_df = related_node_df.fillna(0)
            related_node_df['relation_eval'] = related_node_df['common_shareholder_weight'] + \
                                               related_node_df['common_shareholding_weight'] + \
                                               related_node_df['shareholder_2nd_lv_weight'] + \
                                               related_node_df['shareholding_2nd_lv_weight']
                                               
            for i in range(0,related_node_df.shape[0]):
                row = related_node_df.iloc[i]
                company = row['node']
                relation_eval = row['relation_eval']
                if relation_eval >= threshold:
                    related_company_dict[company] = relation_eval
#                    
##            related_node_df.to_csv(out_file, sep = '\t', index = False)                      
#        # not a company, return error
    return related_company_dict

def find_related_person_for_person(nodeID, threshold = 0.0):
    related_person_dict = {}
    with neo4j_driver.session() as session:
        node_types =session.write_transaction(neo4j_check_node_type, nodeID)
        if 'person' in node_types:
            common_board_member_node_df = session.write_transaction(neo4j_find_common_relation_node, nodeID, 'person', 'shareholder|board_member|company_secretary', 'out')
            common_board_member_node_df['row_weight'] = common_board_member_node_df.apply(lambda row: 1, axis=1)
            common_board_member_node_df.loc[common_board_member_node_df['node'] == '***--Place holder--***','row_weight'] = -1
#            common_board_member_node_df.to_csv('out.csv', sep = '\t', index = False)
            common_board_member_weight = common_board_member_node_df.groupby(['node'],as_index = False)['row_weight'].agg('sum')
            common_board_member_weight = common_board_member_weight.rename(columns={'row_weight':'common_board_member_weight'})
#            common_board_member_weight.to_csv('out.csv', sep = '\t', index = False)
            
            for i in range(0,common_board_member_weight.shape[0]):
                row = common_board_member_weight.iloc[i]
                person = row['node']
                relation_eval = row['common_board_member_weight']
                if relation_eval >= threshold:
                    related_person_dict[person] = relation_eval
    return related_person_dict

######################################################################################
def get_company_profile_by_nodeID(nodeID):
    with neo4j_driver.session() as session:
        node_info = session.write_transaction(neo4j_get_company_info_by_nodeID, nodeID)
        node_relation = session.write_transaction(neo4j_get_company_relation_by_nodeID, nodeID)
        node_relation = add_1st_lv_relation(nodeID, node_relation)
#        print (node_info)
        related_company_excl_set = set()
        temp_nodeName = node_info['nodeName']
        related_company_excl_set.add(temp_nodeName)
#        print (related_company_excl_set)
        
        temp_shareholder_list = node_relation['shareholder']
        for i in range(0,len(temp_shareholder_list)):
            temp_shareholder = temp_shareholder_list[i]
            temp_nodeName = temp_shareholder['nodeName']
            related_company_excl_set.add(temp_nodeName)
            
        temp_shareholding_list = node_relation['shareholding']
        for i in range(0,len(temp_shareholding_list)):
            temp_shareholding = temp_shareholding_list[i]
            temp_nodeName = temp_shareholding['nodeName']
            related_company_excl_set.add(temp_nodeName)
        
        related_company_dict = find_related_company_for_company(nodeID, 0.51)
        temp_related_company_list = sorted(related_company_dict)
#        print (related_company_excl_set)
        related_company_list = []
        for i in range(0, len(temp_related_company_list)):
            related_company_nodeName = temp_related_company_list[i]
            if (related_company_nodeName not in related_company_excl_set):
#                print (related_company_nodeName)
                related_company_node_info = session.write_transaction(neo4j_get_node_info_by_nodeName, related_company_nodeName)
#                related_company_node_info['nodeName'] = related_company_nodeName
                related_company_list.append(related_company_node_info)
        related_company_dict = {'relatedCompany':related_company_list}
        related_company_dict = add_2nd_lv_relation(nodeID, related_company_dict)
        out_dict = {}
        out_dict.update(node_info)
        out_dict.update(node_relation)
        out_dict.update(related_company_dict)
    return out_dict

def get_company_news_by_nodeID(nodeID):
    with neo4j_driver.session() as session:
        node_info = session.write_transaction(neo4j_get_company_info_by_nodeID, nodeID)
        nodeName = node_info['nodeName']
        nodeType = node_info['nodeType']
        if nodeType == 'listed_company':
            relatedNews_dict = session.write_transaction(neo4j_get_listed_company_news_by_nodeID, nodeID)
        if nodeType == 'company':
            relatedNews_dict = session.write_transaction(neo4j_get_company_news_by_nodeID, nodeID)
        out_dict = {}
        out_dict.update(node_info)
        out_dict.update(relatedNews_dict)
    return out_dict

def get_person_profile_by_nodeID(nodeID):
    with neo4j_driver.session() as session:
        node_info = session.write_transaction(neo4j_get_person_info_by_nodeID, nodeID)
        node_relation = session.write_transaction(neo4j_get_person_relation_by_nodeID, nodeID)
        node_relation = add_1st_lv_relation(nodeID, node_relation)
        related_person_dict = find_related_person_for_person(nodeID, 0)
        temp_related_person_list = sorted(related_person_dict)
        related_person_list = []
        for i in range(0, len(temp_related_person_list)):
            related_person_nodeName = temp_related_person_list[i]
            related_person_node_info = session.write_transaction(neo4j_get_node_info_by_nodeName, related_person_nodeName)
            related_person_list.append(related_person_node_info)
        related_person_dict = {'relatedAssociate':related_person_list}
        related_person_dict = add_2nd_lv_relation(nodeID, related_person_dict)
        out_dict = {}
        out_dict.update(node_info)
        out_dict.update(node_relation)
        out_dict.update(related_person_dict)
    return out_dict

def get_person_news_by_nodeID(nodeID):
    with neo4j_driver.session() as session:
        node_info = session.write_transaction(neo4j_get_person_info_by_nodeID, nodeID)
        nodeName = node_info['nodeName']
        nodeType = node_info['nodeType']
        if nodeType == 'person':
            relatedNews_dict = session.write_transaction(neo4j_get_person_news_by_nodeID, nodeID)

        out_dict = {}
        out_dict.update(node_info)
        out_dict.update(relatedNews_dict)
    return out_dict

def get_company_relation_graph_by_nodeID(nodeID):
    with neo4j_driver.session() as session:
        relation_list = []    
        profile_dict = get_company_profile_by_nodeID(nodeID)
        nodeName = profile_dict['nodeName']
        nodeType = profile_dict['nodeType']
        nodeID = int(nodeID)
        
        profile_dict_key_list = list(profile_dict.keys())
        for i in range(0, len(profile_dict_key_list)):
            profile_dict_key = profile_dict_key_list[i]
            temp_data = profile_dict[profile_dict_key]
            if type(temp_data) == list:
                for j in range(0, len(temp_data)):
                    temp_data2 = temp_data[j]
                    if type(temp_data2) == dict and 'relation' in temp_data2.keys():
                        temp_relation = temp_data2['relation']
                        relation_list = relation_list + temp_relation
        relation_list = deduplicate_dict_list(relation_list)
        out_dict = {'nodeName':nodeName,
                    'nodeType':nodeType,
                    'nodeID':nodeID,
                    'relation':relation_list}
        return out_dict
        
def get_person_relation_graph_by_nodeID(nodeID):
    with neo4j_driver.session() as session:
        relation_list = [] 
        profile_dict = get_person_profile_by_nodeID(nodeID)
        nodeName = profile_dict['nodeName']
        nodeType = profile_dict['nodeType']
        nodeID = int(nodeID)
        
        profile_dict_key_list = list(profile_dict.keys())
        for i in range(0, len(profile_dict_key_list)):
            profile_dict_key = profile_dict_key_list[i]
            temp_data = profile_dict[profile_dict_key]
            if type(temp_data) == list:
                for j in range(0, len(temp_data)):
                    temp_data2 = temp_data[j]
                    if type(temp_data2) == dict and 'relation' in temp_data2.keys():
                        temp_relation = temp_data2['relation']
                        relation_list = relation_list + temp_relation
        relation_list = deduplicate_dict_list(relation_list)
        out_dict = {'nodeName':nodeName,
                    'nodeType':nodeType,
                    'nodeID':nodeID,
                    'relation':relation_list}
        return out_dict
    
def get_relations_between_nodes(nodeID_1, nodeID_2, numStep):
    with neo4j_driver.session() as session:
        relation_list = []
        node1_info = session.write_transaction(neo4j_get_company_info_by_nodeID, nodeID_1)
        node1Name = node1_info['nodeName']
        node1Type = node1_info['nodeType']
        node2_info = session.write_transaction(neo4j_get_company_info_by_nodeID, nodeID_2)
        node2Name = node2_info['nodeName']
        node2Type = node2_info['nodeType']
        relation_list = session.write_transaction(neo4j_get_relations_between_nodes,nodeID_1, nodeID_2, numStep)
        out_dict = {'node1Name':node1Name,
                    'node1Type':node1Type,
                    'node1ID':nodeID_1,
                    'node2Name':node2Name,
                    'node2Type':node2Type,
                    'node2ID':nodeID_2,
                    'relation':relation_list}
        return out_dict

def calc_string_similarity(row, nodeName_str):
    alias = str(row['alias'])
    sim1 = fuzz.token_set_ratio(alias, nodeName_str)
    sim2 = fuzz.ratio(alias, nodeName_str)
    simlarity = sim1/100 + sim2/10000
    return simlarity

def search_node_by_nodeName(nodeName_str, num_return):
    nodeName_str = nodeName_str.upper()
    out_dict = {}
    with neo4j_driver.session() as session:
        nodeID_df = session.write_transaction(neo4j_get_all_node_id_by_labels, ['person','company'])
        nodeID_df = nodeID_df[['nodeID','nodeName','nodeType']]
        node_alias_df = pd.read_csv('node_alias.csv', sep = '\t')
#        node_alias_df = node_alias_df.rename(columns = {'nodeName': 'masterNodeName'})
        node_alias_df = node_alias_df.merge(nodeID_df, how = 'left', left_on = 'nodeName', right_on = 'nodeName')
#        node_alias_df.to_csv('new_node_alias2.csv', sep = '\t', index = False)
        node_alias_df['similarity'] = node_alias_df.apply(lambda row:calc_string_similarity(row, nodeName_str), axis=1)
        node_alias_df = node_alias_df.sort_values(by="similarity",inplace=False, ascending=False)
        if (num_return > node_alias_df.shape[0]):
            num_return = node_alias_df.shape[0]
        row_i = 0
        num_result = 0
        ID_set = set()
        searchResult_list = []
        while (num_result < num_return):
            temp_row = node_alias_df.iloc[row_i]
            nodeID = temp_row['nodeID']
            nodeName = temp_row['nodeName']
            nodeType = temp_row['nodeType']
            alias = temp_row['alias']
            
            if nodeID not in ID_set:
                is_number = re.search("\d+", alias)
                if is_number and alias ==  is_number.group():
                    alias = "Stock code: " + alias
                elif (alias != nodeName):
                    alias = 'Also known as: ' + alias 
                temp_result = {}
                temp_result['nodeName'] = clean_str(nodeName)
                temp_result['nodeType'] = nodeType
                temp_result['nodeID'] = int(nodeID)
                temp_result['alias'] = alias   
                searchResult_list.append(temp_result)
                ID_set.add(nodeID)
                num_result = num_result + 1
            row_i = row_i + 1
#        node_alias_df.to_csv('new_node_alias2.csv', sep = '\t', index = False) 
        out_dict = {'searchResult':searchResult_list}
    return (out_dict)

api_host_ip = '10.6.73.73'
api_port = 8888
neo4j_url = "bolt://10.6.73.73:7687"
neo4j_driver = GraphDatabase.driver(neo4j_url, auth=("neo4j", "AstriData"))
out_dict = search_node_by_nodeName('CK', 5)
print (out_dict)

#def get_company_relation_graph_by_nodeID(nodeID):
#    with neo4j_driver.session() as session:
#        relation_list = []    
#        profile_dict = get_company_profile_by_nodeID(nodeID)
#        nodeName = profile_dict['nodeName']
#        nodeType = profile_dict['nodeType']
#        nodeID = int(nodeID)
#        
#        board_member_list = profile_dict['board_member']
#        for i in range(0, len(board_member_list)):
#            board_member = board_member_list[i]
#            startNodeName = board_member['nodeName']
#            startNodeID = board_member['nodeID']
#            relation = "board_member"
#            endNodeName = nodeName
#            endNodeType = nodeType
#            endNodeID = nodeID
#            relation_dict = {'startNodeName':startNodeName,
#                             'startNodeType':'person',
#                             'startNodeID':startNodeID,
#                             'relation':relation,
#                             'endNodeName':endNodeName,
#                             'endNodeType':endNodeType,
#                             'endNodeID':endNodeID}
#            relation_list.append(relation_dict)
#        
#        shareholder_list = profile_dict['shareholder']
#        for i in range(0, len(shareholder_list)):
#            shareholder = shareholder_list[i]456
#            startNodeName = shareholder['nodeName']
#            startNodeType = shareholder['nodeType']
#            startNodeID = shareholder['nodeID']
#            relation = "shareholder"
#            endNodeName = nodeName
#            endNodeType = nodeType
#            endNodeID = nodeID
#            relation_dict = {'startNodeName':startNodeName,
#                             'startNodeType':startNodeType,
#                             'startNodeID':startNodeID,
#                             'relation':relation,
#                             'endNodeName':endNodeName,
#                             'endNodeType':endNodeType,
#                             'endNodeID':endNodeID}
#            relation_list.append(relation_dict)
#        
#        shareholding_list = profile_dict['shareholding']
#        for i in range(0, len(shareholding_list)):
#            shareholding = shareholding_list[i]
#            startNodeName = nodeName
#            startNodeType = nodeType
#            startNodeID = nodeID            
#            relation = "shareholder"
#            endNodeName = shareholding['nodeName']
#            endNodeType = shareholding['nodeType']
#            endNodeID = shareholding['nodeID']
#            relation_dict = {'startNodeName':startNodeName,
#                             'startNodeType':startNodeType,
#                             'startNodeID':startNodeID,
#                             'relation':relation,
#                             'endNodeName':endNodeName,
#                             'endNodeType':endNodeType,
#                             'endNodeID':endNodeID}
#            relation_list.append(relation_dict)
#        
#        relatedCompany_list = profile_dict['relatedCompany']
#        for i in range(0, len(relatedCompany_list)):
#            relatedCompany = relatedCompany_list[i]
#            temp_nodeID = relatedCompany['nodeID']
#            temp_relation_list = session.write_transaction(neo4j_find_2nd_lv_relation_between_nodes, nodeID, temp_nodeID)
#            relation_list = relation_list + temp_relation_list        
#        relation_list = deduplicate_dict_list(relation_list)
#        
#        out_dict = {'nodeName':nodeName,
#                    'nodeType':nodeType,
#                    'nodeID':nodeID,
#                    'relation':relation_list}  
#    return out_dict
#
#def get_person_relation_graph_by_nodeID(nodeID):
#    with neo4j_driver.session() as session:
#        relation_list = []    
#        profile_dict = get_person_profile_by_nodeID(nodeID)
#        nodeName = profile_dict['nodeName']
#        nodeType = profile_dict['nodeType']
#        nodeID = int(nodeID)
#        
#        relatedCompany_list = profile_dict['relatedCompany']
#        for i in range(0, len(relatedCompany_list)):
#            relatedCompany = relatedCompany_list[i]
#            temp_nodeID = relatedCompany['nodeID']
#            temp_relation_list = session.write_transaction(neo4j_find_1st_lv_relation_between_nodes, nodeID, temp_nodeID)
#            relation_list = relation_list + temp_relation_list
#        
#        relatedAssociate_list = profile_dict['relatedAssociate']
#        for i in range(0, len(relatedAssociate_list)):
#            relatedAssociate = relatedAssociate_list[i]
#            temp_nodeID = relatedAssociate['nodeID']
#            temp_relation_list = session.write_transaction(neo4j_find_2nd_lv_relation_between_nodes, nodeID, temp_nodeID)
#            relation_list = relation_list + temp_relation_list
#
#        relation_list = deduplicate_dict_list(relation_list)
#        print (len(relation_list))
#        out_dict = {'nodeName':nodeName,
#                    'nodeType':nodeType,
#                    'nodeID':nodeID,
#                    'relation':relation_list}  
#    return out_dict    