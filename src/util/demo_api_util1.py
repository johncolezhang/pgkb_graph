#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import re

non_ascii_re = r'[^\x00-\x7F]+'

def clean_str(temp_string):
    temp_string = temp_string.replace('\u2019', "'")
    temp_string = temp_string.replace('&#39;', "'")
    temp_string = temp_string.replace('&lt;', "<")
    temp_string = temp_string.replace('&gt;', ">")  
    temp_string = re.sub(non_ascii_re,' ', temp_string)
    temp_string = temp_string.replace(' {2,}', " ")
    temp_string = temp_string.strip()
    return temp_string

def neo4j_check_node_type(tx, nodeID):
    label_list = []
    cypher_stm = "MATCH (n) " + \
                  "WHERE ID(n) = " + str(nodeID) + " " + \
                  "RETURN labels(n)"
    cypher_results = tx.run(cypher_stm)
    for record in cypher_results:
        label_list = record['labels(n)']
    return label_list

def neo4j_find_relation_degree(tx, relaton_type, direction = 'none'):
    if (direction.lower() == 'in'):
        cypher_stm1 = 'MATCH (n1) <-[r:' +  relaton_type + ']- (n2)'
    elif (direction.lower() == 'out'):
        cypher_stm1 = 'MATCH (n1) -[r:' +  relaton_type + ']-> (n2)'
    else:
        cypher_stm1 = 'MATCH (n1) -[r:' +  relaton_type + ']- (n2)'
    cypher_stm2 = 'RETURN n1.name, count(r)'
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2)
    degree_dict = {}
    for record in cypher_results:
        name = record['n1.name']
        count_r = record['count(r)']
        degree_dict[name] = count_r
    key_list = list(degree_dict.keys())
    value_list = list(degree_dict.values())
    degree_df = pd.DataFrame({'node':key_list, 'degree':value_list})
    temp_df = pd.DataFrame([["***--Place holder--***",
                             0]], columns = ['node', 'degree'])
    degree_df = degree_df.append(temp_df, ignore_index=True)
    return degree_df

def neo4j_get_node_properties_by_type(tx, node_type):
    property_dict = {}
    cypher_stm1 = "MATCH (n:" + node_type + ")"
    cypher_stm2 = "RETURN n.name, properties(n)" 
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2)
    col_list = ['node',
                'common_node']
    node_df = pd.DataFrame(columns = col_list)
    for record in cypher_results:
        n_name = record['n.name']
        n_property = record['properties(n)']
        property_dict[n_name] = n_property
    
    key_list = list(property_dict.keys())
    value_list = list(property_dict.values())
    node_df = pd.DataFrame({'node':key_list, 'property':value_list})
    return node_df

def neo4j_find_common_relation_node(tx, nodeID, node_type, relaton_type, direction = 'none'):
    if (direction.lower() == 'in'):
        cypher_stm1 = "MATCH p = (n1) <-[:" + relaton_type + "]- (n2) -[:" + relaton_type + "]-> (n3:" + node_type + ")"
    elif (direction.lower() == 'out'):
        cypher_stm1 = "MATCH p = (n1) -[:" + relaton_type + "]-> (n2) <-[:" + relaton_type + "]- (n3:" + node_type + ")"
    else:
        cypher_stm1 = "MATCH p = (n1) -[:" + relaton_type + "]- (n2) -[:" + relaton_type + "]- (n3:" + node_type + ")"
    cypher_stm2 = "WHERE ID(n1) = " + str(nodeID) + " and n1 <> n3"
    cypher_stm3 = "RETURN DISTINCT n2.name, n3.name"
    
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2 + "\n" +
                            cypher_stm3)
    col_list = ['node',
                'common_node']
    node_df = pd.DataFrame(columns = col_list)
    temp_df = pd.DataFrame([["***--Place holder--***",
                             "***--Place holder--***"]], columns = col_list)
    node_df = node_df.append(temp_df, ignore_index=True)
    
    for record in cypher_results:
        n2_name = record['n2.name']
        n3_name = record['n3.name']
        temp_df = pd.DataFrame([[n3_name,
                                 n2_name]], columns = col_list)
        node_df = node_df.append(temp_df, ignore_index=True)
    return node_df

def neo4j_find_2nd_lv_reach_node(tx, nodeID, node_type, relaton_type, direction = 'none'):
    if (direction.lower() == 'forward'):
        cypher_stm1 = "MATCH p = (n1) -[:" + relaton_type + "]-> (n2) -[:" + relaton_type + "]-> (n3:" + node_type + ")"
    elif (direction.lower() == 'backward'):
        cypher_stm1 = "MATCH p = (n1) <-[:" + relaton_type + "]- (n2) <-[:" + relaton_type + "]- (n3:" + node_type + ")"
    else:
        cypher_stm1 = "MATCH p = (n1) -[:" + relaton_type + "]- (n2) -[:" + relaton_type + "]- (n3:" + node_type + ")"
    cypher_stm2 = "WHERE ID(n1) = " + str(nodeID) + " and n1 <> n3"
    cypher_stm3 = "RETURN DISTINCT n2.name, n3.name"
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2 + "\n" +
                            cypher_stm3)
    col_list = ['node',
                'common_node']
    node_df = pd.DataFrame(columns = col_list)
    temp_df = pd.DataFrame([["***--Place holder--***",
                             "***--Place holder--***"]], columns = col_list)
    node_df = node_df.append(temp_df, ignore_index=True)
    
    for record in cypher_results:
        n2_name = record['n2.name']
        n3_name = record['n3.name']
        temp_df = pd.DataFrame([[n3_name,
                                 n2_name]], columns = col_list)
        node_df = node_df.append(temp_df, ignore_index=True)
    return node_df

def neo4j_get_node_names_by_type(tx, node_type):
    cypher_stm1 = "MATCH (n:" + node_type + ")"
    cypher_stm2 = "return DISTINCT n.name"
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2)
    result_list = []
    for record in cypher_results:
        result_list.append(record['n.name'])
    return result_list

def neo4j_find_connected_nodes(tx, pattern, node_1_type, node_2_type):
    connected_node_list = []
    cypher_stm1 = "MATCH (sTaRt_NoDe:" + node_1_type + ")" + pattern + "(EnD_NoDe:" + node_2_type + ")"
    cypher_stm2 = "WHERE sTaRt_NoDe <> EnD_NoDe"
    cypher_stm3 = "RETURN DISTINCT sTaRt_NoDe.name, EnD_NoDe.name"
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2 + "\n" +
                            cypher_stm3)
    
    for record in cypher_results:
        start_node = record['sTaRt_NoDe.name']
        end_node = record['EnD_NoDe.name']
        connected_nodes = (start_node, end_node)
        connected_node_list.append(connected_nodes)
    return connected_node_list

######################################################################################
    
def neo4j_get_node_info_by_nodeName(tx, nodeName):
    cypher_stm = "MATCH (n) WHERE n.name = '" + nodeName + "' " + \
                 "RETURN ID(n), labels(n)"
    cypher_results = tx.run(cypher_stm)
    nodeName = clean_str(nodeName)
    for record in cypher_results:
        nodeID = int(record['ID(n)'])
        nodeType = str(record['labels(n)'])
        if ('listed_company' in nodeType):
            nodeType = 'listed_company'
        elif ('company' in nodeType):
            nodeType = 'company'
        elif ('person' in nodeType):
            nodeType = 'person'
        node_info = {'nodeName':nodeName,
                     'nodeType':nodeType,
                     'nodeID':nodeID}
    return node_info

def neo4j_get_company_info_by_nodeID(tx, nodeID):
    cypher_stm = "MATCH (n) " + \
                 "WHERE ID(n) = " + str(nodeID) + " " + \
                 "RETURN n.name, labels(n), n.stock_code, n.company_url"
    cypher_results = tx.run(cypher_stm)
    for record in cypher_results:
        nodeName = str(record['n.name'])
        nodeName = clean_str(nodeName)
        nodeType = str(record['labels(n)'])
        stock_code = str(record['n.stock_code'])
        company_url = str(record['n.company_url'])
    if ('listed_company' in nodeType):
        nodeType = 'listed_company'
    elif ('company' in nodeType):
        nodeType = 'company'
    elif ('person' in nodeType):
        nodeType = 'person'
    nodeID = int(nodeID)
    node_info = {'nodeName':nodeName,
                 'nodeType':nodeType,
                 'nodeID':nodeID}
    if nodeType == 'listed_company':
        node_info['stock_code'] = int(stock_code)
        node_info['company_url'] = company_url
    return node_info

def neo4j_get_person_info_by_nodeID(tx, nodeID):
    cypher_stm = "MATCH (n) " + \
                 "WHERE ID(n) = " + str(nodeID) + " " + \
                 "RETURN n.name, labels(n)"
    cypher_results = tx.run(cypher_stm)
    for record in cypher_results:
        nodeName = str(record['n.name'])
        nodeName = clean_str(nodeName)
        nodeType = str(record['labels(n)'])
    if ('listed_company' in nodeType):
        nodeType = 'listed_company'
    elif ('company' in nodeType):
        nodeType = 'company'
    elif ('person' in nodeType):
        nodeType = 'person'
    nodeID = int(nodeID)
    node_info = {'nodeName':nodeName,
                 'nodeType':nodeType,
                 'nodeID':nodeID}
    return node_info

def neo4j_get_company_relation_by_nodeID(tx, nodeID):   
    cypher_stm = "MATCH (n1) <-[r:board_member]- (n2) " + \
                 "WHERE ID(n1) = " + str(nodeID) + " " + \
                 "RETURN n2.name, r.role, ID(n2)"
    cypher_results = tx.run(cypher_stm)
    board_member_list = []
    for record in cypher_results:
        temp_nodeName = str(record['n2.name'])
        temp_nodeName = clean_str(temp_nodeName)
        temp_role = str(record['r.role'])
        temp_role = clean_str(temp_role)
        temp_nodeID = int(record['ID(n2)'])
        board_member = {'nodeName':temp_nodeName,
                        'role':temp_role,
                        'nodeID':temp_nodeID}
        board_member_list.append(board_member)
        
#    cypher_stm = "MATCH (n1) <-[r:company_secretary]- (n2)" + \
#                 "WHERE ID(n1) = " + str(nodeID) + " " + \
#                 "RETURN n2.name, ID(n2)"
#    cypher_results = tx.run(cypher_stm)
#    company_secretary_list = []
#    for record in cypher_results:
#        temp_nodeName = str(record['n2.name'])
#        temp_nodeID = int(record['ID(n2)'])
#        company_secretary = {'nodeName':temp_nodeName,
#                             'nodeID':temp_nodeID}
#        company_secretary_list.append(company_secretary)
    
    cypher_stm = "MATCH (n1) -[r:industry]- (n2) " + \
                 "WHERE ID(n1) = " + str(nodeID) + " " + \
                 "RETURN n2.name"
    cypher_results = tx.run(cypher_stm)
    industry_list = []
    for record in cypher_results:
        temp_industry = str(record['n2.name'])
        industry_list.append(temp_industry)
    
    cypher_stm = "MATCH (n1) <-[r:shareholder]- (n2) " + \
                 "WHERE ID(n1) = " + str(nodeID) + " " + \
                 "RETURN n2.name, labels(n2), ID(n2)"
    cypher_results = tx.run(cypher_stm)
    
    shareholder_list = []
    for record in cypher_results:
        temp_nodeName= str(record['n2.name'])
        temp_nodeName = clean_str(temp_nodeName)
        temp_nodeType = str(record['labels(n2)'])
        if ('listed_company' in temp_nodeType):
            temp_nodeType = 'listed_company'
        elif ('company' in temp_nodeType):
            temp_nodeType = 'company'
        elif ('person' in temp_nodeType):
            temp_nodeType = 'person'
        temp_nodeID = int(record['ID(n2)'])
        shareholder = {'nodeName':temp_nodeName,
                       'nodeType':temp_nodeType,
                       'nodeID':temp_nodeID}
        shareholder_list.append(shareholder)
    
    cypher_stm = "MATCH (n1) -[r:shareholder]-> (n2) " + \
                 "WHERE ID(n1) = " + str(nodeID) + " " + \
                 "RETURN n2.name, labels(n2), ID(n2)"
    cypher_results = tx.run(cypher_stm) 
    
    shareholding_list = []
    for record in cypher_results:
        temp_nodeName= str(record['n2.name'])
        temp_nodeName = clean_str(temp_nodeName)
        temp_nodeType = str(record['labels(n2)'])
        if ('listed_company' in temp_nodeType):
            temp_nodeType = 'listed_company'
        elif ('company' in temp_nodeType):
            temp_nodeType = 'company'
        elif ('person' in temp_nodeType):
            temp_nodeType = 'person'
        temp_nodeID = int(record['ID(n2)'])
        shareholding = {'nodeName':temp_nodeName,
                       'nodeType':temp_nodeType,
                       'nodeID':temp_nodeID}
        shareholding_list.append(shareholding)
    node_relation = {'industry':industry_list,
                     'board_member':board_member_list,
#                     'company_secretary':company_secretary_list,
                     'shareholder':shareholder_list,
                     'shareholding':shareholding_list}
    return node_relation

def neo4j_get_listed_company_news_by_nodeID(tx, nodeID):
    cypher_stm = "MATCH (n1) " + \
                 "WHERE ID(n1) = " + str(nodeID) + " " + \
                 "RETURN n1.name"
    cypher_results = tx.run(cypher_stm)
    for record in cypher_results:
        temp_relatedCompany = str(record['n1.name'])
        temp_relatedCompany = clean_str(temp_relatedCompany)
        
    cypher_stm = "MATCH (n1) <-[r]- (n2:news) " + \
                 "WHERE ID(n1) = " + str(nodeID) + " " + \
                 "RETURN n2.name, type(r), ID(n2), n2.publish_date, n2.source, n2.link"
    cypher_results = tx.run(cypher_stm)
    news_list = []
    for record in cypher_results:
        temp_newsTitle = str(record['n2.name'])
        temp_newsTitle = clean_str(temp_newsTitle)
        temp_newsType = str(record['type(r)'])
        temp_nodeID = int(record['ID(n2)'])
        temp_publishDate = int(record['n2.publish_date'])
        temp_source = str(record['n2.source'])
        temp_link = str(record['n2.link'])
        news = {'newsTitle':temp_newsTitle,
                'newsType':temp_newsType,
                'nodeID':temp_nodeID,
                'publishDate':temp_publishDate,
                'source':temp_source,
                'relatedCompany':temp_relatedCompany,
                'link':temp_link
                }
        news_list.append(news)
    relatedNews_dict = {'relatedNews':news_list}
    return relatedNews_dict

def neo4j_get_company_news_by_nodeID(tx, nodeID):
    cypher_stm = "MATCH (n1) -- (n2:company) <-[r]- (n3:news) " + \
                 "WHERE ID(n1) = " + str(nodeID) + " " + \
                 "RETURN n3.name, n2.name, type(r), ID(n3), n3.publish_date, n3.source, n3.link"
    cypher_results = tx.run(cypher_stm)
    news_list = []
    for record in cypher_results:
        temp_newsTitle = str(record['n3.name'])
        temp_newsTitle = clean_str(temp_newsTitle)
        temp_newsType = str(record['type(r)'])
        temp_nodeID = int(record['ID(n3)'])
        temp_publishDate = int(record['n3.publish_date'])
        temp_source = str(record['n3.source'])
        temp_relatedCompany = str(record['n2.name'])
        temp_relatedCompany = clean_str(temp_relatedCompany)
        temp_link = str(record['n3.link'])
        news = {'newsTitle':temp_newsTitle,
                'newsType':temp_newsType,
                'nodeID':temp_nodeID,
                'publishDate':temp_publishDate,
                'source':temp_source,
                'relatedCompany':temp_relatedCompany,
                'link':temp_link
                }
        news_list.append(news)
    relatedNews_dict = {'relatedNews':news_list}   
    return relatedNews_dict

def neo4j_get_person_relation_by_nodeID(tx, nodeID):
    cypher_stm = "MATCH (n1) -[r:board_member]-> (n2) " + \
                 "WHERE ID(n1) = " + str(nodeID) + " " + \
                 "RETURN n2.name, r.role, ID(n2)"
    cypher_results = tx.run(cypher_stm)
    relatedCompany_list = []
    for record in cypher_results:
        temp_nodeName = str(record['n2.name'])
        temp_nodeName = clean_str(temp_nodeName)
        temp_role = str(record['r.role'])
        temp_role = clean_str(temp_role)
        temp_nodeID = int(record['ID(n2)'])
        relatedCompany = {'nodeName':temp_nodeName,
                        'role':temp_role,
                        'nodeID':temp_nodeID}
        relatedCompany_list.append(relatedCompany)
    node_relation = {'relatedCompany':relatedCompany_list}
    
#    cypher_stm = "MATCH (n1) -[r:company_secretary]-> (n2)" + \
#                 "WHERE ID(n1) = " + str(nodeID) + " " + \
#                 "RETURN n2.name, r.role, ID(n2)"
#    cypher_results = tx.run(cypher_stm)
#    relatedCompany_list = []
#    for record in cypher_results:
#        temp_nodeName = str(record['n2.name'])
#        temp_role = str(record['r.role'])
#        temp_nodeID = int(record['ID(n2)'])
#        relatedCompany = {'nodeName':temp_nodeName,
#                        'role':'Company Secretary',
#                        'nodeID':temp_nodeID}
#        relatedCompany_list.append(relatedCompany)
        
    cypher_stm = "MATCH (n1) -[r:shareholder]-> (n2) " + \
                 "WHERE ID(n1) = " + str(nodeID) + " " + \
                 "RETURN n2.name, r.role, ID(n2)"
    cypher_results = tx.run(cypher_stm)
    for record in cypher_results:
        temp_nodeName = str(record['n2.name'])
        temp_nodeName = clean_str(temp_nodeName)
        temp_role = str(record['r.role'])
        temp_nodeID = int(record['ID(n2)'])
        relatedCompany = {'nodeName':temp_nodeName,
                          'role':'Shareholder',
                          'nodeID':temp_nodeID}
        relatedCompany_list.append(relatedCompany)
    node_relation = {'relatedCompany':relatedCompany_list}
    return node_relation
        
def neo4j_get_person_news_by_nodeID(tx, nodeID):
    cypher_stm = "MATCH (n1) -- (n2:company) <-[r]- (n3:news) " + \
                 "WHERE ID(n1) = " + str(nodeID) + " " + \
                 "RETURN n3.name, n2.name, type(r), ID(n3), n3.publish_date, n3.source, n3.link"
    cypher_results = tx.run(cypher_stm)
    news_list = []
    for record in cypher_results:
        temp_newsTitle = str(record['n3.name'])
        temp_newsTitle = clean_str(temp_newsTitle)
        temp_newsType = str(record['type(r)'])
        temp_nodeID = int(record['ID(n3)'])
        temp_publishDate = int(record['n3.publish_date'])
        temp_source = str(record['n3.source'])
        temp_relatedCompany = str(record['n2.name'])
        temp_relatedCompany = clean_str(temp_relatedCompany)
        temp_link = str(record['n3.link'])
        news = {'newsTitle':temp_newsTitle,
                'newsType':temp_newsType,
                'nodeID':temp_nodeID,
                'publishDate':temp_publishDate,
                'source':temp_source,
                'relatedCompany':temp_relatedCompany,
                'link':temp_link
                }
        news_list.append(news)
    relatedNews_dict = {'relatedNews':news_list}   
    return relatedNews_dict

def neo4j_find_1st_lv_relation_between_nodes(tx, nodeID_1, nodeID_2):
    cypher_stm = "MATCH (n1) -[r]- (n2)" + \
                 "WHERE ID(n1) = " + str(nodeID_1) + " and ID(n2) = " + str(nodeID_2) + " " + \
                 "RETURN startnode(r).name, labels(startnode(r)), ID(startnode(r)), type(r), endnode(r).name, labels(endnode(r)), ID(endnode(r))"
    cypher_results = tx.run(cypher_stm)
    relation_list = []
    for record in cypher_results:
        temp_startNodeName = str(record['startnode(r).name'])
        temp_startNodeName = clean_str(temp_startNodeName)
        temp_startNodeType = str(record['labels(startnode(r))'])
        temp_startNodeID = int(record['ID(startnode(r))'])
        relation = str(record['type(r)'])
        temp_endNodeName = str(record['endnode(r).name'])
        temp_endNodeName = clean_str(temp_endNodeName)
        temp_endNodeType = str(record['labels(endnode(r))'])
        temp_endNodeID = int(record['ID(endnode(r))'])
        
        if ('listed_company' in temp_startNodeType):
            temp_startNodeType = 'listed_company'
        elif ('company' in temp_startNodeType):
            temp_startNodeType = 'company'
        elif ('person' in temp_startNodeType):
            temp_startNodeType = 'person'
            
        if ('listed_company' in temp_endNodeType):
            temp_endNodeType = 'listed_company'
        elif ('company' in temp_endNodeType):
            temp_endNodeType = 'company'
        elif ('person' in temp_endNodeType):
            temp_endNodeType = 'person'
            
        relation_dict = {'startNodeName':temp_startNodeName,
                         'startNodeType':temp_startNodeType,
                         'startNodeID':temp_startNodeID,
                         'relation':relation,
                         'endNodeName':temp_endNodeName,
                         'endNodeType':temp_endNodeType,
                         'endNodeID':temp_endNodeID}
        relation_list.append(relation_dict)
    return relation_list

def neo4j_find_2nd_lv_relation_between_nodes(tx, nodeID_1, nodeID_2):
    cypher_stm = "MATCH (n1) -[r1]- (n) -[r2]- (n2) " + \
                 "WHERE ID(n1) = " + str(nodeID_1) + " and ID(n2) = " + str(nodeID_2) + " " + \
                 "and (n:person or n:company) " + \
                 "RETURN startnode(r1).name, labels(startnode(r1)), ID(startnode(r1)), type(r1), endnode(r1).name, labels(endnode(r1)), ID(endnode(r1)), " +\
                 "startnode(r2).name, labels(startnode(r2)), ID(startnode(r2)), type(r2), endnode(r2).name, labels(endnode(r2)), ID(endnode(r2))"
    cypher_results = tx.run(cypher_stm)
    relation_list = []
    
    for record in cypher_results:
        temp_startNodeName = str(record['startnode(r1).name'])
        temp_startNodeName = clean_str(temp_startNodeName)
        temp_startNodeType = str(record['labels(startnode(r1))'])
        temp_startNodeID = int(record['ID(startnode(r1))'])
        relation = str(record['type(r1)'])
        temp_endNodeName = str(record['endnode(r1).name'])
        temp_endNodeName = clean_str(temp_endNodeName)
        temp_endNodeType = str(record['labels(endnode(r1))'])
        temp_endNodeID = int(record['ID(endnode(r1))'])
        
        if ('listed_company' in temp_startNodeType):
            temp_startNodeType = 'listed_company'
        elif ('company' in temp_startNodeType):
            temp_startNodeType = 'company'
        elif ('person' in temp_startNodeType):
            temp_startNodeType = 'person'
            
        if ('listed_company' in temp_endNodeType):
            temp_endNodeType = 'listed_company'
        elif ('company' in temp_endNodeType):
            temp_endNodeType = 'company'
        elif ('person' in temp_endNodeType):
            temp_endNodeType = 'person'
        relation_dict = {'startNodeName':temp_startNodeName,
                         'startNodeType':temp_startNodeType,
                         'startNodeID':temp_startNodeID,
                         'relation':relation,
                         'endNodeName':temp_endNodeName,
                         'endNodeType':temp_endNodeType,
                         'endNodeID':temp_endNodeID}
        relation_list.append(relation_dict)
        
        temp_startNodeName = str(record['startnode(r2).name'])
        temp_startNodeName = clean_str(temp_startNodeName)
        temp_startNodeType = str(record['labels(startnode(r2))'])
        temp_startNodeID = int(record['ID(startnode(r2))'])
        relation = str(record['type(r2)'])
        temp_endNodeName = str(record['endnode(r2).name'])
        temp_endNodeName = clean_str(temp_endNodeName)
        temp_endNodeType = str(record['labels(endnode(r2))'])
        temp_endNodeID = int(record['ID(endnode(r2))'])
        
        if ('listed_company' in temp_startNodeType):
            temp_startNodeType = 'listed_company'
        elif ('company' in temp_startNodeType):
            temp_startNodeType = 'company'
        elif ('person' in temp_startNodeType):
            temp_startNodeType = 'person'
            
        if ('listed_company' in temp_endNodeType):
            temp_endNodeType = 'listed_company'
        elif ('company' in temp_endNodeType):
            temp_endNodeType = 'company'
        elif ('person' in temp_endNodeType):
            temp_endNodeType = 'person'
        relation_dict = {'startNodeName':temp_startNodeName,
                         'startNodeType':temp_startNodeType,
                         'startNodeID':temp_startNodeID,
                         'relation':relation,
                         'endNodeName':temp_endNodeName,
                         'endNodeType':temp_endNodeType,
                         'endNodeID':temp_endNodeID}
        relation_list.append(relation_dict)
    return relation_list

def neo4j_get_relations_between_nodes(tx, nodeID_1, nodeID_2, numStep):
    cypher_stm = "MATCH p = (n1) -[step:shareholder|:board_member*0.." + str(numStep) + "]- (n2) " + \
                 "WHERE ID(n1) = " + str(nodeID_1) + " and ID(n2) = " + str(nodeID_2) + " " + \
                 "WITH [s IN step] AS STEPS " + \
                 "UNWIND STEPS AS r " + \
                 "RETURN DISTINCT startnode(r).name, labels(startnode(r)), ID(startnode(r)), type(r), endnode(r).name, labels(endnode(r)), ID(endnode(r))"
    cypher_results = tx.run(cypher_stm)
    relation_list = []

    for record in cypher_results:
        temp_startNodeName = str(record['startnode(r).name'])
        temp_startNodeName = clean_str(temp_startNodeName)
        temp_startNodeType = str(record['labels(startnode(r))'])
        temp_startNodeID = int(record['ID(startnode(r))'])
        relation = str(record['type(r)'])
        temp_endNodeName = str(record['endnode(r).name'])
        temp_endNodeName = clean_str(temp_endNodeName)
        temp_endNodeType = str(record['labels(endnode(r))'])
        temp_endNodeID = int(record['ID(endnode(r))'])
        
        if ('listed_company' in temp_startNodeType):
            temp_startNodeType = 'listed_company'
        elif ('company' in temp_startNodeType):
            temp_startNodeType = 'company'
        elif ('person' in temp_startNodeType):
            temp_startNodeType = 'person'
            
        if ('listed_company' in temp_endNodeType):
            temp_endNodeType = 'listed_company'
        elif ('company' in temp_endNodeType):
            temp_endNodeType = 'company'
        elif ('person' in temp_endNodeType):
            temp_endNodeType = 'person'
            
        relation_dict = {'startNodeName':temp_startNodeName,
                         'startNodeType':temp_startNodeType,
                         'startNodeID':temp_startNodeID,
                         'relation':relation,
                         'endNodeName':temp_endNodeName,
                         'endNodeType':temp_endNodeType,
                         'endNodeID':temp_endNodeID}
        relation_list.append(relation_dict)
    return relation_list

def neo4j_get_all_node_id_by_labels(tx, label_list):
    cypher_stm = "MATCH (n) WHERE" 
    for i in range(0, len(label_list)):
        cypher_stm = cypher_stm + " n:" + label_list[i] + " or"
    cypher_stm = cypher_stm[:-3]
    cypher_stm = cypher_stm + " RETURN ID(n), n.name, labels(n)"
    cypher_results = tx.run(cypher_stm)
    
#    out_df = pd.DataFrame(columns = ['nodeID', 'nodeName','nodeType'])
    nodeID_list = []
    nodeName_list = []
    nodeType_list = []
    for record in cypher_results:
        nodeID = record['ID(n)']
        nodeName = record['n.name']
        nodeType = record['labels(n)']
        if ('listed_company' in nodeType):
            nodeType = 'listed_company'
        elif ('company' in nodeType):
            nodeType = 'company'
        elif ('person' in nodeType):
            nodeType = 'person'
        
        nodeID_list.append(nodeID)
        nodeName_list.append(nodeName)
        nodeType_list.append(nodeType)
#        temp_df = pd.DataFrame([[nodeID, nodeName, nodeType]], columns = ['nodeID', 'nodeName', 'nodeType'])
#        out_df = out_df.append(temp_df, ignore_index = True)

    node_id_df = pd.DataFrame.from_dict({'nodeID':nodeID_list,
                                         'nodeName':nodeName_list,
                                         'nodeType':nodeType_list})
    return node_id_df