import pandas as pd
from neo4j.v1 import GraphDatabase
import ast

def neo4j_create_node(tx, node_type, entity, attribute):
    cypher_stm = "MERGE (n:" + node_type + " {name:'" + entity + "'"
    for key in attribute.keys():
        cypher_stm = cypher_stm + ", " + key + ":" + "'" + str(attribute[key]) + "'"
    cypher_stm = cypher_stm + "})"
    tx.run(cypher_stm)

def neo4j_create_relation(tx, node1_type, entity1, node2_type, entity2, relation, attribute):
    cypher_stm1 = "MERGE (n1:" + node1_type + " {name:'" + entity1 + "'})"
    cypher_stm2 = "MERGE (n2:" + node2_type + " {name:'" + entity2 + "'})"
    
    if type(attribute) is dict and len(attribute) > 0:
        cypher_stm3 = "MERGE (n1)-[r:" + relation + " {"
        for key in attribute.keys():
            if (type(attribute[key]) == str):
                cypher_stm3 = cypher_stm3 + key + ":'" + str(attribute[key]) +"', "
            else:
                cypher_stm3 = cypher_stm3 + key + ":" + str(attribute[key]) +", "
        cypher_stm3 = cypher_stm3[:-2] + "}]->(n2)"
    else:
        cypher_stm3 = "MERGE (n1)-[r:" + relation + "]->(n2)"
    tx.run(cypher_stm1 + "\n" +
           cypher_stm2 + "\n" +
           cypher_stm3)

def neo4j_mod_rel_prop_dtype(tx, rel_prop, dtype):
    int_list = ['int', 'integer']
    flaot_list = ['float']
    str_list = ['str', 'string']
    dtype = dtype.lower().strip()
    if (dtype in int_list or dtype in flaot_list or dtype in str_list): 
        cypher_stm1 = "MATCH () -[r]- ()"
        cypher_stm2 = "WHERE exists(r." + rel_prop + ")"
        cypher_stm3 = "SET r." + rel_prop + " = "
        if (dtype in int_list):
            cypher_stm3 = cypher_stm3 + "toInteger(r." + rel_prop + ")"
        elif (dtype in flaot_list):
            cypher_stm3 = cypher_stm3 + "toFloat(r." + rel_prop + ")"
        elif (dtype in str_list):
            cypher_stm3 = cypher_stm3 + "toString(r." + rel_prop + ")"
        tx.run(cypher_stm1 + "\n" +
               cypher_stm2 + "\n" +
               cypher_stm3)

def neo4j_delete_all(tx):
    tx.run("MATCH (n)"
           "DETACH DELETE n")

def neo4j_get_node_id_by_name(tx, node_name):
    cypher_stm1 = "MATCH (n) WHERE n.name = '" + node_name + "'"
    cypher_stm2 = "RETURN ID(n)"
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2)
    result_list = []
    node_id = ""
    for record in cypher_results:
        result_list.append(str(record['ID(n)']))
    if len(result_list) > 0:
        node_id = result_list[0]
    return node_id

def neo4j_get_node_name_by_id(tx, node_id):
    cypher_stm1 = "MATCH (n) WHERE ID(n) = " + str(node_id)
    cypher_stm2 = "RETURN n.name"
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2)
    result_list = []
    node_name = ""
    for record in cypher_results:
        result_list.append(str(record['n.name']))
    if len(result_list) > 0:
        node_name = result_list[0]
    return node_name

def neo4j_get_node_names_by_type(tx, node_type):
    cypher_stm1 = "MATCH (n:" + node_type + ")"
    cypher_stm2 = "return DISTINCT n.name"
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2)
    result_list = []
    for record in cypher_results:
        result_list.append(record['n.name'])
    return result_list

def neo4j_get_node_id_by_type(tx, node_type):
    cypher_stm1 = "MATCH (n:" + node_type + ")"
    cypher_stm2 = "return DISTINCT ID(n)"
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2)
    result_list = []
    for record in cypher_results:
        result_list.append(record['ID(n)'])
    return result_list
    
def neo4j_get_entity_name_from_alias(tx, alias_name):
    cypher_stm1 = "MATCH (n1) -[:alias]-> (n2) WHERE n1.name = '" + alias_name + "'"
    cypher_stm2 = "RETURN n2.name"
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2)
    entity_name = alias_name
    result_list = []
    for record in cypher_results:
        result_list.append(record['n2.name'])
    if len(result_list) > 0:
        entity_name = result_list[0]
    return entity_name

def neo4j_find_relations_by_type(tx, rel_type, limit = 0):
    cypher_stm1 = "MATCH (n1) -[r]-> (n2)"
    cypher_stm2 = "WHERE TYPE(r) = '" + str(rel_type) +"'"
    cypher_stm3 = "RETURN n1.name, n2.name, LABELS(n1), LABELS(n2), properties(r)"
    if limit > 0:
        cypher_stm3 = cypher_stm3 + " LIMIT " + str(limit)
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2 + "\n" +
                            cypher_stm3)
    col_list = ['relationship_number',
                'entity_1',
                'entity_1_type',
                'entity_2',
                'entity_2_type',
                'relation',
                'attribute']
    rel_df = pd.DataFrame(columns = col_list)
    rel_num = 1
    for record in cypher_results:
        entity_1 = record['n1.name']
        entity_1_type_list = record['LABELS(n1)']
        entity_1_type =entity_1_type_list[0]
        entity_2 = record['n2.name']
        entity_2_type_list = record['LABELS(n2)']
        entity_2_type =entity_2_type_list[0]
        attribute = record['properties(r)']
        temp_df = pd.DataFrame([[rel_num,
                                 entity_1,
                                 entity_1_type,
                                 entity_2,
                                 entity_2_type,
                                 rel_type,
                                 attribute]],
                                columns = col_list)
        rel_df = rel_df.append(temp_df, ignore_index=True)
        rel_num = rel_num + 1
    return rel_df

def neo4j_find_self_loops(tx, limit = 0):
    cypher_stm1 = "MATCH (n1) -[r]-> (n1)"
    cypher_stm2 = "RETURN n1.name, LABELS(n1), properties(r),  TYPE(r)"
    if limit > 0:
        cypher_stm2 = cypher_stm2 + " LIMIT " + str(limit)
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2)
    col_list = ['relationship_number',
                'entity_1',
                'entity_1_type',
                'entity_2',
                'entity_2_type',
                'relation',
                'attribute']
    rel_df = pd.DataFrame(columns = col_list)
    rel_num = 1
    for record in cypher_results:
        entity_1 = record['n1.name']
        entity_1_type_list = record['LABELS(n1)']
        entity_1_type =entity_1_type_list[0]
        entity_2 = record['n1.name']
        entity_2_type_list = record['LABELS(n1)']
        entity_2_type = entity_2_type_list[0]
        rel_type = record['TYPE(r)']
        attribute = record['properties(r)']
        temp_df = pd.DataFrame([[rel_num,
                                 entity_1,
                                 entity_1_type,
                                 entity_2,
                                 entity_2_type,
                                 rel_type,
                                 attribute]],
                                columns = col_list)
        rel_df = rel_df.append(temp_df, ignore_index=True)
        rel_num = rel_num + 1
    return rel_df

def neo4j_remove_duplicated_relations(tx):
    cypher_stm1 = "MATCH (n1) -[r1]-> (n2)"
    cypher_stm2 = "MATCH (n1) -[r2]-> (n2)"
    cypher_stm3 = "WHERE TYPE(r1) = TYPE(r2) and ID(n1) <> ID(n2) and ID(r1) <> ID(r2)"
    cypher_stm4 = "RETURN ID(n1), ID(n2), ID(r1), TYPE(r1), ID(r2)"
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2 + "\n" +
                            cypher_stm3 + "\n" +
                            cypher_stm4)
    dup_rel_dict = {}
    for record in cypher_results:
        n1_id = int(record['ID(n1)'])
        n2_id = int(record['ID(n2)'])
        r1_id = int(record['ID(r1)'])
        r2_id = int(record['ID(r2)'])
        r1_type = record['TYPE(r1)']
#        print (r1_type)
        dup_rel_key = str(n1_id) + "_:_" + r1_type + "_:_" + str(n2_id)
        r_id_set = set()
        if dup_rel_key in dup_rel_dict.keys():
            r_id_set = dup_rel_dict[dup_rel_key]
        r_id_set.add(r1_id)
        r_id_set.add(r2_id)
        dup_rel_dict[dup_rel_key] = r_id_set
    for dup_rel_key in dup_rel_dict.keys():
        r_id_set = dup_rel_dict[dup_rel_key]
        for r_id in r_id_set:
            if r_id != max(r_id_set):
                cypher_stm1 = "MATCH () -[r]-> () WHERE ID(r) = " + str(r_id)
                cypher_stm2 = "DELETE r"
                tx.run(cypher_stm1 + "\n" +
                       cypher_stm2)

def neo4j_make_alias_relations(tx, master_node_name, alias_node_name):
    master_node_exist = False
    alias_node_exist = False
    
    master_node_id = ""
    alias_node_id = ""
    
    cypher_stm1 = "MATCH (n) WHERE n.name = '" + master_node_name + "'"
    cypher_stm2 = "RETURN ID(n)"
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2)
    result_list = []
    for record in cypher_results:
        result_list.append(str(record['ID(n)']))
    if len(result_list) > 0:
        master_node_exist = True
        master_node_id = result_list[0]
        
    cypher_stm1 = "MATCH (n) WHERE n.name = '" + alias_node_name + "'"
    cypher_stm2 = "RETURN ID(n)"
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2)
    result_list = []
    for record in cypher_results:
        result_list.append(str(record['ID(n)']))
    if len(result_list) > 0:
        alias_node_exist = True
        alias_node_id = result_list[0]
    if (master_node_exist and alias_node_exist and master_node_id != alias_node_id):
        cypher_stm1 = "MATCH (n1) WHERE ID(n1) = " + str(master_node_id)
        cypher_stm2 = "MATCH (n2) WHERE ID(n2) = " + str(alias_node_id)
        cypher_stm3 = "MERGE (n2) -[:alias]-> (n1)"
        tx.run(cypher_stm1 + "\n" +
               cypher_stm2 + "\n" +
               cypher_stm3)

def neo4j_transfer_relations(tx, keep_node_id, remove_node_id):
    keep_node_exist = False
    remove_node_exist = False
    
    if (keep_node_id != remove_node_id):
    #    Check if nodes exist        
        cypher_stm1 = "MATCH (n) WHERE ID(n) = " + str(keep_node_id)
        cypher_stm2 = "RETURN ID(n)"
        cypher_results = tx.run(cypher_stm1 + "\n" +
                                cypher_stm2)
        result_list = []
        for record in cypher_results:
            result_list.append(str(record['ID(n)']))
        if len(result_list) > 0:
            keep_node_exist = True
        cypher_stm1 = "MATCH (n) WHERE ID(n) = " + str(remove_node_id)
        cypher_stm2 = "RETURN ID(n)"
        cypher_results = tx.run(cypher_stm1 + "\n" +
                                cypher_stm2)
        result_list = []
        for record in cypher_results:
            result_list.append(str(record['ID(n)']))
        if len(result_list) > 0:
            remove_node_exist = True
        if (keep_node_exist and remove_node_exist):            
        #   transfer node relationships
            cypher_stm1 = "MATCH (n1) <-[r]- ()"
            cypher_stm2 = "WHERE ID(n1) = "+ str(remove_node_id)
            cypher_stm3 = "RETURN DISTINCT TYPE(r)"
            cypher_results = tx.run(cypher_stm1 + "\n" +
                                    cypher_stm2 + "\n" +
                                    cypher_stm3)
            for record in cypher_results:
                relation = str(record['TYPE(r)'])
                cypher_stm1 = "MATCH (n1) <-[r:" + relation + "]- (b) WHERE ID(n1) = " + str(remove_node_id)
                cypher_stm2 = "MATCH (n2) WHERE ID(n2) = " + str(keep_node_id)
                cypher_stm3 = "WITH n1, r, n2, b"
                cypher_stm4 = "MERGE (n2) <-[x:" + relation + "]- (b)"
                cypher_stm5 = "SET x = r"
                tx.run(cypher_stm1 + "\n" +
                       cypher_stm2 + "\n" +
                       cypher_stm3 + "\n" +
                       cypher_stm4 + "\n" +
                       cypher_stm5)
            cypher_stm1 = "MATCH (n1) -[r]-> ()"
            cypher_stm2 = "WHERE ID(n1) = "+ str(remove_node_id)
            cypher_stm3 = "RETURN DISTINCT TYPE(r)"
            cypher_results = tx.run(cypher_stm1 + "\n" +
                                    cypher_stm2 + "\n" +
                                    cypher_stm3)
            for record in cypher_results:
                relation = str(record['TYPE(r)'])
                cypher_stm1 = "MATCH (n1) -[r:" + relation + "]-> (b) WHERE ID(n1) = " + str(remove_node_id)
                cypher_stm2 = "MATCH (n2) WHERE ID(n2) = " + str(keep_node_id)
                cypher_stm3 = "WITH n1, r, n2, b"
                cypher_stm4 = "MERGE (n2) -[x:" + relation + "]-> (b)"
                cypher_stm5 = "SET x = r"
                tx.run(cypher_stm1 + "\n" +
                       cypher_stm2 + "\n" +
                       cypher_stm3 + "\n" +
                       cypher_stm4 + "\n" +
                       cypher_stm5)
        #   delete remove_node relationships
            cypher_stm1 = "MATCH (n1) -[r]- () WHERE ID(n1) = " + str(remove_node_id)
            cypher_stm2 = "DELETE r"
            tx.run(cypher_stm1 + "\n" +
                   cypher_stm2)            

def neo4j_arrange_alias_cluster(tx, master_id):
    cypher_stm1 = "MATCH shortestpath((n1) -[:alias*]- (n2)) WHERE n1 <> n2 AND ID(n1) = " + str(master_id)
    cypher_stm2 = "RETURN DISTINCT ID(n2)"
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2)
    master_id = int(master_id)
    alias_id_set = set()
    for record in cypher_results:
        alias_id = record['ID(n2)']
        if (alias_id != master_id):
            alias_id_set.add(alias_id)
    for alias_id in alias_id_set:        
        cypher_stm1 = "MATCH (n1) <-[r]- (n2) WHERE ID(n1) = " + str(alias_id)
        cypher_stm2 = "RETURN ID(n2), TYPE(r), ID(r)"
        cypher_results = tx.run(cypher_stm1 + "\n" +
                                cypher_stm2)
        for record in cypher_results:
            n2_id = record['ID(n2)']
            relation = record['TYPE(r)']
            r_id = record['ID(r)']

            if relation != 'alias':
                cypher_stm1= 'MATCH (n1), (n2)'
                cypher_stm2 = 'WHERE ID(n1) = ' + str(master_id) + ' AND ID(n2) = ' + str(n2_id)
                cypher_stm3 = 'CREATE (n1) <-[r:' + relation + ']- (n2)'
                cypher_stm4 = 'RETURN ID(r)'
                cypher_results2 = tx.run(cypher_stm1 + "\n" +
                                         cypher_stm2 + "\n" +
                                         cypher_stm3 + "\n" +
                                         cypher_stm4)
                for record2 in cypher_results2:
                    r_id2 = record2['ID(r)']
      
                cypher_stm1 = 'MATCH ()<-[r1]-(), ()<-[r2]-()'
                cypher_stm2 = 'WHERE ID(r1) = ' + str(r_id) + ' AND ID(r2) = ' + str(r_id2)
                cypher_stm3 = 'SET r2 = r1'
                tx.run(cypher_stm1 + "\n" +
                       cypher_stm2 + "\n" +
                       cypher_stm3)
                
                cypher_stm1 = 'MATCH ()<-[r1]-()'
                cypher_stm2 = 'WHERE ID(r1) = ' + str(r_id)
                cypher_stm3 = 'DELETE r1'
                tx.run(cypher_stm1 + "\n" +
                       cypher_stm2 + "\n" +
                       cypher_stm3)
                
        cypher_stm1 = "MATCH (n1) -[r]-> (n2) WHERE ID(n1) = " + str(alias_id)
        cypher_stm2 = "RETURN ID(n2), TYPE(r), ID(r)"
        cypher_results = tx.run(cypher_stm1 + "\n" +
                                cypher_stm2)
        for record in cypher_results:
            n2_id = record['ID(n2)']
            relation = record['TYPE(r)']
            r_id = record['ID(r)']

            if relation != 'alias':
                cypher_stm1= 'MATCH (n1), (n2)'
                cypher_stm2 = 'WHERE ID(n1) = ' + str(master_id) + ' AND ID(n2) = ' + str(n2_id)
                cypher_stm3 = 'CREATE (n1) -[r:' + relation + ']-> (n2)'
                cypher_stm4 = 'RETURN ID(r)'
                cypher_results2 = tx.run(cypher_stm1 + "\n" +
                                         cypher_stm2 + "\n" +
                                         cypher_stm3 + "\n" +
                                         cypher_stm4)
                for record2 in cypher_results2:
                    r_id2 = record2['ID(r)']
      
                cypher_stm1 = 'MATCH ()-[r1]->(), ()-[r2]->()'
                cypher_stm2 = 'WHERE ID(r1) = ' + str(r_id) + ' AND ID(r2) = ' + str(r_id2)
                cypher_stm3 = 'SET r2 = r1'
                tx.run(cypher_stm1 + "\n" +
                       cypher_stm2 + "\n" +
                       cypher_stm3)
                
                cypher_stm1 = 'MATCH ()-[r1]->()'
                cypher_stm2 = 'WHERE ID(r1) = ' + str(r_id)
                cypher_stm3 = 'DELETE r1'
                tx.run(cypher_stm1 + "\n" +
                       cypher_stm2 + "\n" +
                       cypher_stm3)
        
        cypher_stm1 = "MATCH (n1) -[r:alias]- () WHERE ID(n1) = " + str(alias_id)
        cypher_stm2 = "DELETE r"
        tx.run(cypher_stm1 + "\n" +
               cypher_stm2)

        cypher_stm1 = "MATCH (n1), (n2)"
        cypher_stm2 = "WHERE ID(n1) = " + str(alias_id) + " AND ID(n2) = " + str(master_id)
        cypher_stm3 = "MERGE (n1) -[:alias]-> (n2)"
        tx.run(cypher_stm1 + "\n" +
               cypher_stm2 + "\n" +
               cypher_stm3)
       
def neo4j_merge_relations_by_type(tx, merge_rel_list, new_rel):
    for i in range(len(merge_rel_list)-1,-1,-1):
        merge_rel = merge_rel_list[i]
        cypher_stm1 = 'MATCH (n1)-[r1:' + merge_rel + ']->(n2)'
        cypher_stm2 = 'MERGE (n1)-[r2:' + new_rel + ']->(n2)'
        cypher_stm3 = 'SET r2 = r1'
        if merge_rel != new_rel:
            cypher_stm3 = cypher_stm3  + "\n" + 'DELETE r1'
        tx.run(cypher_stm1 + "\n" +
               cypher_stm2 + "\n" +
               cypher_stm3)

def neo4j_remove_relations_by_type(tx, rel_type):
    cypher_stm1 = 'MATCH () -[r:' +  rel_type + ']-> ()'
    cypher_stm2 = 'DELETE r'
    tx.run(cypher_stm1 + "\n" +
           cypher_stm2)

def neo4j_find_cycle_nodes(tx, start_node_id, relation):
    cypher_stm1 = 'MATCH (n1) -[:' + relation + '*]-> (n2) -[:' + relation + '*]-> (n1)'
    cypher_stm2 = 'WHERE ID(n2) = ' + str(start_node_id)
    cypher_stm3 = 'RETURN ID(n1)'
    cycle_node_id_set = set()
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2 + "\n" +
                            cypher_stm3)
    for record in cypher_results:
        node_id = record['ID(n1)']
        cycle_node_id_set.add(node_id)
    if (len(cycle_node_id_set) > 0):
        cycle_node_id_set.add(start_node_id)
    return cycle_node_id_set

def neo4j_add_node_label(tx, node_id, label):
    alias_node_id_set = set()
    cypher_stm1 = 'MATCH shortestpath((n1) -[:alias*]- (n2))'
    cypher_stm2 = 'WHERE n1 <> n2 AND ID(n1) = ' + str(node_id)
    cypher_stm3 = 'RETURN ID(n2)'
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2 + "\n" +
                            cypher_stm3)
    for record in cypher_results:
        alias_node_id = record['ID(n2)']
        alias_node_id_set.add(alias_node_id)
    alias_node_id_set.add(node_id)
    for alias_node_id in alias_node_id_set:
        cypher_stm1 = 'MATCH (n1) WHERE ID(n1) = ' + str(alias_node_id)
        cypher_stm2 = 'SET n1:' + str(label)
        tx.run(cypher_stm1 + "\n" +
               cypher_stm2)

#    cypher_results = tx.run(cypher_stm1 + "\n" +
#                            cypher_stm2)
#    relationships_list = []
#    for record in cypher_results:
#        relationships  = record['p'].relationships
#        relationships_list.append(relationships)
#    for relation in relationships_list[4]:
#        print (relation.nodes[0])
#        print (relation.nodes[1])
#        print ()
        
def neo4j_check_if_master_node(tx, node_id):
    ID = ""
    cypher_stm1 = 'MATCH (n1) -[:alias]-> (n2) WHERE ID(n1) = ' + str(node_id)
    cypher_stm2 = 'RETURN ID(n2)'
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2)
    for record in cypher_results:
        ID = record['ID(n2)']
    return ID

def neo4j_add_properties_to_node(tx, node_id, property_dict):
    alias_node_id_set = set()
    cypher_stm1 = 'MATCH shortestpath((n1) -[:alias*]- (n2))'
    cypher_stm2 = 'WHERE n1 <> n2 AND ID(n1) = ' + str(node_id)
    cypher_stm3 = 'RETURN ID(n2)'
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2 + "\n" +
                            cypher_stm3)
    for record in cypher_results:
        alias_node_id = record['ID(n2)']
        alias_node_id_set.add(alias_node_id)
    alias_node_id_set.add(node_id)
    
    for alias_node_id in alias_node_id_set:
        cypher_stm1 = 'MATCH (n1) WHERE ID(n1) = ' + str(alias_node_id)
        cypher_stm2 = 'SET n1 += {'
        property_key_list = list(property_dict.keys())
        for i in range(0, len(property_key_list)):
            property_key = property_key_list[i]
            property_value = property_dict[property_key]
            cypher_stm2 = cypher_stm2 + property_key + ':'
            if type(property_value) is str:
                cypher_stm2 = cypher_stm2 + "'" + property_value + "',"
            else:
                cypher_stm2 = cypher_stm2 + str(property_value) + ","
        cypher_stm2 = cypher_stm2[:-1] + '}'
        tx.run(cypher_stm1 + "\n" +
               cypher_stm2)
    
def neo4j_remove_orphan_nodes(tx):
    cypher_stm1 = 'MATCH (n) WHERE (NOT (n)-[]-()) DETACH DELETE n'
    tx.run(cypher_stm1)
    
def neo4j_get_node_properties_by_id(tx, node_id):
    cypher_stm1 = 'MATCH (n) WHERE ID(n) = ' + str(node_id)
    cypher_stm2 = 'RETURN properties(n)'
    cypher_results = tx.run(cypher_stm1 + "\n" +
                            cypher_stm2)
    for record in cypher_results:
        result = record['properties(n)']
    return (result)
    