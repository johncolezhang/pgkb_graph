#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from util.neo4j_util import neo4jUtil
import json

def step4_upload_neo4j():
    neo4j_host = "neo4j://172.16.231.80:17687"
    neo4j_user = "neo4j"
    neo4j_pwd = "123456"
    neo4j_database = "neo4j"

    neo_util = neo4jUtil(host=neo4j_host, user=neo4j_user, password=neo4j_pwd)


    print("Delete all nodes and relationships")
    neo_util.delete_all_neo4j(neo4j_database)

    print("Add nodes")
    with open("json/nodes.json", "r") as f:
        node_list = json.load(f)
    # node_list = list(filter(lambda x: x["label"][0] == "chemical", node_list))
    neo_util.add_node_to_neo4j(node_list, neo4j_database)

    with open("json/rs_position_nodes.json", "r") as f:
        node_list = json.load(f)
    neo_util.add_node_to_neo4j(node_list, neo4j_database)

    print("Add edges")
    with open("json/edges.json") as f:
        edge_list = json.load(f)
    neo_util.add_edge_to_neo4j(edge_list, neo4j_database)


if __name__ == "__main__":
    step4_upload_neo4j()
