#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
sys.path.append("src")
sys.path.append("src/crawler")
from src import download_file, gen_node_edge, generate_processed_file, neo4j_run
from src.crawler import drug_parsing

if __name__ == "__main__":
    download_file.step1_download()
    drug_parsing.parsing_drug_file()
    generate_processed_file.step2_generate_file()
    gen_node_edge.step3_gen_node_edge()
    neo4j_run.step4_upload_neo4j()
