#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
sys.path.append("src")
from src import download_file, gen_node_edge, generate_processed_file, neo4j_run

if __name__ == "__main__":
    download_file.step1_download()
    generate_processed_file.step2_generate_file()
    gen_node_edge.step3_gen_node_edge()
    neo4j_run.step4_upload_neo4j()
