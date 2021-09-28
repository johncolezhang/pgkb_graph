#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
download all file automatically, and parse update date from status file.
"""

import requests
import os
import zipfile
import shutil

def download_allele_definition():
    gene_list = [
        "CACNA1S", "CFTR", "CYP2B6", "CYP2C9", "CYP2C19", "CYP2D6", "CYP3A5",
        "CYP4F2", "DPYD", "G6PD", "IFNL3", "MT-RNR1", "NUDT15", "RYR1",
        "SLCO1B1", "TPMT", "UGT1A1", "VKORC1"
    ]

    allele_template_link = "https://api.pharmgkb.org/v1/download/file/attachment/{}_allele_definition_table.xlsx"

    link_list = [allele_template_link.format(x) for x in gene_list]

    # move older folder to bak folder, and delete old one.
    folder = "allele_definition"
    bak_folder = os.path.join("bak", folder)
    if os.path.isdir(folder):
        if os.path.isdir(bak_folder):
            shutil.rmtree(bak_folder)
        shutil.copytree(folder, bak_folder)
        shutil.rmtree(folder)
    os.mkdir(folder)

    for x in link_list:
        filename = x.split("/")[-1]
        content = requests.get(x).content

        with open(os.path.join(folder, filename), "wb") as f:
            f.write(content)


def download_clinical_annotation():
    clinical_annotation_link = "https://api.pharmgkb.org/v1/download/file/data/clinicalAnnotations.zip"
    filename = clinical_annotation_link.split("/")[-1]

    # move older folder to bak folder, and delete old one.
    folder = "clinical_annotation"
    bak_folder = os.path.join("bak", folder)
    if os.path.isdir(folder):
        if os.path.isdir(bak_folder):
            shutil.rmtree(bak_folder)
        shutil.copytree(folder, bak_folder)
        shutil.rmtree(folder)
    os.mkdir(folder)

    content = requests.get(clinical_annotation_link).content
    with open(os.path.join(folder, filename), "wb") as f:
        f.write(content)

    with zipfile.ZipFile(os.path.join(folder, filename), "r") as zip_ref:
        zip_ref.extractall(folder)

    os.remove(os.path.join(folder, filename))


def download_diplotype():
    gene_list = [
        "CYP2B6", "CYP2C9", "CYP2C19", "CYP2D6", "CYP3A5",
        "DPYD", "NUDT15", "SLCO1B1", "TPMT", "UGT1A1"
    ]

    diplotype_template_link = "https://api.pharmgkb.org/v1/download/file/attachment/{}_Diplotype_Phenotype_Table.xlsx"

    link_list = [diplotype_template_link.format(x) for x in gene_list]

    # move older folder to bak folder, and delete old one.
    folder = "diplotype"
    bak_folder = os.path.join("bak", folder)
    if os.path.isdir(folder):
        if os.path.isdir(bak_folder):
            shutil.rmtree(bak_folder)
        shutil.copytree(folder, bak_folder)
        shutil.rmtree(folder)
    os.mkdir(folder)

    for x in link_list:
        filename = x.split("/")[-1]
        content = requests.get(x).content

        with open(os.path.join(folder, filename), "wb") as f:
            f.write(content)


def download_drug_label():
    drug_label_link = "https://api.pharmgkb.org/v1/download/file/data/drugLabels.zip"
    filename = drug_label_link.split("/")[-1]

    # move older folder to bak folder, and delete old one.
    folder = "drug_label"
    bak_folder = os.path.join("bak", folder)
    if os.path.isdir(folder):
        if os.path.isdir(bak_folder):
            shutil.rmtree(bak_folder)
        shutil.copytree(folder, bak_folder)
        shutil.rmtree(folder)
    os.mkdir(folder)

    content = requests.get(drug_label_link).content
    with open(os.path.join(folder, filename), "wb") as f:
        f.write(content)

    with zipfile.ZipFile(os.path.join(folder, filename), "r") as zip_ref:
        zip_ref.extractall(folder)

    os.remove(os.path.join(folder, filename))


def download_frequency():
    gene_list = [
        "CACNA1S", "CYP2B6", "CYP2C9", "CYP2C19", "CYP2D6", "CYP3A5",
        "CYP4F2", "DPYD", "MT-RNR1", "NUDT15", "RYR1",
        "SLCO1B1", "TPMT", "UGT1A1", "VKORC1", "HLA-A", "HLA-B"
    ]

    frequency_template_link = "https://api.pharmgkb.org/v1/download/file/attachment/{}_frequency_table.xlsx"

    link_list = [frequency_template_link.format(x) for x in gene_list]

    # move older folder to bak folder, and delete old one.
    folder = "frequency"
    bak_folder = os.path.join("bak", folder)
    if os.path.isdir(folder):
        if os.path.isdir(bak_folder):
            shutil.rmtree(bak_folder)
        shutil.copytree(folder, bak_folder)
        shutil.rmtree(folder)
    os.mkdir(folder)

    for x in link_list:
        filename = x.split("/")[-1]

        content = requests.get(x).content

        if b"No such file" in content:
            print("filename {} download failed, use new link".format(filename))
            new_link = "https://api.pharmgkb.org/v1/download/file/attachment/{}_Allele_Frequency_Table.xlsx".format(
                filename.split("_")[0]
            )
            content = requests.get(new_link).content

        with open(os.path.join(folder, filename), "wb") as f:
            f.write(content)


def download_functionality():
    gene_list = [
        "CACNA1S", "CYP2B6", "CYP2C9", "CYP2C19", "CYP2D6", "CYP3A5",
        "DPYD", "MT-RNR1", "NUDT15", "RYR1",
        "SLCO1B1", "TPMT", "UGT1A1"
    ]

    func_template_link = "https://api.pharmgkb.org/v1/download/file/attachment/{}_allele_functionality_reference.xlsx"

    link_list = [func_template_link.format(x) for x in gene_list]

    # move older folder to bak folder, and delete old one.
    folder = "functionality"
    bak_folder = os.path.join("bak", folder)
    if os.path.isdir(folder):
        if os.path.isdir(bak_folder):
            shutil.rmtree(bak_folder)
        shutil.copytree(folder, bak_folder)
        shutil.rmtree(folder)
    os.mkdir(folder)

    for x in link_list:
        filename = x.split("/")[-1]

        content = requests.get(x).content

        if b"No such file" in content:
            print("filename {} download failed, use new link".format(filename))
            new_link = "https://api.pharmgkb.org/v1/download/file/attachment/{}_Allele_Functionality_Table.xlsx".format(
                filename.split("_")[0]
            )
            content = requests.get(new_link).content

        with open(os.path.join(folder, filename), "wb") as f:
            f.write(content)


def download_genes():
    genes_link = "https://api.pharmgkb.org/v1/download/file/data/genes.zip"
    filename = genes_link.split("/")[-1]

    # move older folder to bak folder, and delete old one.
    folder = "genes"
    bak_folder = os.path.join("bak", folder)
    if os.path.isdir(folder):
        if os.path.isdir(bak_folder):
            shutil.rmtree(bak_folder)
        shutil.copytree(folder, bak_folder)
        shutil.rmtree(folder)
    os.mkdir(folder)

    content = requests.get(genes_link).content
    with open(os.path.join(folder, filename), "wb") as f:
        f.write(content)

    with zipfile.ZipFile(os.path.join(folder, filename), "r") as zip_ref:
        zip_ref.extractall(folder)

    os.remove(os.path.join(folder, filename))


def download_guideline_annotation():
    genes_link = "https://api.pharmgkb.org/v1/download/file/data/dosingGuidelines.json.zip"
    filename = genes_link.split("/")[-1]

    # move older folder to bak folder, and delete old one.
    folder = "guideline_annotation"
    bak_folder = os.path.join("bak", folder)
    if os.path.isdir(folder):
        if os.path.isdir(bak_folder):
            shutil.rmtree(bak_folder)
        shutil.copytree(folder, bak_folder)
        shutil.rmtree(folder)
    os.mkdir(folder)

    content = requests.get(genes_link).content
    with open(os.path.join(folder, filename), "wb") as f:
        f.write(content)

    with zipfile.ZipFile(os.path.join(folder, filename), "r") as zip_ref:
        zip_ref.extractall(folder)

    os.remove(os.path.join(folder, filename))


def download_variant_annotation():
    genes_link = "https://api.pharmgkb.org/v1/download/file/data/variantAnnotations.zip"
    filename = genes_link.split("/")[-1]

    # move older folder to bak folder, and delete old one.
    folder = "variant_annotation"
    bak_folder = os.path.join("bak", folder)
    if os.path.isdir(folder):
        if os.path.isdir(bak_folder):
            shutil.rmtree(bak_folder)
        shutil.copytree(folder, bak_folder)
        shutil.rmtree(folder)
    os.mkdir(folder)

    content = requests.get(genes_link).content
    with open(os.path.join(folder, filename), "wb") as f:
        f.write(content)

    with zipfile.ZipFile(os.path.join(folder, filename), "r") as zip_ref:
        zip_ref.extractall(folder)

    os.remove(os.path.join(folder, filename))


def download_variants():
    genes_link = "https://api.pharmgkb.org/v1/download/file/data/variants.zip"
    filename = genes_link.split("/")[-1]

    # move older folder to bak folder, and delete old one.
    folder = "variants"
    bak_folder = os.path.join("bak", folder)
    if os.path.isdir(folder):
        if os.path.isdir(bak_folder):
            shutil.rmtree(bak_folder)
        shutil.copytree(folder, bak_folder)
        shutil.rmtree(folder)
    os.mkdir(folder)

    content = requests.get(genes_link).content
    with open(os.path.join(folder, filename), "wb") as f:
        f.write(content)

    with zipfile.ZipFile(os.path.join(folder, filename), "r") as zip_ref:
        zip_ref.extractall(folder)

    os.remove(os.path.join(folder, filename))


def download_chemical():
    chemical_link = "https://api.pharmgkb.org/v1/download/file/data/chemicals.zip"
    filename = chemical_link.split("/")[-1]

    # move older folder to bak folder, and delete old one.
    folder = "chemicals"
    bak_folder = os.path.join("bak", folder)
    if os.path.isdir(folder):
        if os.path.isdir(bak_folder):
            shutil.rmtree(bak_folder)
        shutil.copytree(folder, bak_folder)
        shutil.rmtree(folder)
    os.mkdir(folder)

    content = requests.get(chemical_link).content
    with open(os.path.join(folder, filename), "wb") as f:
        f.write(content)

    with zipfile.ZipFile(os.path.join(folder, filename), "r") as zip_ref:
        zip_ref.extractall(folder)

    os.remove(os.path.join(folder, filename))


def download_drug():
    drug_link = "https://api.pharmgkb.org/v1/download/file/data/drugs.zip"
    filename = drug_link.split("/")[-1]

    # move older folder to bak folder, and delete old one.
    folder = "drugs"
    bak_folder = os.path.join("bak", folder)
    if os.path.isdir(folder):
        if os.path.isdir(bak_folder):
            shutil.rmtree(bak_folder)
        shutil.copytree(folder, bak_folder)
        shutil.rmtree(folder)
    os.mkdir(folder)

    content = requests.get(drug_link).content
    with open(os.path.join(folder, filename), "wb") as f:
        f.write(content)

    with zipfile.ZipFile(os.path.join(folder, filename), "r") as zip_ref:
        zip_ref.extractall(folder)

    os.remove(os.path.join(folder, filename))


def step1_download():
    download_allele_definition()
    download_clinical_annotation()
    download_diplotype()
    download_drug_label()
    download_frequency()
    download_functionality()
    download_genes()
    download_guideline_annotation()
    download_variant_annotation()
    download_variants()
    download_chemical()
    download_drug()

if __name__ == "__main__":
    step1_download()
