# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# ==============================================================================
# NOTEBOOK: BRONZE ANEEL
# CAMINHO NO JOB: src/notebooks/bronze/aneel.py
# DEPENDENCIAS NO JOB (Depende de): Sem dependencias (1a etapa de ingestao)
# TABELAS GERADAS:
#   1. portfolio-lakehouse-bronze.aneel.aneel_gd_empreendimentos
# ==============================================================================
import os
import sys

# 1. Localiza dinamicamente a pasta raiz do repositorio (onde fica 'src')
diretorio_busca = os.path.abspath(".")
while os.path.exists(os.path.join(diretorio_busca, "src")) is False:
    diretorio_pai = os.path.dirname(diretorio_busca)
    if diretorio_pai == diretorio_busca:
        break
    diretorio_busca = diretorio_pai

if diretorio_busca not in sys.path:
    sys.path.append(diretorio_busca)

from src.ingestion.extractors.aneel_gd_extractor import AneelGdExtractor
from src.notebooks.base_notebook import BaseNotebook

# 2. Instancia o contrato BaseNotebook (carrega as tabelas dinamicamente do catalog_dictionary.py)
extrator_aneel = AneelGdExtractor()
notebook = BaseNotebook(
    camada="bronze",
    dominio_ou_fonte="aneel",
    extrator_bronze=extrator_aneel
)
notebook.configurar_widgets(dbutils_obj=dbutils)

# COMMAND ----------

# 3. Executa a ingestao Bronze para 'todas' ou apenas para a tabela selecionada no Widget
notebook.executar_com_widgets(dbutils_obj=dbutils)