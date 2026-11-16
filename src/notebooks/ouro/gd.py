# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# ==============================================================================
# NOTEBOOK: OURO GD (STAR SCHEMA POWER BI)
# CAMINHO NO JOB: src/notebooks/ouro/gd.py
# DEPENDENCIAS NO JOB (Depende de):
#   - prata_gd         (src/notebooks/prata/gd.py)         [Obrigatoria: fornece fato_empreendimentos_prata e dim_classes_consumo]
#   - prata_localidade (src/notebooks/prata/localidade.py) [Recomendada para fechar o Star Schema completo]
#   - prata_tempo      (src/notebooks/prata/tempo.py)      [Recomendada para fechar o Star Schema completo]
# TABELAS GERADAS (em ordem):
#   1. portfolio-lakehouse-ouro.gd.dim_enquadramento
#   2. portfolio-lakehouse-ouro.gd.dim_titular
#   3. portfolio-lakehouse-ouro.gd.dim_tecnologia
#   4. portfolio-lakehouse-ouro.gd.dim_modalidade
#   5. portfolio-lakehouse-ouro.gd.fato_empreendimentos
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

from src.notebooks.base_notebook import BaseNotebook

# 2. Instancia o contrato BaseNotebook (carrega as tabelas dinamicamente do catalog_dictionary.py)
notebook = BaseNotebook(
    camada="ouro",
    dominio_ou_fonte="gd"
)
notebook.configurar_widgets(dbutils_obj=dbutils)

# COMMAND ----------

# 3. Executa a transformacao Ouro de GD ('todas' ou a tabela selecionada no Widget)
notebook.executar_com_widgets(dbutils_obj=dbutils)