# Databricks notebook source
# ==============================================================================
# NOTEBOOK: PRATA LOCALIDADE
# CAMINHO NO JOB: src/notebooks/prata/localidade.py
# DEPENDENCIAS NO JOB (Depende de):
#   - bronze_aneel (src/notebooks/bronze/aneel.py)
#   - prata_gd     (src/notebooks/prata/gd.py) -> pois dim_localidade faz JOIN com dim_distribuidoras
# TABELAS GERADAS (em ordem):
#   1. portfolio-lakehouse-prata.localidade.dim_regioes
#   2. portfolio-lakehouse-prata.localidade.dim_ufs
#   3. portfolio-lakehouse-prata.localidade.dim_municipios
#   4. portfolio-lakehouse-prata.localidade.dim_localidade
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
    camada="prata",
    dominio_ou_fonte="localidade"
)
notebook.configurar_widgets(dbutils_obj=dbutils)

# COMMAND ----------

# 3. Executa a transformacao Prata de Localidade ('todas' ou a tabela selecionada no Widget)
notebook.executar_com_widgets(dbutils_obj=dbutils)
