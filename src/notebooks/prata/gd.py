# Databricks notebook source
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
    dominio_ou_fonte="gd"
)
notebook.configurar_widgets(dbutils_obj=dbutils)

# COMMAND ----------

# 3. Executa a transformacao Prata de GD ('todas' ou a tabela selecionada no Widget)
notebook.executar_com_widgets(dbutils_obj=dbutils)
