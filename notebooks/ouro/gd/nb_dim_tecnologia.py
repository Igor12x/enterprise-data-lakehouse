# Databricks notebook source
import sys
import os

caminho_raiz = os.path.abspath('../../..')
if caminho_raiz not in sys.path:
    sys.path.append(caminho_raiz)

dbutils.widgets.dropdown('modo_carga', 'full', ['full', 'incremental'], 'Modo de Carga')
modo_carga_param = dbutils.widgets.get('modo_carga').strip().lower()

from src.flows.flow_aneel import FlowAneel

fluxo = FlowAneel()
fluxo.executar_modelo_dbt(nome_modelo_ou_pasta='dim_tecnologia', modo_carga=modo_carga_param)
