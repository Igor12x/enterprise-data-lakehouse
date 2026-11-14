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
fluxo.executar_tabela_bronze(chave_tabela='aneel_gd_empreendimentos', modo_carga=modo_carga_param)
