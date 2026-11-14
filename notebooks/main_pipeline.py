# Databricks notebook source
import os
import sys

# 1. Localiza a pasta raiz do projeto (onde fica a pasta 'src') e adiciona ao Python
diretorio_atual = os.path.abspath(".")
diretorio_pai = os.path.abspath("..")

if os.path.exists(os.path.join(diretorio_atual, "src")) is True:
    caminho_raiz = diretorio_atual
else:
    caminho_raiz = diretorio_pai

if caminho_raiz not in sys.path:
    sys.path.append(caminho_raiz)

# 2. Cria os filtros visuais (Widgets) no topo do Notebook do Databricks
dbutils.widgets.dropdown("modo_carga", "full", ["full", "incremental"], "1. Modo de Carga")
dbutils.widgets.dropdown("camada_alvo", "todas", ["bronze", "prata", "ouro", "todas"], "2. Camada Alvo")

modo_carga_selecionado = dbutils.widgets.get("modo_carga").strip().lower()
camada_selecionada = dbutils.widgets.get("camada_alvo").strip().lower()

# COMMAND ----------

from src.flows.flow_aneel import FlowAneel

# =============================================================================
# PONTO DE ENTRADA PRINCIPAL DA PIPELINE (SUBSTITUI O ANTIGO MAIN_INGEST)
# Ordem obrigatoria de dependencia:
#   1. BRONZE (1 tabela extraida da ANEEL):
#      - gd.aneel_gd_empreendimentos
#   2. PRATA (8 tabelas em ordem de hierarquia):
#      - tempo.dim_calendario -> localidade.dim_regioes -> localidade.dim_ufs
#      - gd.dim_distribuidoras -> gd.dim_classes_consumo -> localidade.dim_municipios
#      - localidade.dim_localidade (depende de dim_regioes, dim_ufs, dim_distribuidoras e dim_municipios)
#      - gd.fato_empreendimentos_prata (depende de dim_municipios, dim_distribuidoras e dim_classes_consumo)
#   3. OURO (5 tabelas do Star Schema Power BI - Dimensoes antes da Fato):
#      - gd.dim_enquadramento -> gd.dim_titular -> gd.dim_tecnologia -> gd.dim_modalidade
#      - gd.fato_empreendimentos (ultima tabela, depende de todas as dimensoes anteriores)
# =============================================================================
orquestrador = FlowAneel()

try:
    print("=================================================================")
    print(f"INICIANDO PIPELINE LAKEHOUSE | CAMADA: {camada_selecionada.upper()} | MODO: {modo_carga_selecionado.upper()}")
    print("=================================================================")

    if camada_selecionada == "bronze":
        resumo_execucao = orquestrador.run_bronze(modo_carga=modo_carga_selecionado)
    elif camada_selecionada == "prata":
        resumo_execucao = orquestrador.run_prata(modo_carga=modo_carga_selecionado)
    elif camada_selecionada == "ouro":
        resumo_execucao = orquestrador.run_ouro(modo_carga=modo_carga_selecionado)
    elif camada_selecionada == "todas":
        resumo_execucao = orquestrador.run(modo_carga=modo_carga_selecionado)
    else:
        raise ValueError(
            f"[Notebook] Camada '{camada_selecionada}' invalida. "
            f"Use 'bronze', 'prata', 'ouro' ou 'todas'."
        )

    print("\n[SUCESSO] Resumo das tabelas processadas na ordem de dependencia:")
    for nome_tabela, status_tabela in resumo_execucao.items():
        print(f"  - {nome_tabela}: {status_tabela}")

except Exception as erro_execucao:
    print(f"\n[ERRO CRITICO] Execucao interrompida para proteger dependencias: {str(erro_execucao)}")
    raise erro_execucao
