# Skill 04: Testes, Ingestao Cloud-Native via Azure Function & Orquestracao no Databricks

## 1. Testes Unitarios Locais (`tests/unit/`)
- Rodam 100% localmente sem exigir sessao Java/Spark ativa (`python -m pytest -v tests/unit/`).
- Validam:
  - `test_base_extractor.py`: Contrato abstrato `BaseExtractor`, metadados de auditoria, validacao de arquivo `.parquet` local, validacao de URI `abfss://` da Landing Zone e obrigatoriedade do disparo da Azure Function (`disparar_sync_function_brasil`).
  - `test_config.py`: Configuracoes, catalogo SSOT (`tempo`, `localidade`, `gd` e schema Bronze `aneel`), existencia dos 13 modelos `.sql`, dos 5 notebooks em `src/notebooks/` e dos 4 arquivos em `azure_functions/aneel_landing_br/`, separacao de responsabilidades (`DatabricksRuntime`, `DbtRunner`, `AdlsStorageConnector`, `DeltaCatalogManager`), verificacao de `requirements.txt` e contrato dinamico de `BaseNotebook`.

## 2. Testes de Integracao (`tests/integration/`)
- `tests/integration/test_integracao_conexoes.py` valida:
  1. Disponibilidade HTTP 200 e cabecalho binario `PAR1` da ANEEL.
  2. Autenticacao SharedKey no Azure ADLS Gen2 (`portfolioigordatalake`).
  3. Conectividade SQL/Spark com o Azure Databricks.

## 3. Ingestao Cloud-Native contra Geo-Block (`func-lakehouse-ingestao-br` em `Brazil South`)
- Portais governamentais brasileiros (`dadosabertos.aneel.gov.br`) bloqueiam conexoes vindas de datacenters nos EUA (`East US 2`).
- Em toda execucao do notebook `src/notebooks/bronze/aneel.py` (`full` ou `incremental`), `AneelGdExtractor.extract_landing_brasil_uri()` aciona **obrigatoriamente** a Azure Function `func-lakehouse-ingestao-br` (`POST /api/sync_aneel_landing`, lida do segredo `aneel-br-function-url` no Key Vault `kv-portfolio-igor`).
- A Azure Function em `Brazil South` baixa o `.parquet` atualizado da ANEEL em streaming (blocos de 1 MB) e sobrescreve o arquivo unico `abfss://portfolio-lakehouse-bronze@portfolioigordatalake.dfs.core.windows.net/landing/aneel_gd/empreendimento-geracao-distribuida.parquet` em blocos de 4 MB antes de o Spark gravar a tabela Delta `portfolio-lakehouse-bronze.aneel.aneel_gd_empreendimentos`.

## 4. Orquestracao via Databricks Workflows (Jobs)
- Os 5 notebooks modulares em `src/notebooks/` possuem cabecalhos documentando suas dependencias no Job:
  1. `src/notebooks/bronze/aneel.py` (`bronze_aneel`) — Sem dependencias.
  2. `src/notebooks/prata/tempo.py` (`prata_tempo`) — Sem dependencias.
  3. `src/notebooks/prata/gd.py` (`prata_gd`) — Depende de `bronze_aneel`.
  4. `src/notebooks/prata/localidade.py` (`prata_localidade`) — Depende de `bronze_aneel` e `prata_gd` (pois `dim_localidade` faz JOIN com `dim_distribuidoras`).
  5. `src/notebooks/ouro/gd.py` (`ouro_gd`) — Depende de `prata_gd`, `prata_localidade` e `prata_tempo`.
