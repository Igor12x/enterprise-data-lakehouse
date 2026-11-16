# Skill 04: Testes, Landing Zone IP Brasil & Orquestracao no Databricks Workflows

## 1. Testes Unitarios Locais (`tests/unit/`)
- Rodam 100% localmente sem exigir sessao Java/Spark ativa (`python -m pytest -v tests/unit/`).
- Validam:
  - `test_base_extractor.py`: Contrato abstrato `BaseExtractor`, metadados de auditoria, validacao de arquivo `.parquet` local e validacao de URI `abfss://` da Landing Zone.
  - `test_config.py`: Configuracoes, catalogo SSOT (`tempo`, `localidade`, `gd` e schema Bronze `aneel`), existencia dos 13 modelos `.sql` e dos 5 notebooks em `src/notebooks/`, separacao de responsabilidades (`DatabricksRuntime`, `DbtRunner`, `AdlsStorageConnector`, `DeltaCatalogManager`), verificacao de `requirements.txt` e contrato dinamico de `BaseNotebook`.

## 2. Testes de Integracao (`tests/integration/`)
- `tests/integration/test_integracao_conexoes.py` valida:
  1. Disponibilidade HTTP 200 e cabecalho binario `PAR1` da ANEEL.
  2. Autenticacao SharedKey no Azure ADLS Gen2 (`portfolioigordatalake`).
  3. Conectividade SQL/Spark com o Azure Databricks.

## 3. Orquestracao via Databricks Workflows (Jobs)
- Os 5 notebooks modulares em `src/notebooks/` possuem cabecalhos documentando suas dependencias no Job:
  1. `src/notebooks/bronze/aneel.py` (`bronze_aneel`) — Sem dependencias.
  2. `src/notebooks/prata/tempo.py` (`prata_tempo`) — Sem dependencias.
  3. `src/notebooks/prata/gd.py` (`prata_gd`) — Depende de `bronze_aneel`.
  4. `src/notebooks/prata/localidade.py` (`prata_localidade`) — Depende de `bronze_aneel` e `prata_gd` (pois `dim_localidade` faz JOIN com `dim_distribuidoras`).
  5. `src/notebooks/ouro/gd.py` (`ouro_gd`) — Depende de `prata_gd`, `prata_localidade` e `prata_tempo`.
