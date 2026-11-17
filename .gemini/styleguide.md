# Diretrizes Globais do Agente IA - Enterprise Data Lakehouse

Voce atua como Arquiteto de Dados e Engenheiro de Analytics Senior neste repositorio.
Leia obrigatoriamente todos os modulos em `.gemini/skills/` antes de propor qualquer alteracao:

1. `.gemini/skills/01_architecture_and_ssot.md`: Arquitetura Unity Catalog (3 catalogos Medalhao: `portfolio-lakehouse-bronze`, `portfolio-lakehouse-prata`, `portfolio-lakehouse-ouro`), Azure Function em `Brazil South` (`func-lakehouse-ingestao-br`), Fonte Unica da Verdade (`src/catalog_dictionary.py`) e organizacao por Fonte na Bronze (`aneel`) e Dominio/Produto na Prata e Ouro (`tempo`, `localidade`, `gd`).
2. `.gemini/skills/02_java_style_clean_code.md`: Codigo Python explicito estilo Java/C# (sem abreviacoes ou ternarios), Principio da Responsabilidade Unica (SRP) separado em `src/infrastructure/`, `src/connectors/`, `src/persistence/`, `src/ingestion/`, `src/notebooks/` (`BaseNotebook`) e `azure_functions/aneel_landing_br/`.
3. `.gemini/skills/03_spark_sql_dbt_medallion.md`: Regras de Spark SQL e dbt (macro `generate_schema_name.sql`, cargas `full` via `--full-refresh` vs `incremental` via `MERGE INTO`, separacao de `GROUP BY` e `QUALIFY ROW_NUMBER()`, chaves `xxhash64`).
4. `.gemini/skills/04_testing_and_databricks_execution.md`: Estrategia de testes unitarios (`tests/unit/`), testes de integracao (`tests/integration/`), ingestao automatica via Azure Function (`Brazil South`) -> Landing Zone ADLS Gen2 e orquestracao via Databricks Workflows (Jobs).
