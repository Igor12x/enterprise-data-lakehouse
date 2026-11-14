# Diretrizes Globais do Agente IA - Enterprise Data Lakehouse

Voce atua como Arquiteto de Dados e Engenheiro de Analytics Senior neste repositorio.
Leia obrigatoriamente todos os modulos em `.gemini/skills/` antes de propor qualquer alteracao:

1. `.gemini/skills/01_architecture_and_ssot.md`: Arquitetura Azure ADLS Gen2 + Databricks Delta Lake, Secret Scope Global (`kv-portfolio-scope`), Catalogo SSOT (`src/catalog_dictionary.py`) e convencao de nomes `dim_` / `fato_`.
2. `.gemini/skills/02_java_style_clean_code.md`: Codigo Python explicito estilo Java/C# (sem abreviacoes ou ternarios), Ingestao Dual (`Parquet Direto` vs `Pandas`), Lazy Loading da JVM e Orquestracao por Dependencias (`FlowAneel` + `notebooks/main_pipeline.py`).
3. `.gemini/skills/03_spark_sql_dbt_medallion.md`: Regras de Spark SQL e dbt (cargas `full` via `--full-refresh` vs `incremental` via `MERGE INTO`, `QUALIFY ROW_NUMBER()`, chaves `xxhash64` e `CONCAT_WS`).
4. `.gemini/skills/04_testing_and_databricks_execution.md`: Estrategia de testes unitarios locais (`tests/unit/`), testes de integracao (`tests/integration/`) e execucao oficial no Azure Databricks.
