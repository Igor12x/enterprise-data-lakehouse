# Skill 03: Regras Estritas de Databricks Spark SQL, dbt & Power BI

## 1. Materializacao Hibrida (`full` vs `incremental`) no dbt
- Todos os 13 modelos `.sql` em `dbt_project/models/` usam `materialized='incremental'`, `incremental_strategy='merge'`, `unique_key='<pk>'` e `file_format='delta'`.
- **Modo `full`:** O `FlowAneel` envia `dbt run --select <modelo> --full-refresh`. O `dbt-databricks` traduz o `SELECT` em **`CREATE OR REPLACE TABLE`**, recriando a tabela Delta do zero.
- **Modo `incremental`:** O `FlowAneel` omite `--full-refresh`. O `dbt-databricks` gera **`MERGE INTO`** usando a `unique_key`.
- **Deduplicacao Garantida na Prata:** Todos os modelos Prata derivados da Bronze usam `QUALIFY ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY _ingestion_at DESC) = 1` para impedir chaves duplicadas no `MERGE`.

## 2. Otimizacao Power BI (VertiPaq) & Dialeto Spark SQL
- **Surrogate Keys (`xxhash64`):** Na camada Ouro (`gd`), todas as chaves substitutas (`sk_*`) devem ser inteiros de 64 bits (`BIGINT`) gerados com `xxhash64()`. Jamais usar `UBIGINT` ou `hash()` de DuckDB.
- **Funcoes de Data Spark SQL:** Utilizar `DATE_FORMAT(col, 'yyyyMMdd')`, `EXPLODE(SEQUENCE(...))`, `DAYOFWEEK()`, `DAYOFYEAR()` e `ADD_MONTHS()`. Jamais usar `strftime` ou `generate_series`.
- **Concatenacao de Strings:** Proibido usar operador `+` para strings. Usar sempre `CONCAT()` ou `CONCAT_WS()` com `COALESCE()`.
- **Proibido SQL em Strings Python:** Toda logica de transformacao Prata e Ouro reside exclusivamente nos arquivos `.sql` do `dbt_project/`.
