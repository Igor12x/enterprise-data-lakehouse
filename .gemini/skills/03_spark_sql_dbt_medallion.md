# Skill 03: Regras Estritas de Databricks Spark SQL, dbt & Power BI

## 1. Schemas Exatos no dbt (`generate_schema_name.sql`)
- A macro `dbt_project/macros/generate_schema_name.sql` sobrescreve o comportamento padrao do dbt para garantir que os schemas criados no Unity Catalog sejam exatamente `gd`, `localidade` e `tempo` (impedindo a concatenacao `gd_gd`, `gd_localidade` ou `gd_tempo`).

## 2. Materializacao Hibrida (`full` vs `incremental`) no dbt
- Todos os 13 modelos `.sql` em `dbt_project/models/` usam `materialized='incremental'`, `incremental_strategy='merge'`, `unique_key='<pk>'` e `file_format='delta'`.
- **Modo `full`:** O `DbtRunner` envia `--full-refresh` (`CREATE OR REPLACE TABLE`).
- **Modo `incremental`:** O `DbtRunner` omite `--full-refresh` (`MERGE INTO` pela `unique_key`).
- **Regra Spark SQL para `GROUP BY` + `QUALIFY`:** No Databricks Spark SQL, nunca use colunas nao agrupadas ou funcoes de agregacao diretamente dentro do `QUALIFY` na mesma query do `GROUP BY`. Separe o `GROUP BY` em uma CTE anterior e aplique `QUALIFY ROW_NUMBER() OVER (...) = 1` na CTE seguinte (como em `dim_municipios.sql`).

## 3. Otimizacao Power BI (VertiPaq) & Dialeto Spark SQL
- **Surrogate Keys (`xxhash64`):** Na camada Ouro (`gd`), todas as chaves substitutas (`sk_*`) sao inteiros de 64 bits (`BIGINT`) gerados com `xxhash64()`.
- **Funcoes Spark SQL:** Utilizar `DATE_FORMAT(col, 'yyyyMMdd')`, `EXPLODE(SEQUENCE(...))`, `DAYOFWEEK()`, `DAYOFYEAR()`, `ADD_MONTHS()`, `CONCAT()` e `CONCAT_WS()` com `COALESCE()`.
- **Proibido SQL em Strings Python para Transformacao:** Toda logica de negocio Prata e Ouro reside exclusivamente nos arquivos `.sql` de `dbt_project/models/`.
