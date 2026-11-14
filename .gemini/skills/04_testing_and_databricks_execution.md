# Skill 04: Testes Unitarios, Testes de Integracao & Execucao no Databricks

## 1. Testes Unitarios Locais (`tests/unit/`)
- Devem rodar **100% no Windows local sem Java/Spark** via `python -m pytest -v tests/unit/`.
- Cobrem:
  - `test_base_extractor.py`: Contrato abstrato, metadados de auditoria e validacao de arquivo `.parquet` direto.
  - `test_config.py`: Configuracoes, catalogo SSOT dos 3 produtos sem prefixo, existencia dos 13 modelos `.sql` com `dim_`/`fato_`, uso obrigatorio de `xxhash64()` + `incremental_strategy='merge'`, integridade matematica de `FlowAneel.MAPA_DEPENDENCIAS`, montagem de `--full-refresh` e validacao de `modo_carga`.

## 2. Testes de Integracao (`tests/integration/`)
- O arquivo `tests/integration/test_integracao_conexoes.py` valida os servicos externos reais:
  1. Disponibilidade da URL oficial da ANEEL (`HTTP 200` + assinatura binaria `PAR1`).
  2. Autenticacao SharedKey no Azure ADLS Gen2 (`portfolioigordatalake` / container `portfolio-lakehouse-bronze`).
  3. Conectividade com o Azure Databricks (via `SparkSession` dentro do cluster ou `databricks-sql-connector` quando credenciais estiverem no `.env`).

## 3. Regra de Execucao Oficial (Sem Gambiarras Locais)
- A execucao real do pipeline (`notebooks/main_pipeline.py` e notebooks individuais em `notebooks/`) e feita **diretamente no Azure Databricks** via **Git Folders** no cluster `cluster-portfolio-small`.
- Nunca adicione fallbacks artificiais de `dbutils` ou conexoes SQL improvisadas no notebook principal apenas para contornar a falta de Spark local; mantenha o notebook limpo e nativo para o Databricks.
