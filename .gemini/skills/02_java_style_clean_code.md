# Skill 02: Clean Code Explicito (Estilo Java/C#), SRP & BaseNotebook

## 1. Estilo de Codificacao (Perfil BI / Java / C#)
- **Proibido Abreviacoes e "Magicas" Pythonicas:** Nunca use operadores ternarios (`a if cond else b`), curto-circuitos (`x = a or b`) ou list comprehensions complexas. Escreva blocos `if / elif / else`, lacos `for` tradicionais e classes POJO/DTO com `__init__` explicito.
- **Zero Codigo Morto ou Pastas Duplicadas:** Nao existe pasta `notebooks/` fora de `src/` nem pasta `src/flows/`. Toda a execucao do Databricks fica unificada em `src/notebooks/`.

## 2. Separacao Estrita por Responsabilidade Unica (SRP)
1. **`src/infrastructure/databricks_runtime.py` (`DatabricksRuntime`):**
   - Gerencia `SparkSession` (Lazy Loading), valida e instala pacotes ausentes do `requirements.txt` (`garantir_dependencias_requirements`) e auto-descobre `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH` e `DATABRICKS_TOKEN` em tempo de execucao.
2. **`src/infrastructure/dbt_runner.py` (`DbtRunner`):**
   - Monta e executa comandos do dbt via `[sys.executable, "-m", "dbt.cli.main", "run", ...]`.
3. **`src/connectors/adls_storage_connector.py` (`AdlsStorageConnector`):**
   - Assinatura HTTP SharedKey (HMAC-SHA256) e upload REST em blocos de 4 MB (`Put Block` / `Put Block List`) para a Landing Zone do ADLS Gen2.
4. **`src/persistence/delta_catalog_manager.py` (`DeltaCatalogManager`):**
   - DDL no Unity Catalog (`CREATE SCHEMA IF NOT EXISTS`, `CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION` com auto-recuperacao caso a pasta fisica tenha sido apagada no ADLS) e escrita Delta na Bronze (`full` Overwrite ou `incremental` `MERGE INTO`).
5. **`src/ingestion/` (`BaseExtractor` e `AneelGdExtractor`) + `azure_functions/aneel_landing_br/`:**
   - Suporta 3 modos separados de ingestao Bronze:
     - Tradicional via Pandas (`run()` -> `extract()` -> `write_delta_bronze()`)
     - Tradicional via Parquet Local Direto (`run_parquet_direct()` -> `extract_parquet_file()` -> `write_parquet_direto_bronze()`)
     - Opcao Cloud-Native para fontes com restricao de IP no Brasil (`run_landing_brasil()` -> `extract_landing_brasil_uri()` -> `disparar_sync_function_brasil()` -> `write_parquet_landing_brasil_bronze()`). Dispara **obrigatoriamente** a Azure Function em `Brazil South` em toda execucao (`full` ou `incremental`), impedindo atualizacoes falsas com arquivos antigos do ADLS.

## 3. Contrato de Notebooks (`src/notebooks/base_notebook.py`)
- Todos os notebooks em `src/notebooks/<camada>/<produto>.py` instanciam `BaseNotebook`.
- `BaseNotebook` garante o `requirements.txt`, carrega dinamicamente as tabelas do `catalog_dictionary.py`, cria os Widgets (`modo_carga` e `tabela_alvo`: `'todas'` + tabelas do catalogo), valida `validar_modo_carga()` e `validar_selecao_tabela()` e executa a carga.
