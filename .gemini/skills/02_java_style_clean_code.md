# Skill 02: Clean Code Explicito (Estilo Java/C#), Ingestao Dual & Orquestracao

## 1. Estilo de Codificacao (Perfil BI / Java / C#)
- **Proibido Abreviacoes e "Magicas" Pythonicas:** Nunca use operadores ternarios (`a if cond else b`), curto-circuitos (`x = a or b`) ou list comprehensions complexas. Escreva blocos `if / elif / else`, lacos `for` tradicionais e classes POJO/DTO com `__init__` explicito.
- **Nomes Descritivos e Didaticos:** Use nomes completos (`dataframe_bruto`, `contrato_tabela`, `modo_carga`) e comentarios que conectem os conceitos com Power BI, SQL e Java/C#.
- **Zero Codigo Morto ou Arquivos Vazios:** Mantenha apenas arquivos, pastas, metodos e imports ativamente utilizados.

## 2. Padrao Dual de Ingestao Bronze (Parquet Direto vs Pandas)
- **Opcao 1 — Parquet Direto (Padrao da ANEEL):** `run_parquet_direct()` -> `extract_parquet_file()` -> `write_parquet_direto_bronze()`. Baixa o `.parquet` em blocos de 1 MB para disco e lê nativamente no Spark (`spark.read.parquet()`), evitando conversao desnecessaria para Pandas em memoria RAM.
- **Opcao 2 — Via Pandas (Mantida para APIs JSON/CSV):** `run()` -> `extract()` -> `write_delta_bronze()`.
- **Logica Compartilhada:** Ambos convergem para `AdlsDatabricksConnector._salvar_dataframe_spark_na_bronze()` para executar `overwrite` (`full`) ou `MERGE INTO` (`incremental`).
- **Lazy Loading da JVM:** Mantenha `self._sessao_spark = None` no `__init__` do `AdlsDatabricksConnector` para que os testes unitarios locais rodem sem Java/Hadoop.

## 3. Orquestracao Central e Arvore de Dependencias
- **Ponto de Entrada Unico:** `notebooks/main_pipeline.py` (substitui o antigo `main_ingest.py`) recebe os widgets `modo_carga` (`full`/`incremental`) e `camada_alvo` (`bronze`/`prata`/`ouro`/`todas`).
- **Protecao de Dependencias (`FlowAneel`):** `ORDEM_TABELAS_PRATA`, `ORDEM_TABELAS_OURO` e `MAPA_DEPENDENCIAS` garantem com `validar_dependencias_tabela()` que nenhuma tabela filha seja gerada antes das tabelas mae existirem.
