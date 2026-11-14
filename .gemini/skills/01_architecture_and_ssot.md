# Skill 01: Arquitetura Cloud Azure, Catalogo SSOT & Padrao de Nomes

## 1. Infraestrutura Azure & FinOps
- **Storage Account (ADLS Gen2):** `portfolioigordatalake` com 3 containers fisicos (`portfolio-lakehouse-bronze`, `portfolio-lakehouse-silver`, `portfolio-lakehouse-gold`).
- **Workspace & Cluster Databricks:** `portfolio-prod-databricks` (Standard, `No Public IP = No` para custo zero ocioso) e cluster `cluster-portfolio-small` (Single Node, Runtime 15.4 LTS, auto-terminate 10 min).
- **Seguranca Global:** Secret Scope `kv-portfolio-scope` (`adls-storage-key`) vinculado ao Azure Key Vault `kv-portfolio-igor`. O `src/config.py` le `.env` localmente e faz fallback automatico via `dbutils.secrets.get` no Databricks.

## 2. Schemas Sem Prefixo & Tabelas com Prefixos `dim_` / `fato_`
- **Schemas (`hive_metastore`):** Nao usam prefixos (`bronze_`, `prata_`, `ouro_`). Os nomes dos schemas sao os 3 Produtos de Dados: `tempo`, `localidade` e `gd`.
- **Separação Fisica:** Definida em `src/config.py` (Bronze) e via `+location_root` em `dbt_project/dbt_project.yml` (Prata e Ouro).
- **Fonte Unica da ANEEL -> Multiplas Pratas e Ouros:** Extrai apenas 1 tabela bruta na Bronze (`aneel_gd_empreendimentos`) que alimenta todo o modelo dimensional:
  - **Bronze (`gd`):** `aneel_gd_empreendimentos`
  - **Prata (`tempo`):** `dim_calendario`
  - **Prata (`localidade`):** `dim_regioes`, `dim_ufs`, `dim_municipios`, `dim_localidade`
  - **Prata (`gd`):** `dim_distribuidoras`, `dim_classes_consumo`, `fato_empreendimentos_prata`
  - **Ouro (`gd`):** `dim_enquadramento`, `dim_titular`, `dim_tecnologia`, `dim_modalidade`, `fato_empreendimentos`

## 3. Unica Fonte da Verdade (SSOT): `src/catalog_dictionary.py`
- Todo novo produto ou tabela deve ser registrado em `DATA_PRODUCTS_CATALOG` (`DataProductSpec` / `TableContract`).
- Use `get_data_product("<produto>")` ou `obter_contrato_por_tabela("<tabela>")` para resolver schemas, chaves primarias e caminhos ADLS dinamicamente.
