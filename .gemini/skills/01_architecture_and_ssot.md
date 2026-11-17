# Skill 01: Arquitetura Cloud Azure, Unity Catalog & Catalogo SSOT

## 1. Infraestrutura Azure ADLS Gen2, Unity Catalog & Azure Function (`Brazil South`)
- **Storage Account (ADLS Gen2):** `portfolioigordatalake` com 3 containers fisicos mapeados para 3 Catalogos no Unity Catalog via External Locations (`cred_adls_portfolio`):
  - **Bronze:** Container e Catalogo `portfolio-lakehouse-bronze`
  - **Prata:** Container e Catalogo `portfolio-lakehouse-prata`
  - **Ouro:** Container e Catalogo `portfolio-lakehouse-ouro`
- **Azure Function de Ingestao (`Brazil South`):** `func-lakehouse-ingestao-br` (Python 3.12, Linux, Consumo Flexivel, codigo em `azure_functions/aneel_landing_br/`). Contorna de forma 100% cloud-native o bloqueio geografico (Geo-Block) do portal da ANEEL para clusters Databricks nos EUA (`East US 2`).
- **Compatibilidade Serverless & Clusters Classicos:** A autenticacao com o ADLS Gen2 no Spark e feita nativamente pelas External Locations do Unity Catalog (proibido usar `spark.conf.set("fs.azure.account.key...")`).
- **Seguranca Global:** Secret Scope `kv-portfolio-scope` vinculado ao Azure Key Vault `kv-portfolio-igor` contendo:
  - `adls-storage-key`: Chave da conta de armazenamento `portfolioigordatalake`.
  - `aneel-br-function-url`: URL completa com Function Key do endpoint `POST /api/sync_aneel_landing`.

## 2. Organizacao de Schemas e Tabelas por Camada
- **Camada Bronze (Alinhada a Fonte - Source-System Aligned):**
  - Catalogo: `portfolio-lakehouse-bronze` | Schema: **`aneel`** (nao existe nada chamado `gd` na Bronze)
  - Tabela: `aneel_gd_empreendimentos`
- **Camada Prata (Alinhada ao Dominio - Domain-Aligned):**
  - Catalogo: `portfolio-lakehouse-prata`
  - Schema **`tempo`:** `dim_calendario`
  - Schema **`localidade`:** `dim_regioes`, `dim_ufs`, `dim_municipios`, `dim_localidade`
  - Schema **`gd`:** `dim_distribuidoras`, `dim_classes_consumo`, `fato_empreendimentos_prata`
- **Camada Ouro (Alinhada ao Produto de Negocio - Business-Product Aligned / Opcao A):**
  - Catalogo: `portfolio-lakehouse-ouro`
  - Schema **`gd`:** `dim_enquadramento`, `dim_titular`, `dim_tecnologia`, `dim_modalidade`, `fato_empreendimentos`

## 3. Unica Fonte da Verdade (SSOT): `src/catalog_dictionary.py`
- Todo produto, fonte ou tabela deve ser registrado exclusivamente em `DATA_PRODUCTS_CATALOG` (`DataProductSpec` / `TableContract`).
- Nunca crie listas de tabelas hardcoded em notebooks ou classes auxiliares: use sempre `listar_tabelas_catalogo(camada, dominio_ou_fonte)` e `obter_contrato_por_tabela(nome_tabela)`.
