from typing import Dict, List


class TableContract:
    """
    Classe (estilo DTO/POJO do Java e C#) que representa a ficha tecnica de uma tabela.
    No contexto do Power BI, define em qual schema ela fica, o nome da tabela e sua Chave Primaria.
    """

    def __init__(
        self,
        layer: str,
        product_id: str,
        schema_name: str,
        table_name: str,
        primary_key: str,
        adls_relative_path: str,
        description: str
    ) -> None:
        self.layer = layer
        self.product_id = product_id
        self.schema_name = schema_name
        self.table_name = table_name
        self.primary_key = primary_key
        self.adls_relative_path = adls_relative_path
        self.description = description

        # Nome completo no catalogo do Databricks (Ex: hive_metastore.gd.fato_empreendimentos)
        self.full_table_identifier = f"hive_metastore.{self.schema_name}.{self.table_name}"


class DataProductSpec:
    """
    Classe que agrupa as tabelas de um Produto de Dados ('tempo', 'localidade' ou 'gd').
    Os schemas no Databricks usam diretamente o nome do produto (sem prefixo bronze_, prata_ ou ouro_).
    """

    def __init__(
        self,
        product_id: str,
        domain_folder: str,
        description: str,
        tables_bronze: Dict[str, Dict[str, str]],
        tables_silver: Dict[str, Dict[str, str]],
        tables_gold: Dict[str, Dict[str, str]]
    ) -> None:
        self.product_id = str(product_id).strip().lower()
        self.domain_folder = str(domain_folder).strip("/")
        self.description = description

        # No Databricks (hive_metastore), o nome do schema e o proprio nome do produto
        self.schema_bronze = self.product_id
        self.schema_silver = self.product_id
        self.schema_gold = self.product_id

        self._tables_bronze = tables_bronze
        self._tables_silver = tables_silver
        self._tables_gold = tables_gold

    def _criar_contrato_tabela(
        self,
        nome_camada: str,
        nome_schema: str,
        dicionario_camada: Dict[str, Dict[str, str]],
        chave_tabela: str
    ) -> TableContract:
        """
        Monta e retorna o objeto TableContract para a tabela solicitada.
        """
        if chave_tabela not in dicionario_camada:
            raise KeyError(
                f"[Catalogo] A tabela '{chave_tabela}' nao foi encontrada na camada "
                f"'{nome_camada}' do produto '{self.product_id}'."
            )

        dados_tabela = dicionario_camada[chave_tabela]
        nome_fisico = dados_tabela["table_name"]
        chave_primaria = dados_tabela["primary_key"]
        descricao = dados_tabela["description"]
        caminho_relativo_adls = f"{self.domain_folder}/{nome_fisico}"

        contrato = TableContract(
            layer=nome_camada,
            product_id=self.product_id,
            schema_name=nome_schema,
            table_name=nome_fisico,
            primary_key=chave_primaria,
            adls_relative_path=caminho_relativo_adls,
            description=descricao
        )
        return contrato

    def get_bronze_table(self, table_key: str) -> TableContract:
        """Retorna o contrato de uma tabela da camada Bronze."""
        return self._criar_contrato_tabela(
            nome_camada="bronze",
            nome_schema=self.schema_bronze,
            dicionario_camada=self._tables_bronze,
            chave_tabela=table_key
        )

    def get_silver_table(self, table_key: str) -> TableContract:
        """Retorna o contrato de uma tabela da camada Prata."""
        return self._criar_contrato_tabela(
            nome_camada="prata",
            nome_schema=self.schema_silver,
            dicionario_camada=self._tables_silver,
            chave_tabela=table_key
        )

    def get_gold_table(self, table_key: str) -> TableContract:
        """Retorna o contrato de uma tabela da camada Ouro."""
        return self._criar_contrato_tabela(
            nome_camada="ouro",
            nome_schema=self.schema_gold,
            dicionario_camada=self._tables_gold,
            chave_tabela=table_key
        )

    def listar_chaves_prata(self) -> List[str]:
        """Retorna uma lista com os nomes das tabelas Prata deste produto."""
        lista_nomes: List[str] = []
        for chave in self._tables_silver.keys():
            lista_nomes.append(chave)
        return lista_nomes

    def listar_chaves_ouro(self) -> List[str]:
        """Retorna uma lista com os nomes das tabelas Ouro deste produto."""
        lista_nomes: List[str] = []
        for chave in self._tables_gold.keys():
            lista_nomes.append(chave)
        return lista_nomes


# =============================================================================
# DICIONARIO CENTRAL DE PRODUTOS DE DADOS (UNICA FONTE DA VERDADE - SSOT)
# Da ANEEL e extraida apenas 1 tabela Bronze (aneel_gd_empreendimentos),
# que alimenta as diversas tabelas Prata e Ouro para o modelo Star Schema.
# =============================================================================
DATA_PRODUCTS_CATALOG: Dict[str, DataProductSpec] = {
    # -------------------------------------------------------------------------
    # PRODUTO 1: TEMPO (Dimensao Corporativa de Calendario)
    # Schema no Databricks: hive_metastore.tempo
    # -------------------------------------------------------------------------
    "tempo": DataProductSpec(
        product_id="tempo",
        domain_folder="corporativo/tempo",
        description="Produto de Dados Corporativo de Calendario e Datas",
        tables_bronze={},
        tables_silver={
            "dim_calendario": {
                "table_name": "dim_calendario",
                "primary_key": "id_data",
                "description": "Dimensao de calendario completa para analises temporais no Power BI"
            }
        },
        tables_gold={}
    ),

    # -------------------------------------------------------------------------
    # PRODUTO 2: LOCALIDADE (Dimensoes Geograficas IBGE derivadas da Bronze GD)
    # Schema no Databricks: hive_metastore.localidade
    # -------------------------------------------------------------------------
    "localidade": DataProductSpec(
        product_id="localidade",
        domain_folder="corporativo/localidade",
        description="Produto de Dados Corporativo de Geografia e Municipios (IBGE)",
        tables_bronze={},
        tables_silver={
            "dim_regioes": {
                "table_name": "dim_regioes",
                "primary_key": "cod_regiao",
                "description": "Macrorregioes geograficas brasileiras (IBGE)"
            },
            "dim_ufs": {
                "table_name": "dim_ufs",
                "primary_key": "cod_uf_ibge",
                "description": "Unidades Federativas (Estados) vinculadas as Regioes"
            },
            "dim_municipios": {
                "table_name": "dim_municipios",
                "primary_key": "cod_municipio_ibge",
                "description": "Municipios brasileiros (IBGE) com distribuidora dominante"
            },
            "dim_localidade": {
                "table_name": "dim_localidade",
                "primary_key": "cod_municipio_ibge",
                "description": "Dimensao consolidada de Localidade (Municipio, UF, Regiao e Distribuidora)"
            }
        },
        tables_gold={}
    ),

    # -------------------------------------------------------------------------
    # PRODUTO 3: GD (Geracao Distribuida ANEEL - 1 Bronze -> Pratas e Ouros)
    # Schema no Databricks: hive_metastore.gd
    # -------------------------------------------------------------------------
    "gd": DataProductSpec(
        product_id="gd",
        domain_folder="aneel/gd",
        description="Produto de Dados de Geracao Distribuida da ANEEL",
        tables_bronze={
            "aneel_gd_empreendimentos": {
                "table_name": "aneel_gd_empreendimentos",
                "primary_key": "CodEmpreendimento",
                "description": "Unica tabela bruta extraida do Parquet oficial da ANEEL"
            }
        },
        tables_silver={
            "dim_distribuidoras": {
                "table_name": "dim_distribuidoras",
                "primary_key": "cnpj_distribuidora",
                "description": "Cadastro higienizado de Distribuidoras de Energia da ANEEL"
            },
            "dim_classes_consumo": {
                "table_name": "dim_classes_consumo",
                "primary_key": "cod_classe_consumo",
                "description": "Dominio de Classes de Consumo Tarifario"
            },
            "fato_empreendimentos_prata": {
                "table_name": "fato_empreendimentos_prata",
                "primary_key": "cod_empreendimento",
                "description": "Tabela fato transacional limpa e deduplicada de Empreendimentos GD na Prata"
            }
        },
        tables_gold={
            "dim_enquadramento": {
                "table_name": "dim_enquadramento",
                "primary_key": "sk_enquadramento",
                "description": "Dimensao Ouro de Enquadramento Tarifario e Grupo de Tensao"
            },
            "dim_titular": {
                "table_name": "dim_titular",
                "primary_key": "sk_titular",
                "description": "Dimensao Ouro de Titular/Consumidor tratada para LGPD"
            },
            "dim_tecnologia": {
                "table_name": "dim_tecnologia",
                "primary_key": "sk_tecnologia",
                "description": "Dimensao Ouro de Tecnologia, Fonte e Porte de Geracao"
            },
            "dim_modalidade": {
                "table_name": "dim_modalidade",
                "primary_key": "sk_modalidade",
                "description": "Dimensao Ouro de Modalidade de Compensacao Energetica"
            },
            "fato_empreendimentos": {
                "table_name": "fato_empreendimentos",
                "primary_key": "sk_empreendimento",
                "description": "Tabela Fato Star Schema para Power BI com chaves inteiras xxhash64"
            }
        }
    )
}


def get_data_product(product_id: str) -> DataProductSpec:
    """
    Busca um Produto de Dados pelo nome ('tempo', 'localidade' ou 'gd').
    """
    if product_id is None:
        raise ValueError("[Catalogo] O nome do produto nao pode ser nulo.")

    chave_limpa = str(product_id).strip().lower()
    if chave_limpa == "":
        raise ValueError("[Catalogo] O nome do produto nao pode ser vazio.")

    if chave_limpa not in DATA_PRODUCTS_CATALOG:
        produtos_validos = ", ".join(DATA_PRODUCTS_CATALOG.keys())
        raise KeyError(
            f"[Catalogo] Produto '{product_id}' nao existe. "
            f"Produtos cadastrados: [{produtos_validos}]"
        )

    produto_selecionado = DATA_PRODUCTS_CATALOG[chave_limpa]
    return produto_selecionado


def obter_contrato_por_tabela(nome_tabela: str) -> TableContract:
    """
    Procura uma tabela pelo nome em todos os produtos (Bronze, Prata e Ouro)
    e retorna seu contrato completo (TableContract).
    """
    if nome_tabela is None or str(nome_tabela).strip() == "":
        raise ValueError("[Catalogo] O nome da tabela nao pode ser vazio.")

    chave_busca = str(nome_tabela).strip()

    for produto in DATA_PRODUCTS_CATALOG.values():
        if chave_busca in produto._tables_bronze:
            return produto.get_bronze_table(chave_busca)

        chaves_prata = produto.listar_chaves_prata()
        if chave_busca in chaves_prata:
            return produto.get_silver_table(chave_busca)

        chaves_ouro = produto.listar_chaves_ouro()
        if chave_busca in chaves_ouro:
            return produto.get_gold_table(chave_busca)

    raise KeyError(
        f"[Catalogo] A tabela '{nome_tabela}' nao esta cadastrada em nenhum produto do catalogo."
    )
