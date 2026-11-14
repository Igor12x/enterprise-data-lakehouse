import pytest
from src.config import settings
from src.catalog_dictionary import get_data_product, obter_contrato_por_tabela
from src.connectors.adls_databricks_connector import AdlsDatabricksConnector
from src.flows.flow_aneel import FlowAneel


def test_config_diretorios_principais():
    """Valida se os caminhos absolutos da classe Settings existem no computador."""
    assert settings.diretorio_raiz is not None
    assert settings.diretorio_raiz.exists() is True
    assert settings.diretorio_src is not None
    assert settings.diretorio_src.exists() is True


def test_catalogo_tres_produtos_sem_prefixo():
    """Valida os produtos 'tempo', 'localidade' e 'gd' sem prefixos nos schemas."""
    produto_tempo = get_data_product("tempo")
    assert produto_tempo.schema_silver == "tempo"
    assert produto_tempo.get_silver_table("dim_calendario").full_table_identifier == "portfolio-lakehouse-prata.tempo.dim_calendario"

    produto_localidade = get_data_product("localidade")
    assert produto_localidade.schema_silver == "localidade"
    assert produto_localidade.get_silver_table("dim_municipios").full_table_identifier == "portfolio-lakehouse-prata.localidade.dim_municipios"
    assert len(produto_localidade.listar_chaves_prata()) == 4

    produto_gd = get_data_product("gd")
    assert produto_gd.schema_bronze == "gd"
    assert produto_gd.schema_silver == "gd"
    assert produto_gd.schema_gold == "gd"
    assert len(produto_gd.listar_chaves_ouro()) == 5
    assert produto_gd.get_gold_table("fato_empreendimentos").full_table_identifier == "portfolio-lakehouse-ouro.gd.fato_empreendimentos"


def test_obter_contrato_por_tabela_em_todas_camadas():
    """Valida a busca direta de contrato pelo nome da tabela (Bronze, Prata e Ouro)."""
    contrato_bronze = obter_contrato_por_tabela("aneel_gd_empreendimentos")
    assert contrato_bronze.layer == "bronze"
    assert contrato_bronze.full_table_identifier == "portfolio-lakehouse-bronze.gd.aneel_gd_empreendimentos"

    contrato_prata = obter_contrato_por_tabela("dim_localidade")
    assert contrato_prata.layer == "prata"
    assert contrato_prata.full_table_identifier == "portfolio-lakehouse-prata.localidade.dim_localidade"

    contrato_ouro = obter_contrato_por_tabela("fato_empreendimentos")
    assert contrato_ouro.layer == "ouro"
    assert contrato_ouro.full_table_identifier == "portfolio-lakehouse-ouro.gd.fato_empreendimentos"


def test_arquivos_sql_dbt_existem_nas_pastas_camada_produto():
    """Garante que todos os 13 arquivos .sql do dbt foram criados na estrutura Camada -> Produto."""
    raiz_models = settings.diretorio_raiz / "dbt_project" / "models"

    arquivos_esperados = [
        raiz_models / "prata" / "tempo" / "dim_calendario.sql",
        raiz_models / "prata" / "localidade" / "dim_regioes.sql",
        raiz_models / "prata" / "localidade" / "dim_ufs.sql",
        raiz_models / "prata" / "localidade" / "dim_municipios.sql",
        raiz_models / "prata" / "localidade" / "dim_localidade.sql",
        raiz_models / "prata" / "gd" / "dim_distribuidoras.sql",
        raiz_models / "prata" / "gd" / "dim_classes_consumo.sql",
        raiz_models / "prata" / "gd" / "fato_empreendimentos_prata.sql",
        raiz_models / "ouro" / "gd" / "dim_enquadramento.sql",
        raiz_models / "ouro" / "gd" / "dim_titular.sql",
        raiz_models / "ouro" / "gd" / "dim_tecnologia.sql",
        raiz_models / "ouro" / "gd" / "dim_modalidade.sql",
        raiz_models / "ouro" / "gd" / "fato_empreendimentos.sql",
    ]

    for caminho_sql in arquivos_esperados:
        assert caminho_sql.exists() is True, f"Arquivo SQL nao encontrado: {caminho_sql}"


def test_modelos_ouro_dbt_usam_xxhash64_e_incremental_merge():
    """Valida se os arquivos .sql da camada Ouro usam xxhash64 e configuracao incremental merge."""
    pasta_ouro = settings.diretorio_raiz / "dbt_project" / "models" / "ouro" / "gd"

    for arquivo_sql in pasta_ouro.glob("*.sql"):
        conteudo = arquivo_sql.read_text(encoding="utf-8")
        assert "xxhash64(" in conteudo, f"O modelo {arquivo_sql.name} deve usar xxhash64."
        assert "incremental_strategy='merge'" in conteudo


def test_flow_aneel_ordem_dependencia_tabelas():
    """
    Garante matematicamente que na ordem de execucao (Bronze -> Prata -> Ouro),
    nenhuma tabela e executada antes das tabelas das quais ela depende.
    """
    fluxo = FlowAneel()

    sequencia_completa = [fluxo.TABELA_BRONZE_ANEEL]
    for tabela_prata in fluxo.ORDEM_TABELAS_PRATA:
        sequencia_completa.append(tabela_prata)
    for tabela_ouro in fluxo.ORDEM_TABELAS_OURO:
        sequencia_completa.append(tabela_ouro)

    tabelas_ja_processadas = []

    for nome_tabela in sequencia_completa:
        dependencias = fluxo.MAPA_DEPENDENCIAS[nome_tabela]
        for tabela_mae in dependencias:
            assert tabela_mae in tabelas_ja_processadas, (
                f"Erro de dependencia: '{nome_tabela}' tentou rodar antes de '{tabela_mae}'."
            )
        tabelas_ja_processadas.append(nome_tabela)


def test_flow_aneel_montagem_comandos_dbt_full_e_incremental():
    """Valida se o FlowAneel gera '--full-refresh' no modo full e omite no incremental."""
    fluxo = FlowAneel()

    comando_full = fluxo.montar_comando_dbt(seletor_modelo="fato_empreendimentos", modo_carga="full")
    assert "--full-refresh" in comando_full

    comando_incremental = fluxo.montar_comando_dbt(seletor_modelo="fato_empreendimentos", modo_carga="incremental")
    assert "--full-refresh" not in comando_incremental


def test_validacao_modo_carga_conector():
    """Valida a checagem de modo_carga sem acionar a sessao do Spark."""
    conector = AdlsDatabricksConnector()
    assert conector.validar_modo_carga("FULL") == "full"
    assert conector.validar_modo_carga("incremental") == "incremental"

    with pytest.raises(ValueError):
        conector.validar_modo_carga("erro")
