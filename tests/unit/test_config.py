import pytest
from src.config import settings
from src.catalog_dictionary import (
    get_data_product,
    listar_tabelas_catalogo,
    obter_contrato_por_tabela
)
from src.connectors.adls_storage_connector import AdlsStorageConnector
from src.infrastructure.databricks_runtime import DatabricksRuntime
from src.infrastructure.dbt_runner import DbtRunner
from src.ingestion.extractors.aneel_gd_extractor import AneelGdExtractor
from src.notebooks.base_notebook import BaseNotebook
from src.persistence.delta_catalog_manager import DeltaCatalogManager


def test_config_diretorios_principais():
    """Valida se os caminhos absolutos da classe Settings existem no computador."""
    assert settings.diretorio_raiz is not None
    assert settings.diretorio_raiz.exists() is True
    assert settings.diretorio_src is not None
    assert settings.diretorio_src.exists() is True


def test_catalogo_tres_produtos_e_fonte_aneel_bronze():
    """Valida os produtos 'tempo', 'localidade' e 'gd' (com schema_bronze='aneel')."""
    produto_tempo = get_data_product("tempo")
    assert produto_tempo.schema_silver == "tempo"
    assert produto_tempo.get_silver_table("dim_calendario").full_table_identifier == "portfolio-lakehouse-prata.tempo.dim_calendario"

    produto_localidade = get_data_product("localidade")
    assert produto_localidade.schema_silver == "localidade"
    assert produto_localidade.get_silver_table("dim_municipios").full_table_identifier == "portfolio-lakehouse-prata.localidade.dim_municipios"
    assert len(produto_localidade.listar_chaves_prata()) == 4

    produto_gd = get_data_product("gd")
    assert produto_gd.schema_bronze == "aneel"
    assert produto_gd.schema_silver == "gd"
    assert produto_gd.schema_gold == "gd"
    assert len(produto_gd.listar_chaves_ouro()) == 5
    assert produto_gd.get_gold_table("fato_empreendimentos").full_table_identifier == "portfolio-lakehouse-ouro.gd.fato_empreendimentos"


def test_obter_contrato_por_tabela_em_todas_camadas():
    """Valida a busca direta de contrato pelo nome da tabela (Bronze, Prata e Ouro)."""
    contrato_bronze = obter_contrato_por_tabela("aneel_gd_empreendimentos")
    assert contrato_bronze.layer == "bronze"
    assert contrato_bronze.full_table_identifier == "portfolio-lakehouse-bronze.aneel.aneel_gd_empreendimentos"
    assert contrato_bronze.adls_relative_path == "aneel/aneel_gd_empreendimentos"

    contrato_prata = obter_contrato_por_tabela("dim_localidade")
    assert contrato_prata.layer == "prata"
    assert contrato_prata.full_table_identifier == "portfolio-lakehouse-prata.localidade.dim_localidade"

    contrato_ouro = obter_contrato_por_tabela("fato_empreendimentos")
    assert contrato_ouro.layer == "ouro"
    assert contrato_ouro.full_table_identifier == "portfolio-lakehouse-ouro.gd.fato_empreendimentos"


def test_arquivos_sql_dbt_e_src_notebooks_existem():
    """Garante que os 13 arquivos .sql do dbt e os 5 notebooks em src/notebooks existem e que notebooks/ e src/flows/ foram removidos."""
    raiz_models = settings.diretorio_raiz / "dbt_project" / "models"
    raiz_src_notebooks = settings.diretorio_src / "notebooks"

    arquivos_esperados = [
        raiz_models / "bronze" / "aneel" / "_bronze_aneel__sources.yml",
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
        raiz_src_notebooks / "base_notebook.py",
        raiz_src_notebooks / "bronze" / "aneel.py",
        raiz_src_notebooks / "prata" / "tempo.py",
        raiz_src_notebooks / "prata" / "localidade.py",
        raiz_src_notebooks / "prata" / "gd.py",
        raiz_src_notebooks / "ouro" / "gd.py",
    ]

    for caminho in arquivos_esperados:
        assert caminho.exists() is True, f"Arquivo esperado nao encontrado: {caminho}"

    assert (settings.diretorio_raiz / "notebooks").exists() is False
    assert (settings.diretorio_src / "flows").exists() is False
    assert (raiz_models / "bronze" / "gd").exists() is False


def test_modelos_ouro_dbt_usam_xxhash64_e_incremental_merge():
    """Valida se os arquivos .sql da camada Ouro usam xxhash64 e configuracao incremental merge."""
    pasta_ouro = settings.diretorio_raiz / "dbt_project" / "models" / "ouro" / "gd"

    for arquivo_sql in pasta_ouro.glob("*.sql"):
        conteudo = arquivo_sql.read_text(encoding="utf-8")
        assert "xxhash64(" in conteudo, f"O modelo {arquivo_sql.name} deve usar xxhash64."
        assert "incremental_strategy='merge'" in conteudo


def test_separacao_tres_responsabilidades_infra_conector_persistencia():
    """Valida a separacao em DatabricksRuntime, DbtRunner, AdlsStorageConnector e DeltaCatalogManager."""
    runtime = DatabricksRuntime()
    dbt_runner = DbtRunner(runtime=runtime)
    adls_connector = AdlsStorageConnector()
    delta_manager = DeltaCatalogManager(runtime=runtime)

    assert hasattr(runtime, "validar_modo_carga") is False
    assert hasattr(delta_manager, "validar_modo_carga") is False
    assert delta_manager._formatar_identificador_sql("portfolio-lakehouse-bronze.aneel.aneel_gd_empreendimentos") == (
        "`portfolio-lakehouse-bronze`.`aneel`.`aneel_gd_empreendimentos`"
    )
    assert adls_connector.container_bronze == settings.ADLS_CONTAINER_BRONZE

    comando_full = dbt_runner.montar_comando_dbt(seletor_modelo="fato_empreendimentos", modo_carga="full")
    assert "--full-refresh" in comando_full

    comando_incremental = dbt_runner.montar_comando_dbt(seletor_modelo="fato_empreendimentos", modo_carga="incremental")
    assert "--full-refresh" not in comando_incremental


def test_databricks_runtime_leitura_e_validacao_requirements():
    """Garante que o DatabricksRuntime valida todos os pacotes do requirements.txt."""
    runtime = DatabricksRuntime()
    dependencias = runtime.ler_dependencias_requirements()

    assert len(dependencias) > 0
    assert any("dbt-core" in item for item in dependencias)
    assert any("dbt-databricks" in item for item in dependencias)

    faltantes = runtime.garantir_dependencias_requirements()
    assert faltantes == []


def test_contrato_base_notebook_carregamento_dinamico_ssot():
    """Valida que o BaseNotebook carrega dinamicamente a lista de tabelas do catalog_dictionary.py."""
    nb_localidade = BaseNotebook(
        camada="prata",
        dominio_ou_fonte="localidade"
    )

    assert nb_localidade.opcoes_modo_carga == ["full", "incremental"]
    assert nb_localidade.opcoes_tabela_alvo == [
        "todas",
        "dim_regioes",
        "dim_ufs",
        "dim_municipios",
        "dim_localidade"
    ]
    assert nb_localidade.validar_selecao_tabela("todas") == listar_tabelas_catalogo("prata", "localidade")
    assert nb_localidade.validar_selecao_tabela("dim_ufs") == ["dim_ufs"]
    assert nb_localidade.validar_parametros("FULL", "todas") == ("full", "todas")
    assert nb_localidade.validar_parametros("incremental", "dim_ufs") == ("incremental", "dim_ufs")

    extrator_aneel = AneelGdExtractor()
    nb_bronze_aneel = BaseNotebook(
        camada="bronze",
        dominio_ou_fonte="aneel",
        extrator_bronze=extrator_aneel
    )
    assert nb_bronze_aneel.opcoes_tabela_alvo == ["todas", "aneel_gd_empreendimentos"]

    with pytest.raises(TypeError):
        BaseNotebook(camada="bronze", dominio_ou_fonte="aneel")

    with pytest.raises(ValueError):
        nb_localidade.validar_parametros("full", "fato_empreendimentos")

    with pytest.raises(ValueError):
        BaseNotebook(camada="invalida", dominio_ou_fonte="gd")
