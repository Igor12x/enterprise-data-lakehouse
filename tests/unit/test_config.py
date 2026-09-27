import pytest
from pathlib import Path
from src.config import settings


def test_config_diretorios_principais():
    """
    Valida se os caminhos absolutos calculados pela classe Settings
    realmente existem no disco da maquina ou servidor.
    """
    # 1. Validacao da existencia da pasta raiz e da pasta src
    assert settings.diretorio_raiz is not None
    assert settings.diretorio_raiz.exists() is True

    assert settings.diretorio_src is not None
    assert settings.diretorio_src.exists() is True

    # 2. Validacao se a pasta src realmente esta dentro da raiz
    caminho_esperado_src = settings.diretorio_raiz / "src"
    assert settings.diretorio_src == caminho_esperado_src


def test_config_valores_padrao():
    """
    Valida se os parametros de sistema e valores default de bucket
    foram atribuidos conforme as diretrizes do Lakehouse.
    """
    assert settings.TIMEZONE_LOCAL == "America/Sao_Paulo"
    assert settings.TIMEOUT_CONEXAO_SEGUNDOS == 30

    assert settings.R2_BUCKET_BRONZE == "lakehouse-bronze"
    assert settings.R2_BUCKET_SILVER == "lakehouse-prata"
    assert settings.R2_BUCKET_GOLD == "lakehouse-ouro"

    assert settings.MOTHERDUCK_DATABASE_BRONZE == "bronze"
    assert settings.MOTHERDUCK_DATABASE_PRATA == "prata"
    assert settings.MOTHERDUCK_DATABASE_OURO == "ouro"


def test_config_variaveis_obrigatorias_preenchidas():
    """
    Percorre a lista de credenciais obrigatorias e valida se nenhuma
    delas ficou com valor nulo ou vazio apos a inicializacao.
    """
    lista_checagem = [
        ("R2_ENDPOINT_URL", settings.R2_ENDPOINT_URL),
        ("R2_ACCESS_KEY_ID", settings.R2_ACCESS_KEY_ID),
        ("R2_SECRET_ACCESS_KEY", settings.R2_SECRET_ACCESS_KEY),
        ("MOTHERDUCK_TOKEN", settings.MOTHERDUCK_TOKEN)
    ]

    for item in lista_checagem:
        nome_variavel = item[0]
        valor_variavel = item[1]

        # Validacao defensiva tradicional
        assert valor_variavel is not None, f"A variavel {nome_variavel} esta nula."
        
        texto_limpo = str(valor_variavel).strip()
        tamanho_texto = len(texto_limpo)
        
        assert tamanho_texto > 0, f"A variavel {nome_variavel} nao possui conteudo."