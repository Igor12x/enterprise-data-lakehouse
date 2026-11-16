import base64
from email.utils import formatdate
import hashlib
import hmac
import os
import pytest
import requests
from src.config import settings
from src.ingestion.extractors.aneel_gd_extractor import AneelGdExtractor


def _gerar_cabecalhos_azure_shared_key(
    nome_conta_storage: str,
    chave_conta_storage: str,
    nome_container: str
) -> dict:
    """
    Gera os cabecalhos HTTP assinados (SharedKey) exigidos pela API REST do Azure Storage
    para validar a existencia do container sem precisar instalar SDK adicional.
    """
    data_rfc1123 = formatdate(timeval=None, localtime=False, usegmt=True)
    versao_api_azure = "2023-11-03"

    texto_para_assinar = (
        f"GET\n\n\n\n\n\n\n\n\n\n\n\n"
        f"x-ms-date:{data_rfc1123}\n"
        f"x-ms-version:{versao_api_azure}\n"
        f"/{nome_conta_storage}/{nome_container}\n"
        f"restype:container"
    )

    chave_decodificada = base64.b64decode(chave_conta_storage)
    assinatura_hmac = hmac.new(
        chave_decodificada,
        texto_para_assinar.encode("utf-8"),
        hashlib.sha256
    ).digest()
    assinatura_base64 = base64.b64encode(assinatura_hmac).decode("utf-8")

    cabecalhos = {
        "x-ms-date": data_rfc1123,
        "x-ms-version": versao_api_azure,
        "Authorization": f"SharedKey {nome_conta_storage}:{assinatura_base64}"
    }
    return cabecalhos


def test_integracao_api_aneel_disponivel_e_parquet_valido():
    """
    Verifica na internet se a URL oficial da ANEEL esta no ar (HTTP 200)
    e se o inicio do arquivo baixado corresponde a um arquivo .parquet valido (bytes 'PAR1').
    """
    extrator = AneelGdExtractor()

    resposta_http = extrator.sessao_http.get(
        url=extrator.url_origem,
        timeout=settings.TIMEOUT_CONEXAO_SEGUNDOS,
        stream=True
    )

    try:
        assert resposta_http.status_code == 200, (
            f"A API da ANEEL retornou status HTTP inesperado: {resposta_http.status_code}"
        )

        tamanho_bytes_cabecalho = resposta_http.headers.get("Content-Length")
        if tamanho_bytes_cabecalho is not None:
            tamanho_inteiro = int(tamanho_bytes_cabecalho)
            assert tamanho_inteiro > 0, "O arquivo Parquet da ANEEL retornou tamanho 0."

        primeiros_bytes = next(resposta_http.iter_content(chunk_size=1024))
        assinatura_parquet = primeiros_bytes[:4]
        assert assinatura_parquet == b"PAR1", (
            f"O arquivo retornado pela ANEEL nao e um Parquet valido. Cabecalho: {assinatura_parquet}"
        )
    finally:
        resposta_http.close()


def test_integracao_conexao_azure_adls_gen2():
    """
    Valida a autenticacao real no Azure Data Lake Storage Gen2 (portfolioigordatalake)
    e confirma se o container Bronze (portfolio-lakehouse-bronze) esta acessivel.
    """
    nome_conta = settings.AZURE_STORAGE_ACCOUNT_NAME
    chave_conta = settings.AZURE_STORAGE_ACCOUNT_KEY
    container_bronze = settings.ADLS_CONTAINER_BRONZE

    url_container = (
        f"https://{nome_conta}.blob.core.windows.net/"
        f"{container_bronze}?restype=container"
    )
    cabecalhos = _gerar_cabecalhos_azure_shared_key(
        nome_conta_storage=nome_conta,
        chave_conta_storage=chave_conta,
        nome_container=container_bronze
    )

    resposta_azure = requests.get(
        url=url_container,
        headers=cabecalhos,
        timeout=settings.TIMEOUT_CONEXAO_SEGUNDOS
    )

    assert resposta_azure.status_code == 200, (
        f"Falha ao conectar no Azure Storage '{nome_conta}' / container '{container_bronze}'. "
        f"Status HTTP: {resposta_azure.status_code} | Resposta: {resposta_azure.text}"
    )


def test_integracao_conexao_databricks():
    """
    Valida a conectividade com o Azure Databricks:
    - Se estiver rodando dentro de um cluster Databricks, testa a SparkSession nativa.
    - Se estiver rodando no PC local, testa via databricks-sql-connector usando
      DATABRICKS_HOST, DATABRICKS_HTTP_PATH e DATABRICKS_TOKEN.
    """
    esta_no_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ

    if esta_no_databricks is True:
        from src.infrastructure.databricks_runtime import DatabricksRuntime

        runtime = DatabricksRuntime()
        sessao_spark = runtime.obter_sessao_spark()
        resultado_sql = sessao_spark.sql("SELECT 1 AS teste_conexao").collect()
        assert resultado_sql[0]["teste_conexao"] == 1
        return

    host_databricks = settings._obter_valor(nome_chave="DATABRICKS_HOST")
    http_path_databricks = settings._obter_valor(nome_chave="DATABRICKS_HTTP_PATH")
    token_databricks = settings._obter_valor(nome_chave="DATABRICKS_TOKEN")

    variaveis_faltantes = []
    if host_databricks is None or str(host_databricks).strip() == "":
        variaveis_faltantes.append("DATABRICKS_HOST")
    if http_path_databricks is None or str(http_path_databricks).strip() == "":
        variaveis_faltantes.append("DATABRICKS_HTTP_PATH")
    if token_databricks is None or str(token_databricks).strip() == "":
        variaveis_faltantes.append("DATABRICKS_TOKEN")

    if len(variaveis_faltantes) > 0:
        lista_texto = ", ".join(variaveis_faltantes)
        pytest.skip(
            f"Teste Databricks ignorado no PC local: defina {lista_texto} no .env "
            f"para testar o Databricks remotamente (ou execute dentro do cluster Databricks)."
        )

    from databricks import sql as databricks_sql

    host_limpo = str(host_databricks).replace("https://", "").replace("http://", "").strip("/")
    conexao = databricks_sql.connect(
        server_hostname=host_limpo,
        http_path=str(http_path_databricks).strip(),
        access_token=str(token_databricks).strip()
    )

    try:
        cursor = conexao.cursor()
        cursor.execute("SELECT 1 AS teste_conexao")
        linha = cursor.fetchone()
        assert linha is not None
        assert linha[0] == 1
        cursor.close()
    finally:
        conexao.close()
