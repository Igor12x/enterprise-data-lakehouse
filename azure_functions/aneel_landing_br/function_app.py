import base64
from email.utils import formatdate
import hashlib
import hmac
import json
import os
from pathlib import Path
import tempfile
from urllib.parse import quote
import azure.functions as func
import requests

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

URL_PARQUET_ANEEL = (
    "https://dadosabertos.aneel.gov.br/dataset/5e0fafd2-21b9-4d5b-b622-40438d40aba2/"
    "resource/cd29f6eb-e08d-4db7-b6fb-ed6e3b682d27/download/"
    "empreendimento-geracao-distribuida.parquet"
)
CAMINHO_LANDING_ADLS = "landing/aneel_gd/empreendimento-geracao-distribuida.parquet"
TIMEOUT_HTTP_SEGUNDOS = 300


def _assinar_put_azure_blob(
    storage_account: str,
    storage_key: str,
    container_name: str,
    caminho_blob: str,
    tamanho_bytes: int,
    tipo_conteudo: str,
    parametros_query_ordenados: str
) -> dict:
    """
    Gera cabecalhos HTTP assinados com SharedKey (HMAC-SHA256) para PUT no Azure ADLS Gen2.
    """
    data_rfc1123 = formatdate(timeval=None, localtime=False, usegmt=True)
    versao_api_azure = "2023-11-03"

    if tamanho_bytes > 0:
        texto_tamanho = str(tamanho_bytes)
    else:
        texto_tamanho = ""

    recurso_canonico = f"/{storage_account}/{container_name}/{caminho_blob}"
    if parametros_query_ordenados != "":
        recurso_canonico = f"{recurso_canonico}\n{parametros_query_ordenados}"

    texto_para_assinar = (
        f"PUT\n"
        f"\n"
        f"\n"
        f"{texto_tamanho}\n"
        f"\n"
        f"{tipo_conteudo}\n"
        f"\n"
        f"\n"
        f"\n"
        f"\n"
        f"\n"
        f"\n"
        f"x-ms-date:{data_rfc1123}\n"
        f"x-ms-version:{versao_api_azure}\n"
        f"{recurso_canonico}"
    )

    chave_decodificada = base64.b64decode(storage_key)
    assinatura_hmac = hmac.new(
        chave_decodificada,
        texto_para_assinar.encode("utf-8"),
        hashlib.sha256
    ).digest()
    assinatura_base64 = base64.b64encode(assinatura_hmac).decode("utf-8")

    cabecalhos = {
        "x-ms-date": data_rfc1123,
        "x-ms-version": versao_api_azure,
        "Content-Length": str(tamanho_bytes),
        "Content-Type": tipo_conteudo,
        "Authorization": f"SharedKey {storage_account}:{assinatura_base64}"
    }
    return cabecalhos


def _baixar_parquet_aneel_para_temp() -> Path:
    """
    Baixa o arquivo .parquet oficial da ANEEL em streaming (blocos de 1 MB) para /tmp.
    """
    pasta_temp = Path(tempfile.gettempdir())
    caminho_arquivo = pasta_temp / "aneel_gd_empreendimentos.parquet"

    sessao_http = requests.Session()
    sessao_http.headers.update({
        "User-Agent": "Enterprise-Data-Lakehouse/1.0 (Azure Function Brazil South)",
        "Accept": "application/octet-stream"
    })

    resposta_http = sessao_http.get(
        url=URL_PARQUET_ANEEL,
        timeout=TIMEOUT_HTTP_SEGUNDOS,
        stream=True
    )
    resposta_http.raise_for_status()

    tamanho_bloco_1mb = 1024 * 1024
    with open(caminho_arquivo, mode="wb") as escritor_arquivo:
        for bloco_bytes in resposta_http.iter_content(chunk_size=tamanho_bloco_1mb):
            if bloco_bytes:
                escritor_arquivo.write(bloco_bytes)

    return caminho_arquivo


def _enviar_arquivo_em_blocos_adls(
    arquivo_local: Path,
    storage_account: str,
    storage_key: str,
    container_name: str,
    caminho_blob: str
) -> str:
    """
    Envia o arquivo local para a Landing Zone do ADLS Gen2 em blocos de 4 MB (Put Block + Put Block List).
    """
    caminho_blob_limpo = str(caminho_blob).strip("/")
    url_base_blob = (
        f"https://{storage_account}.blob.core.windows.net/"
        f"{container_name}/{caminho_blob_limpo}"
    )

    tamanho_bloco_4mb = 4 * 1024 * 1024
    lista_ids_blocos = []
    indice_bloco = 0

    with open(arquivo_local, mode="rb") as leitor_arquivo:
        while True:
            bytes_bloco = leitor_arquivo.read(tamanho_bloco_4mb)
            if not bytes_bloco:
                break

            indice_bloco = indice_bloco + 1
            id_bloco_texto = f"bloco-{indice_bloco:06d}"
            id_bloco_base64 = base64.b64encode(id_bloco_texto.encode("utf-8")).decode("utf-8")
            lista_ids_blocos.append(id_bloco_base64)

            parametros_canonicos = f"blockid:{id_bloco_base64}\ncomp:block"
            cabecalhos_bloco = _assinar_put_azure_blob(
                storage_account=storage_account,
                storage_key=storage_key,
                container_name=container_name,
                caminho_blob=caminho_blob_limpo,
                tamanho_bytes=len(bytes_bloco),
                tipo_conteudo="application/octet-stream",
                parametros_query_ordenados=parametros_canonicos
            )

            id_bloco_url = quote(id_bloco_base64, safe="")
            url_put_block = f"{url_base_blob}?comp=block&blockid={id_bloco_url}"

            resposta_bloco = requests.put(
                url=url_put_block,
                headers=cabecalhos_bloco,
                data=bytes_bloco,
                timeout=TIMEOUT_HTTP_SEGUNDOS
            )
            if resposta_bloco.status_code != 201:
                raise RuntimeError(
                    f"Falha no upload do bloco {indice_bloco} para ADLS: "
                    f"HTTP {resposta_bloco.status_code} - {resposta_bloco.text}"
                )

    linhas_xml = ['<?xml version="1.0" encoding="utf-8"?><BlockList>']
    for id_bloco in lista_ids_blocos:
        linhas_xml.append(f"<Latest>{id_bloco}</Latest>")
    linhas_xml.append("</BlockList>")
    corpo_xml_bytes = "".join(linhas_xml).encode("utf-8")

    cabecalhos_lista = _assinar_put_azure_blob(
        storage_account=storage_account,
        storage_key=storage_key,
        container_name=container_name,
        caminho_blob=caminho_blob_limpo,
        tamanho_bytes=len(corpo_xml_bytes),
        tipo_conteudo="application/xml",
        parametros_query_ordenados="comp:blocklist"
    )
    url_put_blocklist = f"{url_base_blob}?comp=blocklist"

    resposta_final = requests.put(
        url=url_put_blocklist,
        headers=cabecalhos_lista,
        data=corpo_xml_bytes,
        timeout=TIMEOUT_HTTP_SEGUNDOS
    )
    if resposta_final.status_code != 201:
        raise RuntimeError(
            f"Falha ao confirmar BlockList no ADLS: "
            f"HTTP {resposta_final.status_code} - {resposta_final.text}"
        )

    uri_abfss = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{caminho_blob_limpo}"
    return uri_abfss


@app.route(route="sync_aneel_landing", methods=["POST"])
def sync_aneel_landing(req: func.HttpRequest) -> func.HttpResponse:
    """
    Endpoint HTTP POST em Brazil South: baixa o .parquet da ANEEL (contornando Geo-Block)
    e grava diretamente na Landing Zone do Azure ADLS Gen2.
    """
    storage_account = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", "").strip()
    storage_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY", "").strip()
    container_bronze = os.environ.get("ADLS_CONTAINER_BRONZE", "").strip()

    if storage_account == "" or storage_key == "" or container_bronze == "":
        corpo_erro = json.dumps({
            "status": "erro",
            "mensagem": (
                "Variaveis obrigatorias ausentes na Function App: "
                "AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_ACCOUNT_KEY, ADLS_CONTAINER_BRONZE."
            )
        })
        return func.HttpResponse(corpo_erro, status_code=500, mimetype="application/json")

    caminho_temp: Path = None

    try:
        caminho_temp = _baixar_parquet_aneel_para_temp()
        tamanho_mb = round(caminho_temp.stat().st_size / (1024 * 1024), 2)

        uri_landing_abfss = _enviar_arquivo_em_blocos_adls(
            arquivo_local=caminho_temp,
            storage_account=storage_account,
            storage_key=storage_key,
            container_name=container_bronze,
            caminho_blob=CAMINHO_LANDING_ADLS
        )

        corpo_sucesso = json.dumps({
            "status": "sucesso",
            "regiao_execucao": "Brazil South",
            "tamanho_mb": tamanho_mb,
            "uri_landing_abfss": uri_landing_abfss
        })
        return func.HttpResponse(corpo_sucesso, status_code=200, mimetype="application/json")

    except Exception as erro_execucao:
        corpo_falha = json.dumps({
            "status": "erro",
            "mensagem": str(erro_execucao)
        })
        return func.HttpResponse(corpo_falha, status_code=500, mimetype="application/json")

    finally:
        if caminho_temp is not None and caminho_temp.exists() is True:
            os.remove(caminho_temp)
