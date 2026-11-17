import base64
from email.utils import formatdate
import hashlib
import hmac
from pathlib import Path
from urllib.parse import quote
import requests
from src.config import settings


class AdlsStorageConnector:
    """
    Responsabilidade Unica (Conectividade HTTP/REST com Azure Storage ADLS Gen2):
    Realiza a assinatura criptografica SharedKey (HMAC-SHA256) e gerencia a Landing Zone
    transitoria no ADLS Gen2 (upload em blocos de 4 MB e limpeza apos ingestao na Bronze).
    """

    def __init__(self) -> None:
        self.storage_account = settings.AZURE_STORAGE_ACCOUNT_NAME
        self.storage_key = settings.AZURE_STORAGE_ACCOUNT_KEY
        self.container_bronze = settings.ADLS_CONTAINER_BRONZE

    def _assinar_requisicao_azure(
        self,
        metodo_http: str,
        caminho_recurso: str,
        texto_tamanho: str,
        tipo_conteudo: str,
        parametros_query_ordenados: str
    ) -> dict:
        """
        Gera cabecalhos HTTP assinados com SharedKey (HMAC-SHA256) para operacoes REST
        no Azure Blob / ADLS Gen2 sem exigir bibliotecas extras.
        """
        data_rfc1123 = formatdate(timeval=None, localtime=False, usegmt=True)
        versao_api_azure = "2023-11-03"

        caminho_limpo = str(caminho_recurso).strip("/")
        if caminho_limpo != "":
            recurso_canonico = f"/{self.storage_account}/{self.container_bronze}/{caminho_limpo}"
        else:
            recurso_canonico = f"/{self.storage_account}/{self.container_bronze}"

        if parametros_query_ordenados != "":
            recurso_canonico = f"{recurso_canonico}\n{parametros_query_ordenados}"

        texto_para_assinar = (
            f"{metodo_http}\n"
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

        chave_decodificada = base64.b64decode(self.storage_key)
        assinatura_hmac = hmac.new(
            chave_decodificada,
            texto_para_assinar.encode("utf-8"),
            hashlib.sha256
        ).digest()
        assinatura_base64 = base64.b64encode(assinatura_hmac).decode("utf-8")

        cabecalhos = {
            "x-ms-date": data_rfc1123,
            "x-ms-version": versao_api_azure,
            "Authorization": f"SharedKey {self.storage_account}:{assinatura_base64}"
        }

        if texto_tamanho != "":
            cabecalhos["Content-Length"] = texto_tamanho
        if tipo_conteudo != "":
            cabecalhos["Content-Type"] = tipo_conteudo

        return cabecalhos

    def _assinar_put_azure_blob(
        self,
        caminho_blob: str,
        tamanho_bytes: int,
        tipo_conteudo: str,
        parametros_query_ordenados: str
    ) -> dict:
        """
        Gera os cabecalhos HTTP assinados com SharedKey (HMAC-SHA256) para operacoes PUT.
        """
        if tamanho_bytes > 0:
            texto_tamanho = str(tamanho_bytes)
        else:
            texto_tamanho = ""

        cabecalhos = self._assinar_requisicao_azure(
            metodo_http="PUT",
            caminho_recurso=caminho_blob,
            texto_tamanho=texto_tamanho,
            tipo_conteudo=tipo_conteudo,
            parametros_query_ordenados=parametros_query_ordenados
        )
        if tamanho_bytes == 0:
            cabecalhos["Content-Length"] = "0"

        return cabecalhos

    def upload_arquivo_para_landing_bronze(
        self,
        caminho_arquivo_local: str,
        caminho_relativo_landing: str
    ) -> str:
        """
        Envia um arquivo local (ex: .parquet baixado no Brasil) diretamente para a Landing Zone
        do container Bronze no Azure ADLS Gen2 em blocos de 4 MB (Put Block + Put Block List)
        e retorna a URI abfss:// pronta para leitura pelo Spark no Databricks.
        """
        arquivo_local = Path(caminho_arquivo_local).resolve()
        if arquivo_local.exists() is False:
            raise FileNotFoundError(
                f"[AdlsStorageConnector] Arquivo local nao encontrado para upload na Landing Zone: {arquivo_local}"
            )

        caminho_blob_limpo = str(caminho_relativo_landing).strip("/")
        url_base_blob = (
            f"https://{self.storage_account}.blob.core.windows.net/"
            f"{self.container_bronze}/{caminho_blob_limpo}"
        )

        tamanho_total_mb = arquivo_local.stat().st_size / (1024 * 1024)
        print(
            f"--> [Landing Zone ADLS] Enviando '{arquivo_local.name}' ({tamanho_total_mb:.2f} MB) "
            f"para {self.container_bronze}/{caminho_blob_limpo}..."
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
                cabecalhos_bloco = self._assinar_put_azure_blob(
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
                    timeout=settings.TIMEOUT_CONEXAO_SEGUNDOS
                )
                if resposta_bloco.status_code != 201:
                    raise RuntimeError(
                        f"[AdlsStorageConnector] Falha no upload do bloco {indice_bloco} para o Azure ADLS: "
                        f"Status {resposta_bloco.status_code} - {resposta_bloco.text}"
                    )

        linhas_xml = ['<?xml version="1.0" encoding="utf-8"?><BlockList>']
        for id_bloco in lista_ids_blocos:
            linhas_xml.append(f"<Latest>{id_bloco}</Latest>")
        linhas_xml.append("</BlockList>")
        corpo_xml_bytes = "".join(linhas_xml).encode("utf-8")

        cabecalhos_lista = self._assinar_put_azure_blob(
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
            timeout=settings.TIMEOUT_CONEXAO_SEGUNDOS
        )
        if resposta_final.status_code != 201:
            raise RuntimeError(
                f"[AdlsStorageConnector] Falha ao confirmar BlockList no Azure ADLS: "
                f"Status {resposta_final.status_code} - {resposta_final.text}"
            )

        uri_landing_abfss = settings.obter_abfss_uri(
            container_name=self.container_bronze,
            caminho_relativo=caminho_blob_limpo
        )
        print(f"--> [Landing Zone ADLS] Upload concluido com sucesso em: {uri_landing_abfss}")
        return uri_landing_abfss

    def excluir_caminho_landing_bronze(self, caminho_relativo_landing: str) -> None:
        """
        Implementa o padrao Transient Landing Zone: exclui recursivamente o arquivo ou
        diretorio temporario na Landing Zone (ex: 'landing') no ADLS Gen2 via DFS REST API
        apos a gravacao bem-sucedida da tabela Delta oficial da camada Bronze.
        """
        if caminho_relativo_landing is None or str(caminho_relativo_landing).strip() == "":
            raise ValueError(
                "[AdlsStorageConnector] O caminho relativo da Landing Zone nao pode ser vazio."
            )

        caminho_limpo = str(caminho_relativo_landing).strip("/")
        url_delete_dfs = (
            f"https://{self.storage_account}.dfs.core.windows.net/"
            f"{self.container_bronze}/{caminho_limpo}?recursive=true"
        )

        cabecalhos_delete = self._assinar_requisicao_azure(
            metodo_http="DELETE",
            caminho_recurso=caminho_limpo,
            texto_tamanho="",
            tipo_conteudo="",
            parametros_query_ordenados="recursive:true"
        )

        resposta_delete = requests.delete(
            url=url_delete_dfs,
            headers=cabecalhos_delete,
            timeout=settings.TIMEOUT_CONEXAO_SEGUNDOS
        )

        if resposta_delete.status_code in [200, 202, 404]:
            print(
                f"--> [Transient Landing Zone] Pasta/arquivo temporario removido do ADLS: "
                f"{self.container_bronze}/{caminho_limpo}"
            )
            return

        raise RuntimeError(
            f"[AdlsStorageConnector] Falha ao remover Landing Zone '{caminho_limpo}' no ADLS: "
            f"Status {resposta_delete.status_code} - {resposta_delete.text}"
        )
