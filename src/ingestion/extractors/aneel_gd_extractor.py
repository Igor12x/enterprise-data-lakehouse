from io import BytesIO
import os
from pathlib import Path
import tempfile
import requests
import pandas as pd
from src.ingestion.base_extractor import BaseExtractor
from src.connectors.adls_storage_connector import AdlsStorageConnector
from src.config import settings


class AneelGdExtractor(BaseExtractor):
    """
    Extrator oficial de Geracao Distribuida da ANEEL.
    Suporta tres formas de extracao:
      1. extract(): Metodo tradicional que baixa o .parquet e converte para DataFrame do Pandas em memoria.
      2. extract_parquet_file(): Metodo tradicional que baixa o .parquet direto para disco local sem Pandas.
      3. extract_landing_brasil_uri(): Opcao para contornar restricao de IP no Brasil (Geo-Block),
         utilizando o arquivo .parquet sincronizado na Landing Zone do Azure ADLS Gen2 (abfss://).
    """

    URL_PARQUET_OFICIAL = (
        "https://dadosabertos.aneel.gov.br/dataset/5e0fafd2-21b9-4d5b-b622-40438d40aba2/"
        "resource/cd29f6eb-e08d-4db7-b6fb-ed6e3b682d27/download/"
        "empreendimento-geracao-distribuida.parquet"
    )

    CAMINHO_LANDING_ADLS = "landing/aneel_gd/empreendimento-geracao-distribuida.parquet"

    def __init__(self, url_origem: str = None) -> None:
        super().__init__(source_name="aneel_gd")
        self.adls_storage_connector = AdlsStorageConnector()

        if url_origem is not None and str(url_origem).strip() != "":
            self.url_origem = str(url_origem).strip()
        else:
            self.url_origem = self.URL_PARQUET_OFICIAL

        self.sessao_http = requests.Session()
        self.sessao_http.headers.update({
            "User-Agent": "Enterprise-Data-Lakehouse/1.0 (Analytics Ingestion Pipeline)",
            "Accept": "application/octet-stream"
        })

    def disparar_sync_function_brasil(self, url_function: str = None) -> dict:
        """
        Aciona obrigatoriamente a Azure Function hospedada em Brazil South (IP brasileiro)
        para baixar o .parquet atualizado da ANEEL na hora e sobrescrever o arquivo unico
        na Landing Zone do ADLS Gen2, evitando qualquer atualizacao falsa com dados antigos.
        """
        url_alvo = None

        if url_function is not None and str(url_function).strip() != "":
            url_alvo = str(url_function).strip()
        else:
            url_configurada = settings.obter_url_function_aneel_br()
            if url_configurada is not None and str(url_configurada).strip() != "":
                url_alvo = str(url_configurada).strip()

        if url_alvo is None or url_alvo == "":
            raise ValueError(
                "[AneelGdExtractor] A URL da Azure Function (ANEEL_BR_FUNCTION_URL / "
                "aneel-br-function-url no Key Vault) nao foi encontrada. "
                "Execucao bloqueada para evitar leitura de dados antigos no ADLS."
            )

        print("--> [AneelGdExtractor] Acionando Azure Function em Brazil South para buscar novos dados na ANEEL...")

        try:
            resposta = self.sessao_http.post(
                url=url_alvo,
                timeout=300
            )
            resposta.raise_for_status()
            dados_retorno = resposta.json()
        except requests.exceptions.RequestException as erro_http:
            raise RuntimeError(
                f"[AneelGdExtractor] Falha ao acionar Azure Function em Brazil South: {str(erro_http)}"
            )

        status_retorno = dados_retorno.get("status", "")
        if status_retorno != "sucesso":
            mensagem_falha = dados_retorno.get("mensagem", "Erro desconhecido na Azure Function.")
            raise RuntimeError(
                f"[AneelGdExtractor] A Azure Function retornou erro: {mensagem_falha}"
            )

        tamanho_mb = dados_retorno.get("tamanho_mb", "?")
        uri_landing = dados_retorno.get("uri_landing_abfss", "")
        print(
            f"--> [AneelGdExtractor] Download na ANEEL via Brazil South concluido | "
            f"Tamanho: {tamanho_mb} MB | Arquivo atualizado: {uri_landing}"
        )
        return dados_retorno

    def extract_landing_brasil_uri(self) -> str:
        """
        Opcao dedicada para fontes que exigem IP no Brasil:
        Dispara obrigatoriamente a Azure Function em Brazil South em toda execucao (FULL ou INCREMENTAL)
        para buscar os dados mais recentes direto na ANEEL antes de o Spark ler a Landing Zone.
        """
        dados_retorno = self.disparar_sync_function_brasil()
        uri_retornada = dados_retorno.get("uri_landing_abfss", "")

        if uri_retornada is not None and str(uri_retornada).strip() != "":
            return str(uri_retornada).strip()

        uri_landing = settings.obter_abfss_uri(
            container_name=settings.ADLS_CONTAINER_BRONZE,
            caminho_relativo=self.CAMINHO_LANDING_ADLS
        )
        return uri_landing

    def sincronizar_parquet_para_landing_adls(self) -> str:
        """
        Sincroniza o .parquet oficial da ANEEL para a Landing Zone do Azure ADLS Gen2.
        Se a Azure Function em Brazil South estiver configurada, delega para a nuvem;
        caso contrario, executa o download local e upload em blocos de 4 MB.
        """
        if settings.ANEEL_BR_FUNCTION_URL is not None and str(settings.ANEEL_BR_FUNCTION_URL).strip() != "":
            dados_retorno = self.disparar_sync_function_brasil(
                url_function=settings.ANEEL_BR_FUNCTION_URL
            )
            uri_retornada = dados_retorno.get("uri_landing_abfss", "")
            if uri_retornada != "":
                return uri_retornada

        caminho_local_temp = self.extract_parquet_file()

        try:
            uri_landing = self.adls_storage_connector.upload_arquivo_para_landing_bronze(
                caminho_arquivo_local=caminho_local_temp,
                caminho_relativo_landing=self.CAMINHO_LANDING_ADLS
            )
        finally:
            if os.path.exists(caminho_local_temp) is True:
                os.remove(caminho_local_temp)

        return uri_landing

    def extract_parquet_file(self, caminho_destino: str = None) -> str:
        """
        Metodo tradicional: Faz o download do arquivo .parquet da ANEEL em blocos de 1 MB
        salvando direto no disco local, sem carregar na memoria RAM e sem converter para Pandas.
        """
        if caminho_destino is not None and str(caminho_destino).strip() != "":
            arquivo_saida = Path(str(caminho_destino).strip()).resolve()
        else:
            pasta_temporaria = Path(tempfile.gettempdir())
            arquivo_saida = pasta_temporaria / "aneel_gd_empreendimentos.parquet"

        print(f"--> [AneelGdExtractor] Conectando na ANEEL (Parquet Direto): {self.url_origem}")

        try:
            resposta_http = self.sessao_http.get(
                url=self.url_origem,
                timeout=settings.TIMEOUT_CONEXAO_SEGUNDOS,
                stream=True
            )
            resposta_http.raise_for_status()

            print(f"--> [AneelGdExtractor] Salvando .parquet direto em: {arquivo_saida}")
            tamanho_bloco_1mb = 1024 * 1024

            with open(arquivo_saida, mode="wb") as arquivo_fisico:
                for bloco_bytes in resposta_http.iter_content(chunk_size=tamanho_bloco_1mb):
                    if bloco_bytes:
                        arquivo_fisico.write(bloco_bytes)

        except requests.exceptions.RequestException as erro_rede:
            mensagem_erro = f"[AneelGdExtractor] Erro no download da ANEEL: {str(erro_rede)}"
            raise RuntimeError(mensagem_erro)

        tamanho_mb = arquivo_saida.stat().st_size / (1024 * 1024)
        print(f"--> [AneelGdExtractor] Download .parquet concluido ({tamanho_mb:.2f} MB).")

        return str(arquivo_saida)

    def extract(self) -> pd.DataFrame:
        """
        Mantido para compatibilidade: Faz o download do arquivo Parquet em memoria
        e converte em um DataFrame do Pandas.
        """
        print(f"--> [AneelGdExtractor] Conectando na ANEEL (Modo Pandas): {self.url_origem}")

        try:
            resposta_http = self.sessao_http.get(
                url=self.url_origem,
                timeout=settings.TIMEOUT_CONEXAO_SEGUNDOS,
                stream=True
            )
            resposta_http.raise_for_status()

            print("--> [AneelGdExtractor] Baixando arquivo Parquet na memoria...")
            buffer_memoria = BytesIO()
            tamanho_bloco_1mb = 1024 * 1024

            for bloco_bytes in resposta_http.iter_content(chunk_size=tamanho_bloco_1mb):
                if bloco_bytes:
                    buffer_memoria.write(bloco_bytes)

            buffer_memoria.seek(0)

        except requests.exceptions.RequestException as erro_rede:
            mensagem_erro = f"[AneelGdExtractor] Erro no download da ANEEL: {str(erro_rede)}"
            raise RuntimeError(mensagem_erro)

        print("--> [AneelGdExtractor] Convertendo Parquet para Pandas com PyArrow...")
        dataframe_dados = pd.read_parquet(buffer_memoria, engine="pyarrow")
        print(f"--> [AneelGdExtractor] Conversao concluida: {len(dataframe_dados):,} linhas.")

        return dataframe_dados
