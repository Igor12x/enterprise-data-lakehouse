from io import BytesIO
from typing import Optional
import requests
import pandas as pd

from src.ingestion.base_extractor import BaseExtractor
from src.config import settings


class AneelGdExtractor(BaseExtractor):
    """
    Extrator de Geracao Distribuida da ANEEL consumindo o recurso
    oficial Apache Parquet via portal CKAN de Dados Abertos.
    """

    # URL oficial extraida do recurso homologado no portal ANEEL
    URL_PARQUET_OFICIAL = (
        "https://dadosabertos.aneel.gov.br/dataset/5e0fafd2-21b9-4d5b-b622-40438d40aba2/"
        "resource/cd29f6eb-e08d-4db7-b6fb-ed6e3b682d27/download/"
        "empreendimento-geracao-distribuida.parquet"
    )

    def __init__(self, url_origem: Optional[str] = None):
        # Inicializa o contrato da classe base definindo o nome da fonte
        super().__init__(source_name="aneel_gd")

        if url_origem is not None and str(url_origem).strip() != "":
            self.url_origem = str(url_origem).strip()
        else:
            self.url_origem = self.URL_PARQUET_OFICIAL

        # Configuracao de sessao HTTP com User-Agent corporativo
        self.sessao_http = requests.Session()
        self.sessao_http.headers.update({
            "User-Agent": "Enterprise-Data-Lakehouse/1.0 (Analytics Ingestion Pipeline)",
            "Accept": "application/octet-stream"
        })

    def extract(self) -> pd.DataFrame:
        """
        Executa o download do fluxo binario do Parquet governamental
        e decodifica em DataFrame colunar na memoria RAM.
        """
        print(f"--> [AneelGdExtractor] Conectando a fonte: {self.url_origem}")

        try:
            # Download via stream para garantir integridade do arquivo
            resposta = self.sessao_http.get(
                url=self.url_origem,
                timeout=settings.TIMEOUT_CONEXAO_SEGUNDOS,
                stream=True
            )
            resposta.raise_for_status()

            print("--> [AneelGdExtractor] Baixando fluxo de dados binarios...")
            buffer_memoria = BytesIO()
            for pedaco in resposta.iter_content(chunk_size=1024 * 1024):  # Blocos de 1MB
                if pedaco:
                    buffer_memoria.write(pedaco)

            # Posiciona o ponteiro de leitura no inicio do buffer
            buffer_memoria.seek(0)

        except requests.exceptions.RequestException as erro_rede:
            mensagem_falha = f"[AneelGdExtractor] Falha no download do Parquet ANEEL: {str(erro_rede)}"
            raise RuntimeError(mensagem_falha)

        print("--> [AneelGdExtractor] Decodificando Parquet com PyArrow...")
        df_dados = pd.read_parquet(buffer_memoria, engine="pyarrow")

        print(f"--> [AneelGdExtractor] Carga concluida: {len(df_dados):,} registros extraidos com sucesso.")
        return df_dados