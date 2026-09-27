import abc
from datetime import datetime, timezone
import pandas as pd

from typing import Optional
from src.connectors.motherduck_connector import MotherDuckConnector
from src.config import settings
from src.connectors.r2_connector import R2Connector


class BaseExtractor(abc.ABC):
    """
    Classe base abstrata para todos os extratores da plataforma Lakehouse.
    Aplica os padroes Template Method e Strategy Pattern para padronizar
    a ingestao de dados, injecao de metadados de auditoria e persistencia na camada Bronze.
    """

    def __init__(self, source_name: str):
        # 1. Validacao do identificador da fonte de dados
        if source_name is None:
            raise ValueError("[BaseExtractor] O parametro 'source_name' nao pode ser nulo.")

        texto_limpo = str(source_name).strip()
        if texto_limpo == "":
            raise ValueError("[BaseExtractor] O parametro 'source_name' nao pode ser vazio.")

        self.source_name = texto_limpo

        # 2. Inicializacao do conector de armazenamento para a camada Bronze
        self.r2_connector = R2Connector()
        # Inicializacao do conector do MotherDuck
        self.motherduck_connector = MotherDuckConnector()

    @abc.abstractmethod
    def extract(self) -> pd.DataFrame:
        """
        Metodo abstrato que OBRIGA todas as classes filhas (APIs, Scrapers, Bancos)
        a implementarem sua propria logica de extracao e retornarem um DataFrame Pandas.
        """
        pass

    def _validate_dataframe(self, df: pd.DataFrame, contexto_operacao: str) -> None:
        """
        Metodo privado centralizador de validacao (Principio DRY).
        Garante em um unico ponto que DataFrames nulos ou vazios sejam
        bloqueados antes de qualquer processamento ou gravacao.
        """
        if df is None:
            raise ValueError(
                f"[BaseExtractor] O DataFrame fornecido para '{contexto_operacao}' nao pode ser nulo."
            )

        if df.empty:
            raise ValueError(
                f"[BaseExtractor] O DataFrame fornecido para '{contexto_operacao}' esta vazio (0 registros)."
            )

    def add_audit_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adiciona metadados corporativos de linhagem e rastreabilidade:
        - _ingestion_at: Timestamp UTC exato da extracao.
        - _source_name: Identificador semantico da origem do dado.
        """
        # Validacao centralizada
        self._validate_dataframe(df, contexto_operacao="injecao de metadados")

        # Cria uma copia defensiva para evitar alteracoes colaterais indesejadas
        df_enriquecido = df.copy()

        # Injeta colunas de governanca no padrao Medallion Bronze
        timestamp_atual = datetime.now(timezone.utc).isoformat()
        df_enriquecido["_ingestion_at"] = timestamp_atual
        df_enriquecido["_source_name"] = self.source_name

        return df_enriquecido

    def save_raw(self, df: pd.DataFrame, target_key: str) -> str:
        """
        Persiste o DataFrame tratado com metadados diretamente no bucket Bronze
        do Cloudflare R2 utilizando compressao Parquet.

        Parametros:
            df (pd.DataFrame): Dados com colunas de negocio e auditoria.
            target_key (str): Caminho semantico no Lakehouse (ex: 'aneel/gd/ano=2026/dados.parquet').

        Retorno:
            str: URI padronizada do arquivo gravado no R2.
        """
        # Validacao centralizada
        self._validate_dataframe(df, contexto_operacao="gravacao na Bronze")

        if target_key is None:
            raise ValueError("[BaseExtractor] A chave de destino 'target_key' deve ser informada.")

        caminho_limpo = str(target_key).strip()
        if caminho_limpo == "":
            raise ValueError("[BaseExtractor] A chave de destino 'target_key' nao pode ser vazia.")

        print(f"--> [BaseExtractor] Iniciando persistencia na camada Bronze: {caminho_limpo}")
        
        uri_final = self.r2_connector.upload_parquet(
            df=df,
            target_key=caminho_limpo
        )

        return uri_final
    
    def catalog_bronze(
        self,
        schema_name: str,
        target_key: str,
        criar_como_tabela: bool = False
    ) -> None:
        """
        Registra o objeto no MotherDuck apontando diretamente para o arquivo no R2.
        Ex: s3://lakehouse-bronze/aneel/gd/empreendimentos_gd.parquet
        """
        caminho_s3_direto = f"s3://{settings.R2_BUCKET_BRONZE}/{target_key}"

        self.motherduck_connector.registrar_tabela_bronze(
            schema_name=schema_name,
            nome_tabela=self.source_name,
            origem_s3_padrao=caminho_s3_direto,
            criar_como_tabela_fisica=criar_como_tabela
        )

    def run(
        self,
        target_key: str,
        schema_name: str = "gd",
        criar_como_tabela_fisica: bool = False
    ) -> str:
        """
        Template Method: Ingestao completa na camada Bronze.
        """
        print(f"\n=======================================================")
        print(f"   INICIANDO PIPELINE DE INGESTAO: {self.source_name.upper()}   ")
        print(f"=======================================================")

        # 1. Extracao
        df_bruto = self.extract()
        self._validate_dataframe(df_bruto, contexto_operacao=f"extracao de {self.source_name}")
        print(f"--> [BaseExtractor] Extracao concluida: {len(df_bruto):,} registros coletados.")

        # 2. Auditoria
        df_auditado = self.add_audit_metadata(df=df_bruto)

        # 3. Persistencia no R2
        uri_arquivo = self.save_raw(df=df_auditado, target_key=target_key)
        print(f"--> [BaseExtractor] Arquivo salvo no R2: {uri_arquivo}")

        # 4. Catalogacao no MotherDuck (bronze.<schema>.<tabela>)
        self.catalog_bronze(
            schema_name=schema_name,
            target_key=target_key,
            criar_como_tabela=criar_como_tabela_fisica
        )

        print(f"--> [BaseExtractor] Ingestao e catalogacao finalizadas com sucesso!")
        print(f"=======================================================\n")

        return uri_arquivo