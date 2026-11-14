import abc
import os
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from src.catalog_dictionary import TableContract
from src.connectors.adls_databricks_connector import AdlsDatabricksConnector


class BaseExtractor(abc.ABC):
    """
    Classe base abstrata para todos os extratores da plataforma Lakehouse.
    Oferece dois caminhos de ingestao para a camada Bronze:
      1. Caminho via Pandas DataFrame: run() -> extract() -> add_audit_metadata() -> save_and_catalog_bronze()
      2. Caminho Direto via Arquivo .parquet (sem Pandas): run_parquet_direct() -> extract_parquet_file() -> save_parquet_direct_to_bronze()
    """

    def __init__(self, source_name: str) -> None:
        if source_name is None:
            raise ValueError("[BaseExtractor] O parametro 'source_name' nao pode ser nulo.")

        texto_limpo = str(source_name).strip()
        if texto_limpo == "":
            raise ValueError("[BaseExtractor] O parametro 'source_name' nao pode ser vazio.")

        self.source_name: str = texto_limpo
        self.lakehouse_connector = AdlsDatabricksConnector()

    @abc.abstractmethod
    def extract(self) -> pd.DataFrame:
        """
        Metodo abstrato que retorna os dados extraidos como um DataFrame do Pandas.
        """
        pass

    def extract_parquet_file(self) -> str:
        """
        Metodo para fontes que ja fornecem arquivo .parquet pronto (ex: ANEEL).
        Baixa o arquivo .parquet no disco e retorna o caminho dele, sem converter para Pandas.
        """
        raise NotImplementedError(
            f"[BaseExtractor] O extrator '{self.source_name}' nao implementou 'extract_parquet_file()'."
        )

    def _validate_dataframe(
        self,
        dataframe_alvo: pd.DataFrame,
        contexto_operacao: str
    ) -> None:
        """
        Valida se o DataFrame do Pandas existe e possui linhas antes de prosseguir.
        """
        if dataframe_alvo is None:
            raise ValueError(
                f"[BaseExtractor] O DataFrame fornecido para '{contexto_operacao}' nao pode ser nulo."
            )

        if dataframe_alvo.empty is True:
            raise ValueError(
                f"[BaseExtractor] O DataFrame fornecido para '{contexto_operacao}' esta vazio (0 registros)."
            )

    def _validate_parquet_file(self, caminho_arquivo_parquet: str) -> None:
        """
        Valida se o arquivo .parquet baixado existe no disco e nao esta vazio (0 bytes).
        """
        if caminho_arquivo_parquet is None or str(caminho_arquivo_parquet).strip() == "":
            raise ValueError("[BaseExtractor] O caminho do arquivo .parquet retornado e vazio.")

        arquivo = Path(caminho_arquivo_parquet)
        if arquivo.exists() is False:
            raise FileNotFoundError(
                f"[BaseExtractor] Arquivo .parquet nao encontrado em: {caminho_arquivo_parquet}"
            )

        if arquivo.stat().st_size == 0:
            raise ValueError(
                f"[BaseExtractor] O arquivo .parquet baixado esta vazio (0 bytes): {caminho_arquivo_parquet}"
            )

    def add_audit_metadata(self, dataframe_bruto: pd.DataFrame) -> pd.DataFrame:
        """
        Adiciona colunas de auditoria (_ingestion_at e _source_name) ao DataFrame do Pandas.
        """
        self._validate_dataframe(
            dataframe_alvo=dataframe_bruto,
            contexto_operacao="injecao de metadados de auditoria"
        )

        dataframe_enriquecido = dataframe_bruto.copy()
        timestamp_atual_utc = datetime.now(timezone.utc).isoformat()

        dataframe_enriquecido["_ingestion_at"] = timestamp_atual_utc
        dataframe_enriquecido["_source_name"] = self.source_name

        return dataframe_enriquecido

    def save_and_catalog_bronze(
        self,
        dataframe_auditado: pd.DataFrame,
        contrato_tabela: TableContract,
        modo_carga: str = "full"
    ) -> str:
        """
        Persiste o DataFrame do Pandas na camada Bronze respeitando o modo 'full' ou 'incremental'.
        """
        self._validate_dataframe(
            dataframe_alvo=dataframe_auditado,
            contexto_operacao="gravacao Delta na Camada Bronze"
        )

        if contrato_tabela is None:
            raise ValueError("[BaseExtractor] O parametro 'contrato_tabela' nao pode ser nulo.")

        uri_final = self.lakehouse_connector.write_delta_bronze(
            df_pandas=dataframe_auditado,
            contrato_tabela=contrato_tabela,
            modo_carga=modo_carga
        )
        return uri_final

    def save_parquet_direct_to_bronze(
        self,
        caminho_arquivo_parquet: str,
        contrato_tabela: TableContract,
        modo_carga: str = "full"
    ) -> str:
        """
        Envia o arquivo .parquet diretamente para o Spark gravar na Bronze, sem converter para Pandas.
        """
        self._validate_parquet_file(caminho_arquivo_parquet=caminho_arquivo_parquet)

        if contrato_tabela is None:
            raise ValueError("[BaseExtractor] O parametro 'contrato_tabela' nao pode ser nulo.")

        uri_final = self.lakehouse_connector.write_parquet_direto_bronze(
            caminho_arquivo_parquet=caminho_arquivo_parquet,
            source_name=self.source_name,
            contrato_tabela=contrato_tabela,
            modo_carga=modo_carga
        )
        return uri_final

    def run(
        self,
        contrato_tabela: TableContract,
        modo_carga: str = "full"
    ) -> str:
        """
        OPCAO 1 (Fluxo via Pandas DataFrame):
        Executa extracao para Pandas, injeta auditoria e grava Delta na Bronze.
        """
        modo_validado = self.lakehouse_connector.validar_modo_carga(modo_carga=modo_carga)

        print("\n=======================================================")
        print(f"   INICIANDO INGESTAO BRONZE (VIA PANDAS): {contrato_tabela.table_name.upper()}   ")
        print(f"   Destino: {contrato_tabela.full_table_identifier}   ")
        print(f"   Modo de Carga: {modo_validado.upper()}   ")
        print("=======================================================")

        dataframe_bruto = self.extract()
        self._validate_dataframe(
            dataframe_alvo=dataframe_bruto,
            contexto_operacao=f"extracao da fonte {self.source_name}"
        )
        print(f"--> [BaseExtractor] Extracao concluida: {len(dataframe_bruto):,} registros coletados.")

        dataframe_auditado = self.add_audit_metadata(dataframe_bruto=dataframe_bruto)

        uri_persistida = self.save_and_catalog_bronze(
            dataframe_auditado=dataframe_auditado,
            contrato_tabela=contrato_tabela,
            modo_carga=modo_validado
        )

        print("--> [BaseExtractor] Ingestao Bronze (via Pandas) finalizada com sucesso!")
        print(f"--> [BaseExtractor] Caminho ADLS: {uri_persistida}")
        print("=======================================================\n")

        return uri_persistida

    def run_parquet_direct(
        self,
        contrato_tabela: TableContract,
        modo_carga: str = "full"
    ) -> str:
        """
        OPCAO 2 (Fluxo Direto de Arquivo .parquet sem Pandas):
        Baixa o arquivo .parquet bruto e entrega direto ao Spark para gravar na Bronze.
        """
        modo_validado = self.lakehouse_connector.validar_modo_carga(modo_carga=modo_carga)

        print("\n=======================================================")
        print(f"   INICIANDO INGESTAO BRONZE (PARQUET DIRETO): {contrato_tabela.table_name.upper()}   ")
        print(f"   Destino: {contrato_tabela.full_table_identifier}   ")
        print(f"   Modo de Carga: {modo_validado.upper()}   ")
        print("=======================================================")

        caminho_parquet_temp = self.extract_parquet_file()
        self._validate_parquet_file(caminho_arquivo_parquet=caminho_parquet_temp)

        try:
            uri_persistida = self.save_parquet_direct_to_bronze(
                caminho_arquivo_parquet=caminho_parquet_temp,
                contrato_tabela=contrato_tabela,
                modo_carga=modo_validado
            )
        finally:
            if os.path.exists(caminho_parquet_temp) is True:
                os.remove(caminho_parquet_temp)
                print(f"--> [BaseExtractor] Arquivo temporario removido: {caminho_parquet_temp}")

        print("--> [BaseExtractor] Ingestao Bronze (Parquet Direto) finalizada com sucesso!")
        print(f"--> [BaseExtractor] Caminho ADLS: {uri_persistida}")
        print("=======================================================\n")

        return uri_persistida