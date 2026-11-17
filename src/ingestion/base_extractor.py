import abc
import os
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from src.catalog_dictionary import TableContract
from src.persistence.delta_catalog_manager import DeltaCatalogManager


class BaseExtractor(abc.ABC):
    """
    Classe base abstrata para todos os extratores da plataforma Lakehouse.
    Oferece tres caminhos de ingestao para a camada Bronze:
      1. Caminho Tradicional via Pandas DataFrame:
         run() -> extract() -> add_audit_metadata() -> save_and_catalog_bronze()
      2. Caminho Tradicional Direto via Arquivo .parquet Local (sem Pandas):
         run_parquet_direct() -> extract_parquet_file() -> save_parquet_direct_to_bronze()
      3. Caminho para Fontes que Exigem IP no Brasil (via Landing Zone ADLS Gen2):
         run_landing_brasil() -> extract_landing_brasil_uri() -> save_landing_brasil_to_bronze()
    """

    def __init__(self, source_name: str) -> None:
        if source_name is None:
            raise ValueError("[BaseExtractor] O parametro 'source_name' nao pode ser nulo.")

        texto_limpo = str(source_name).strip()
        if texto_limpo == "":
            raise ValueError("[BaseExtractor] O parametro 'source_name' nao pode ser vazio.")

        self.source_name: str = texto_limpo
        self.delta_catalog_manager = DeltaCatalogManager()
        self.lakehouse_connector = self.delta_catalog_manager

    @abc.abstractmethod
    def extract(self) -> pd.DataFrame:
        """
        Metodo abstrato que retorna os dados extraidos como um DataFrame do Pandas.
        """
        pass

    def extract_parquet_file(self) -> str:
        """
        Metodo tradicional para fontes que ja fornecem arquivo .parquet pronto (ex: ANEEL).
        Baixa o arquivo .parquet no disco e retorna o caminho dele, sem converter para Pandas.
        """
        raise NotImplementedError(
            f"[BaseExtractor] O extrator '{self.source_name}' nao implementou 'extract_parquet_file()'."
        )

    def extract_landing_brasil_uri(self) -> str:
        """
        Metodo para fontes que bloqueiam IPs fora do Brasil (Geo-Block em datacenters nos EUA).
        Retorna a URI abfss:// do arquivo previamente sincronizado na Landing Zone do ADLS Gen2.
        """
        raise NotImplementedError(
            f"[BaseExtractor] O extrator '{self.source_name}' nao implementou 'extract_landing_brasil_uri()'."
        )

    def sincronizar_parquet_para_landing_adls(self) -> str:
        """
        Baixa o arquivo .parquet da fonte e envia para a Landing Zone do ADLS Gen2.
        """
        raise NotImplementedError(
            f"[BaseExtractor] O extrator '{self.source_name}' nao implementou "
            f"'sincronizar_parquet_para_landing_adls()'."
        )

    def limpar_landing_brasil(self, uri_landing_abfss: str) -> None:
        """
        Hook opcional (padrao Transient Landing Zone) acionado apos a gravacao bem-sucedida
        na tabela Delta da camada Bronze para remover arquivos temporarios de staging.
        """
        return None

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
        Valida se o arquivo .parquet baixado existe no disco local e nao esta vazio (0 bytes).
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

    def _validate_uri_landing_brasil(self, uri_landing_abfss: str) -> None:
        """
        Valida se a URI abfss:// da Landing Zone para fontes que exigem IP no Brasil e valida.
        """
        if uri_landing_abfss is None or str(uri_landing_abfss).strip() == "":
            raise ValueError("[BaseExtractor] A URI abfss:// da Landing Zone retornada e vazia.")

        uri_limpa = str(uri_landing_abfss).strip()
        if uri_limpa.startswith("abfss://") is False:
            raise ValueError(
                f"[BaseExtractor] A URI da Landing Zone deve iniciar com 'abfss://'. Recebido: {uri_limpa}"
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
        Metodo tradicional: envia o arquivo .parquet local diretamente para o Spark gravar na Bronze,
        sem converter para Pandas.
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

    def save_landing_brasil_to_bronze(
        self,
        uri_landing_abfss: str,
        contrato_tabela: TableContract,
        modo_carga: str = "full"
    ) -> str:
        """
        Opcao dedicada para fontes que exigem IP no Brasil: solicita ao Spark a leitura direta
        do arquivo .parquet da Landing Zone abfss:// no ADLS Gen2 e grava na camada Bronze.
        """
        self._validate_uri_landing_brasil(uri_landing_abfss=uri_landing_abfss)

        if contrato_tabela is None:
            raise ValueError("[BaseExtractor] O parametro 'contrato_tabela' nao pode ser nulo.")

        uri_final = self.lakehouse_connector.write_parquet_landing_brasil_bronze(
            uri_landing_abfss=uri_landing_abfss,
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
        OPCAO 1 (Fluxo Tradicional via Pandas DataFrame):
        Executa extracao para Pandas, injeta auditoria e grava Delta na Bronze.
        """
        modo_limpo = str(modo_carga).strip().lower()

        print("\n=======================================================")
        print(f"   INICIANDO INGESTAO BRONZE (VIA PANDAS): {contrato_tabela.table_name.upper()}   ")
        print(f"   Destino: {contrato_tabela.full_table_identifier}   ")
        print(f"   Modo de Carga: {modo_limpo.upper()}   ")
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
            modo_carga=modo_limpo
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
        OPCAO 2 (Fluxo Tradicional Direto de Arquivo .parquet Local sem Pandas):
        Baixa o arquivo .parquet bruto no disco e entrega direto ao Spark para gravar na Bronze.
        """
        modo_limpo = str(modo_carga).strip().lower()

        print("\n=======================================================")
        print(f"   INICIANDO INGESTAO BRONZE (PARQUET DIRETO): {contrato_tabela.table_name.upper()}   ")
        print(f"   Destino: {contrato_tabela.full_table_identifier}   ")
        print(f"   Modo de Carga: {modo_limpo.upper()}   ")
        print("=======================================================")

        caminho_parquet_temp = self.extract_parquet_file()
        self._validate_parquet_file(caminho_arquivo_parquet=caminho_parquet_temp)

        try:
            uri_persistida = self.save_parquet_direct_to_bronze(
                caminho_arquivo_parquet=caminho_parquet_temp,
                contrato_tabela=contrato_tabela,
                modo_carga=modo_limpo
            )
        finally:
            if os.path.exists(caminho_parquet_temp) is True:
                os.remove(caminho_parquet_temp)
                print(f"--> [BaseExtractor] Arquivo temporario removido: {caminho_parquet_temp}")

        print("--> [BaseExtractor] Ingestao Bronze (Parquet Direto) finalizada com sucesso!")
        print(f"--> [BaseExtractor] Caminho ADLS: {uri_persistida}")
        print("=======================================================\n")

        return uri_persistida

    def run_landing_brasil(
        self,
        contrato_tabela: TableContract,
        modo_carga: str = "full"
    ) -> str:
        """
        OPCAO 3 (Fluxo para Fontes que Exigem IP no Brasil via Landing Zone ADLS Gen2):
        Lê o arquivo .parquet previamente sincronizado na Landing Zone (abfss://) por uma maquina
        com IP brasileiro e entrega direto ao Spark no Databricks para gravar na Bronze.
        """
        modo_limpo = str(modo_carga).strip().lower()

        print("\n=======================================================")
        print(f"   INICIANDO INGESTAO BRONZE (LANDING ZONE IP BRASIL): {contrato_tabela.table_name.upper()}   ")
        print(f"   Destino: {contrato_tabela.full_table_identifier}   ")
        print(f"   Modo de Carga: {modo_limpo.upper()}   ")
        print("=======================================================")

        uri_landing_abfss = self.extract_landing_brasil_uri()
        self._validate_uri_landing_brasil(uri_landing_abfss=uri_landing_abfss)

        uri_persistida = self.save_landing_brasil_to_bronze(
            uri_landing_abfss=uri_landing_abfss,
            contrato_tabela=contrato_tabela,
            modo_carga=modo_limpo
        )

        self.limpar_landing_brasil(uri_landing_abfss=uri_landing_abfss)

        print("--> [BaseExtractor] Ingestao Bronze (Landing Zone IP Brasil) finalizada com sucesso!")
        print(f"--> [BaseExtractor] Caminho ADLS: {uri_persistida}")
        print("=======================================================\n")

        return uri_persistida