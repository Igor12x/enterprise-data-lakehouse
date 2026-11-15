from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from src.catalog_dictionary import TableContract
from src.config import settings
from src.infrastructure.databricks_runtime import DatabricksRuntime


class DeltaCatalogManager:
    """
    Responsabilidade Unica (Persistencia Delta Lake e Governanca no Unity Catalog):
    Gerencia a criacao de schemas e tabelas externas no Unity Catalog e executa a
    gravacao de dados na camada Bronze em formato Delta Lake (modo 'full' Overwrite
    ou 'incremental' MERGE INTO).
    """

    def __init__(self, runtime: DatabricksRuntime = None) -> None:
        if runtime is not None:
            self.runtime = runtime
        else:
            self.runtime = DatabricksRuntime()

        self.container_bronze = settings.ADLS_CONTAINER_BRONZE

    def _formatar_identificador_sql(self, identificador: str) -> str:
        """
        Envolve cada parte do identificador (catalogo.schema.tabela) com crases (`)
        para suportar hifens no Unity Catalog (ex: `portfolio-lakehouse-bronze`.`aneel`.`tabela`).
        """
        partes = str(identificador).split(".")
        partes_formatadas = []

        for parte in partes:
            parte_limpa = parte.strip().strip("`")
            partes_formatadas.append(f"`{parte_limpa}`")

        return ".".join(partes_formatadas)

    def verificar_se_tabela_existe(self, identificador_completo_sql: str) -> bool:
        """
        Consulta o Unity Catalog do Databricks para saber se uma tabela ja foi criada.
        """
        spark = self.runtime.obter_sessao_spark()
        identificador_formatado = self._formatar_identificador_sql(identificador_completo_sql)
        tabela_existe = spark.catalog.tableExists(identificador_formatado)
        return tabela_existe

    def garantir_schema_catalogo(self, schema_name: str, catalog_name: str = None) -> None:
        """
        Cria o banco/schema (ex: aneel, gd, localidade, tempo) no Unity Catalog caso ainda nao exista.
        """
        if catalog_name is not None and str(catalog_name).strip() != "":
            catalogo_alvo = str(catalog_name).strip()
        else:
            catalogo_alvo = settings.DATABRICKS_CATALOG_BRONZE

        spark = self.runtime.obter_sessao_spark()
        identificador_schema = self._formatar_identificador_sql(f"{catalogo_alvo}.{schema_name}")
        comando_sql = f"CREATE SCHEMA IF NOT EXISTS {identificador_schema};"
        spark.sql(comando_sql)

    def registrar_tabela_catalogo(
        self,
        schema_name: str,
        table_name: str,
        uri_abfss: str,
        catalog_name: str = None
    ) -> None:
        """
        Vincula a pasta fisica do Azure Data Lake a uma tabela SQL visivel no Databricks e Power BI.
        """
        if catalog_name is not None and str(catalog_name).strip() != "":
            catalogo_alvo = str(catalog_name).strip()
        else:
            catalogo_alvo = settings.DATABRICKS_CATALOG_BRONZE

        spark = self.runtime.obter_sessao_spark()
        identificador_tabela = self._formatar_identificador_sql(
            f"{catalogo_alvo}.{schema_name}.{table_name}"
        )

        comando_sql = f"""
        CREATE TABLE IF NOT EXISTS {identificador_tabela}
        USING DELTA
        LOCATION '{uri_abfss}'
        """
        spark.sql(comando_sql)

    def _salvar_dataframe_spark_na_bronze(
        self,
        dataframe_spark: SparkDataFrame,
        contrato_tabela: TableContract,
        modo_carga: str
    ) -> str:
        """
        Metodo interno compartilhado: recebe um Spark DataFrame e executa a gravacao
        na camada Bronze em modo 'full' (Overwrite) ou 'incremental' (MERGE INTO).
        """
        modo_limpo = str(modo_carga).strip().lower()
        caminho_limpo = str(contrato_tabela.adls_relative_path).strip("/")

        uri_abfss = settings.obter_abfss_uri(
            container_name=self.container_bronze,
            caminho_relativo=caminho_limpo
        )

        spark = self.runtime.obter_sessao_spark()
        self.garantir_schema_catalogo(
            schema_name=contrato_tabela.schema_name,
            catalog_name=contrato_tabela.catalog_name
        )
        tabela_ja_existe = self.verificar_se_tabela_existe(
            identificador_completo_sql=contrato_tabela.full_table_identifier
        )

        if modo_limpo == "full" or tabela_ja_existe is False:
            print(f"--> [DeltaCatalogManager] Modo FULL: Gravando tabela Delta completa em {uri_abfss}")
            (
                dataframe_spark.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(uri_abfss)
            )
            self.registrar_tabela_catalogo(
                schema_name=contrato_tabela.schema_name,
                table_name=contrato_tabela.table_name,
                uri_abfss=uri_abfss,
                catalog_name=contrato_tabela.catalog_name
            )
        else:
            print(
                f"--> [DeltaCatalogManager] Modo INCREMENTAL: Executando MERGE em "
                f"{contrato_tabela.full_table_identifier}"
            )
            nome_view_temp = f"view_temp_bronze_{contrato_tabela.table_name}"
            coluna_pk = contrato_tabela.primary_key
            identificador_destino_sql = self._formatar_identificador_sql(
                contrato_tabela.full_table_identifier
            )

            dataframe_sem_duplicatas = dataframe_spark.dropDuplicates([coluna_pk])
            dataframe_sem_duplicatas.createOrReplaceTempView(nome_view_temp)

            sql_merge = f"""
            MERGE INTO {identificador_destino_sql} AS destino
            USING {nome_view_temp} AS origem
            ON destino.{coluna_pk} = origem.{coluna_pk}
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
            """
            spark.sql(sql_merge)

        return uri_abfss

    def write_delta_bronze(
        self,
        df_pandas: pd.DataFrame,
        contrato_tabela: TableContract,
        modo_carga: str = "full"
    ) -> str:
        """
        OPCAO 1 (Com conversao Pandas -> Spark):
        Recebe um DataFrame do Pandas em memoria, converte para Spark e grava na Bronze.
        """
        if df_pandas is None:
            raise ValueError("[DeltaCatalogManager] O DataFrame Pandas enviado para a Bronze e nulo.")

        if df_pandas.empty is True:
            raise ValueError("[DeltaCatalogManager] O DataFrame Pandas enviado para a Bronze esta vazio.")

        spark = self.runtime.obter_sessao_spark()
        print(f"--> [Bronze via Pandas] Convertendo {len(df_pandas):,} linhas para Spark DataFrame...")
        dataframe_spark = spark.createDataFrame(df_pandas)

        uri_abfss = self._salvar_dataframe_spark_na_bronze(
            dataframe_spark=dataframe_spark,
            contrato_tabela=contrato_tabela,
            modo_carga=modo_carga
        )
        return uri_abfss

    def write_parquet_direto_bronze(
        self,
        caminho_arquivo_parquet: str,
        source_name: str,
        contrato_tabela: TableContract,
        modo_carga: str = "full"
    ) -> str:
        """
        OPCAO 2 (Metodo tradicional de leitura direta de .parquet local sem Pandas):
        Lê um arquivo .parquet local diretamente pelo motor do Spark, adiciona as colunas de
        auditoria (_ingestion_at e _source_name) e grava na Bronze.
        """
        if caminho_arquivo_parquet is None or str(caminho_arquivo_parquet).strip() == "":
            raise ValueError("[DeltaCatalogManager] O caminho do arquivo .parquet nao pode ser vazio.")

        arquivo_fisico = Path(caminho_arquivo_parquet).resolve()
        if arquivo_fisico.exists() is False:
            raise FileNotFoundError(
                f"[DeltaCatalogManager] O arquivo .parquet nao foi encontrado em: {arquivo_fisico}"
            )

        spark = self.runtime.obter_sessao_spark()
        uri_arquivo_local = arquivo_fisico.as_uri()

        print(f"--> [Bronze via Parquet Direto] Lendo arquivo nativamente no Spark: {uri_arquivo_local}")
        dataframe_spark_bruto = spark.read.parquet(uri_arquivo_local)

        timestamp_atual_utc = datetime.now(timezone.utc).isoformat()
        dataframe_com_data = dataframe_spark_bruto.withColumn(
            "_ingestion_at",
            F.lit(timestamp_atual_utc)
        )
        dataframe_spark_auditado = dataframe_com_data.withColumn(
            "_source_name",
            F.lit(str(source_name).strip())
        )

        uri_abfss = self._salvar_dataframe_spark_na_bronze(
            dataframe_spark=dataframe_spark_auditado,
            contrato_tabela=contrato_tabela,
            modo_carga=modo_carga
        )
        return uri_abfss

    def write_parquet_landing_brasil_bronze(
        self,
        uri_landing_abfss: str,
        source_name: str,
        contrato_tabela: TableContract,
        modo_carga: str = "full"
    ) -> str:
        """
        OPCAO 3 (Exclusiva para extracoes que exigem IP no Brasil via Landing Zone ADLS Gen2):
        Lê o arquivo .parquet diretamente da Landing Zone (abfss://) no Azure ADLS Gen2 pelo Spark,
        adiciona as colunas de auditoria (_ingestion_at e _source_name) e grava na camada Bronze.
        """
        if uri_landing_abfss is None or str(uri_landing_abfss).strip() == "":
            raise ValueError("[DeltaCatalogManager] A URI abfss:// da Landing Zone nao pode ser vazia.")

        uri_limpa = str(uri_landing_abfss).strip()
        if uri_limpa.startswith("abfss://") is False:
            raise ValueError(
                f"[DeltaCatalogManager] A URI da Landing Zone deve iniciar com 'abfss://'. Recebido: {uri_limpa}"
            )

        spark = self.runtime.obter_sessao_spark()
        print(f"--> [Bronze via Landing Zone IP Brasil] Lendo .parquet do ADLS no Spark: {uri_limpa}")
        dataframe_spark_bruto = spark.read.parquet(uri_limpa)

        timestamp_atual_utc = datetime.now(timezone.utc).isoformat()
        dataframe_com_data = dataframe_spark_bruto.withColumn(
            "_ingestion_at",
            F.lit(timestamp_atual_utc)
        )
        dataframe_spark_auditado = dataframe_com_data.withColumn(
            "_source_name",
            F.lit(str(source_name).strip())
        )

        uri_abfss = self._salvar_dataframe_spark_na_bronze(
            dataframe_spark=dataframe_spark_auditado,
            contrato_tabela=contrato_tabela,
            modo_carga=modo_carga
        )
        return uri_abfss
