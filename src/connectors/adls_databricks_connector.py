from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.config import settings
from src.catalog_dictionary import TableContract


class AdlsDatabricksConnector:
    """
    Conector responsavel por gravar dados na camada Bronze em formato Delta Lake
    no Azure Data Lake (ADLS Gen2) e consultar o catalogo SQL do Databricks.
    Possui dois metodos de entrada para a Bronze:
      1. write_delta_bronze(): Recebe um DataFrame do Pandas e converte para Spark/Delta.
      2. write_parquet_direto_bronze(): Lê um arquivo .parquet diretamente no Spark sem passar pelo Pandas.
    """

    def __init__(self) -> None:
        self.storage_account = settings.AZURE_STORAGE_ACCOUNT_NAME
        self.storage_key = settings.AZURE_STORAGE_ACCOUNT_KEY
        self.container_bronze = settings.ADLS_CONTAINER_BRONZE

        # Inicia como None para permitir rodar testes locais no Windows sem precisar de Java/Spark
        self._sessao_spark: SparkSession = None

    def obter_sessao_spark(self) -> SparkSession:
        """
        Metodo Getter (estilo Java/C#): Retorna a sessao ativa do Spark no Databricks.
        Na primeira chamada, configura a chave de acesso ao Azure Data Lake.
        """
        if self._sessao_spark is None:
            sessao = SparkSession.builder.getOrCreate()

            propriedade_autenticacao = (
                f"fs.azure.account.key.{self.storage_account}.dfs.core.windows.net"
            )
            sessao.conf.set(propriedade_autenticacao, self.storage_key)

            self._sessao_spark = sessao

        return self._sessao_spark

    def validar_modo_carga(self, modo_carga: str) -> str:
        """
        Valida se o modo de atualizacao escolhido e 'full' (substituicao total)
        ou 'incremental' (atualizacao/insercao via MERGE).
        """
        if modo_carga is None:
            raise ValueError("[Conector] O parametro 'modo_carga' nao pode ser nulo.")

        modo_limpo = str(modo_carga).strip().lower()

        if modo_limpo == "full":
            return "full"
        elif modo_limpo == "incremental":
            return "incremental"
        else:
            raise ValueError(
                f"[Conector] Modo de carga '{modo_carga}' invalido. "
                f"Escolha 'full' ou 'incremental'."
            )

    def verificar_se_tabela_existe(self, identificador_completo_sql: str) -> bool:
        """
        Consulta o catalogo do Databricks (hive_metastore) para saber se uma tabela ja foi criada.
        """
        spark = self.obter_sessao_spark()
        tabela_existe = spark.catalog.tableExists(identificador_completo_sql)
        return tabela_existe

    def garantir_schema_catalogo(self, schema_name: str) -> None:
        """
        Cria o banco/schema (ex: gd, localidade, tempo) no Databricks caso ainda nao exista.
        """
        spark = self.obter_sessao_spark()
        comando_sql = f"CREATE SCHEMA IF NOT EXISTS hive_metastore.{schema_name};"
        spark.sql(comando_sql)

    def registrar_tabela_catalogo(
        self,
        schema_name: str,
        table_name: str,
        uri_abfss: str
    ) -> None:
        """
        Vincula a pasta fisica do Azure Data Lake a uma tabela SQL visivel no Databricks e Power BI.
        """
        spark = self.obter_sessao_spark()
        identificador_tabela = f"hive_metastore.{schema_name}.{table_name}"

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
        Metodo interno compartilhado: recebe um Spark DataFrame (vindo do Pandas ou de um .parquet direto)
        e executa a gravacao na camada Bronze em modo 'full' (Overwrite) ou 'incremental' (MERGE INTO).
        """
        modo_validado = self.validar_modo_carga(modo_carga=modo_carga)
        caminho_limpo = str(contrato_tabela.adls_relative_path).strip("/")

        uri_abfss = settings.obter_abfss_uri(
            container_name=self.container_bronze,
            caminho_relativo=caminho_limpo
        )

        spark = self.obter_sessao_spark()
        self.garantir_schema_catalogo(schema_name=contrato_tabela.schema_name)
        tabela_ja_existe = self.verificar_se_tabela_existe(
            identificador_completo_sql=contrato_tabela.full_table_identifier
        )

        if modo_validado == "full" or tabela_ja_existe is False:
            print(f"--> [Bronze] Modo FULL: Gravando tabela Delta completa em {uri_abfss}")
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
                uri_abfss=uri_abfss
            )
        else:
            print(f"--> [Bronze] Modo INCREMENTAL: Executando MERGE em {contrato_tabela.full_table_identifier}")
            nome_view_temp = f"view_temp_bronze_{contrato_tabela.table_name}"
            coluna_pk = contrato_tabela.primary_key

            dataframe_sem_duplicatas = dataframe_spark.dropDuplicates([coluna_pk])
            dataframe_sem_duplicatas.createOrReplaceTempView(nome_view_temp)

            sql_merge = f"""
            MERGE INTO {contrato_tabela.full_table_identifier} AS destino
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
        Ideal para APIs JSON, CSVs ou listas extraidas via Python.
        """
        if df_pandas is None:
            raise ValueError("[Conector] O DataFrame Pandas enviado para a Bronze e nulo.")

        if df_pandas.empty is True:
            raise ValueError("[Conector] O DataFrame Pandas enviado para a Bronze esta vazio.")

        spark = self.obter_sessao_spark()
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
        OPCAO 2 (Leitura direta de .parquet sem Pandas):
        Lê um arquivo .parquet diretamente pelo motor do Spark, adiciona as colunas de
        auditoria (_ingestion_at e _source_name) e grava na Bronze sem converter para Pandas.
        """
        if caminho_arquivo_parquet is None or str(caminho_arquivo_parquet).strip() == "":
            raise ValueError("[Conector] O caminho do arquivo .parquet nao pode ser vazio.")

        arquivo_fisico = Path(caminho_arquivo_parquet).resolve()
        if arquivo_fisico.exists() is False:
            raise FileNotFoundError(
                f"[Conector] O arquivo .parquet nao foi encontrado em: {arquivo_fisico}"
            )

        spark = self.obter_sessao_spark()
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
