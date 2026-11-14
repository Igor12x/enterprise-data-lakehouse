import base64
from datetime import datetime, timezone
from email.utils import formatdate
import hashlib
import hmac
from pathlib import Path
from urllib.parse import quote
import pandas as pd
import requests
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.config import settings
from src.catalog_dictionary import TableContract


class AdlsDatabricksConnector:
    """
    Conector responsavel por gravar dados na camada Bronze em formato Delta Lake
    no Azure Data Lake (ADLS Gen2) e consultar o catalogo SQL do Databricks.
    Possui tres metodos de entrada para a Bronze:
      1. write_delta_bronze(): Metodo tradicional via DataFrame do Pandas -> Spark/Delta.
      2. write_parquet_direto_bronze(): Metodo tradicional que lê um arquivo .parquet local direto no Spark.
      3. write_parquet_landing_brasil_bronze(): Opcao dedicada para fontes que exigem IP no Brasil
         (lê o arquivo .parquet previamente sincronizado na Landing Zone abfss:// do ADLS Gen2).
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
        Tenta definir a chave do ADLS na sessao (clusters Single User); caso o cluster esteja
        em modo Shared/Standard (onde o Spark bloqueia 'fs.azure.account.key' via codigo com
        CONFIG_NOT_AVAILABLE), utiliza a configuracao ja definida no Spark Config do Cluster.
        """
        if self._sessao_spark is None:
            sessao = SparkSession.builder.getOrCreate()

            propriedade_autenticacao = (
                f"fs.azure.account.key.{self.storage_account}.dfs.core.windows.net"
            )
            try:
                sessao.conf.set(propriedade_autenticacao, self.storage_key)
            except Exception:
                print(
                    "--> [Conector] Cluster em modo Shared/Standard detectado: utilizando "
                    "credencial ADLS configurada no Spark Config do Cluster."
                )

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

    def _assinar_put_azure_blob(
        self,
        caminho_blob: str,
        tamanho_bytes: int,
        tipo_conteudo: str,
        parametros_query_ordenados: str
    ) -> dict:
        """
        Gera os cabecalhos HTTP assinados com SharedKey (HMAC-SHA256) para operacoes PUT
        na API REST do Azure Blob/ADLS Gen2 sem exigir bibliotecas extras.
        """
        data_rfc1123 = formatdate(timeval=None, localtime=False, usegmt=True)
        versao_api_azure = "2023-11-03"

        if tamanho_bytes > 0:
            texto_tamanho = str(tamanho_bytes)
        else:
            texto_tamanho = ""

        recurso_canonico = f"/{self.storage_account}/{self.container_bronze}/{caminho_blob}"
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
            "Content-Length": str(tamanho_bytes),
            "Content-Type": tipo_conteudo,
            "Authorization": f"SharedKey {self.storage_account}:{assinatura_base64}"
        }
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
                f"[Conector] Arquivo local nao encontrado para upload na Landing Zone: {arquivo_local}"
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
                        f"[Conector] Falha no upload do bloco {indice_bloco} para o Azure ADLS: "
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
                f"[Conector] Falha ao confirmar BlockList no Azure ADLS: "
                f"Status {resposta_final.status_code} - {resposta_final.text}"
            )

        uri_landing_abfss = settings.obter_abfss_uri(
            container_name=self.container_bronze,
            caminho_relativo=caminho_blob_limpo
        )
        print(f"--> [Landing Zone ADLS] Upload concluido com sucesso em: {uri_landing_abfss}")
        return uri_landing_abfss

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
            raise ValueError("[Conector] A URI abfss:// da Landing Zone nao pode ser vazia.")

        uri_limpa = str(uri_landing_abfss).strip()
        if uri_limpa.startswith("abfss://") is False:
            raise ValueError(
                f"[Conector] A URI da Landing Zone deve iniciar com 'abfss://'. Recebido: {uri_limpa}"
            )

        spark = self.obter_sessao_spark()
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
