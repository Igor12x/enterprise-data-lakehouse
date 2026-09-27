import os
from io import BytesIO
import boto3
import pandas as pd
from botocore.exceptions import ClientError, BotoCoreError

# Consumo centralizado e validado das configuracoes da plataforma
from src.config import settings


class R2Connector:
    """
    Classe de infraestrutura responsavel pela integracao com o Cloudflare R2
    utilizando o protocolo padronizado da API S3 (boto3).
    Centraliza operacoes de escrita, leitura e remocao de arquivos Parquet.
    """

    def __init__(self):
        # 1. Carregamento dos parametros de conexao validados no config.py
        self.endpoint_url = settings.R2_ENDPOINT_URL
        self.access_key_id = settings.R2_ACCESS_KEY_ID
        self.secret_access_key = settings.R2_SECRET_ACCESS_KEY
        self.bucket_padrao_bronze = settings.R2_BUCKET_BRONZE

        # 2. Inicializacao do cliente S3 com tratamento explicito de falhas
        try:
            self.s3_client = boto3.client(
                service_name="s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                region_name="auto"
            )
        except Exception as erro_inicializacao:
            mensagem_falha = f"[R2Connector] Erro ao instanciar cliente S3: {str(erro_inicializacao)}"
            raise ConnectionError(mensagem_falha)

    def upload_parquet(self, df: pd.DataFrame, target_key: str, bucket_name: str = None) -> str:
        """
        Recebe um DataFrame tabular, serializa para Parquet com compressao Snappy
        em memoria RAM e executa o envio direto para o bucket do Cloudflare R2.

        Parametros:
            df (pd.DataFrame): Dados estruturados a serem persistidos.
            target_key (str): Chave ou caminho virtual do objeto no bucket.
            bucket_name (str, opcional): Bucket de destino. Se None, utiliza o Bronze padrao.

        Retorno:
            str: URI padronizada no formato s3://bucket/chave.
        """
        # 3. Definicao tradicional do bucket de destino via if/else
        if bucket_name is not None:
            bucket_alvo = bucket_name
        else:
            bucket_alvo = self.bucket_padrao_bronze

        # 4. Validacoes defensivas de integridade do DataFrame
        if df is None:
            raise ValueError("[R2Connector] O DataFrame fornecido para gravacao nao pode ser None.")

        total_linhas = len(df)
        if total_linhas == 0:
            print(f"[R2Connector - AVISO] O DataFrame para '{target_key}' nao possui linhas de dados.")

        # 5. Serializacao colunar em memoria RAM utilizando BytesIO
        try:
            buffer_parquet = BytesIO()
            df.to_parquet(
                buffer_parquet,
                index=False,
                engine="pyarrow",
                compression="snappy"
            )
            # Retorna o ponteiro para o byte inicial do buffer
            buffer_parquet.seek(0)
        except Exception as erro_serializacao:
            mensagem_serializacao = f"[R2Connector] Falha na compressao Parquet: {str(erro_serializacao)}"
            raise RuntimeError(mensagem_serializacao)

        # 6. Realizacao do upload com captura de excecoes de rede e permissao
        uri_destino = f"s3://{bucket_alvo}/{target_key}"
        print(f"--> [R2Connector] Gravando {total_linhas} linhas em: {uri_destino}")

        try:
            self.s3_client.put_object(
                Bucket=bucket_alvo,
                Key=target_key,
                Body=buffer_parquet.getvalue()
            )
            print("--> [R2Connector] Upload concluido com sucesso!")
        except ClientError as erro_permissao:
            mensagem_permissao = f"[R2Connector] Falha de autenticacao/autorizacao no R2: {str(erro_permissao)}"
            raise RuntimeError(mensagem_permissao)
        except BotoCoreError as erro_rede:
            mensagem_rede = f"[R2Connector] Falha de conexao de rede com o Cloudflare R2: {str(erro_rede)}"
            raise RuntimeError(mensagem_rede)

        return uri_destino

    def read_parquet(self, target_key: str, bucket_name: str = None) -> pd.DataFrame:
        """
        Le um arquivo Parquet diretamente do bucket e retorna um DataFrame do Pandas.
        Utilizado para validacoes de integridade e auditoria de dados gravados.

        Parametros:
            target_key (str): Chave do objeto no bucket.
            bucket_name (str, opcional): Bucket de origem. Se None, utiliza o Bronze padrao.

        Retorno:
            pd.DataFrame: Dados recuperados e decodificados em memoria.
        """
        if bucket_name is not None:
            bucket_alvo = bucket_name
        else:
            bucket_alvo = self.bucket_padrao_bronze

        print(f"--> [R2Connector] Lendo arquivo: s3://{bucket_alvo}/{target_key}")

        try:
            resposta_s3 = self.s3_client.get_object(
                Bucket=bucket_alvo,
                Key=target_key
            )
            conteudo_bytes = resposta_s3["Body"].read()
            df_recuperado = pd.read_parquet(BytesIO(conteudo_bytes))
            print(f"--> [R2Connector] Leitura concluida: {len(df_recuperado)} linhas carregadas.")
            return df_recuperado
        except ClientError as erro_cliente:
            mensagem_leitura = f"[R2Connector] Erro ao recuperar objeto '{target_key}': {str(erro_cliente)}"
            raise RuntimeError(mensagem_leitura)
        except Exception as erro_generico:
            mensagem_geral = f"[R2Connector] Falha inesperada na leitura do arquivo: {str(erro_generico)}"
            raise RuntimeError(mensagem_geral)

    def delete_file(self, target_key: str, bucket_name: str = None) -> bool:
        """
        Remove um arquivo do bucket do Cloudflare R2.
        Utilizado para limpeza de testes, expurgo de dados e reprocessamentos controlados.

        Parametros:
            target_key (str): Caminho/Chave do arquivo a ser excluido.
            bucket_name (str, opcional): Bucket onde o arquivo reside. Se None, usa o Bronze.

        Retorno:
            bool: True se o comando de delecao foi aceito com sucesso pela nuvem.
        """
        if bucket_name is not None:
            bucket_alvo = bucket_name
        else:
            bucket_alvo = self.bucket_padrao_bronze

        print(f"--> [R2Connector] Removendo arquivo: s3://{bucket_alvo}/{target_key}")

        try:
            self.s3_client.delete_object(
                Bucket=bucket_alvo,
                Key=target_key
            )
            print("--> [R2Connector] Arquivo excluido com sucesso do Cloudflare R2!")
            return True
        except ClientError as erro_delecao:
            mensagem_delecao = f"[R2Connector] Falha ao deletar '{target_key}': {str(erro_delecao)}"
            raise RuntimeError(mensagem_delecao)