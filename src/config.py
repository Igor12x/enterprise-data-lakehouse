import os
from pathlib import Path

try:
    from decouple import Config, RepositoryEnv
    DECOUPLE_DISPONIVEL = True
except ImportError:
    Config = None
    RepositoryEnv = None
    DECOUPLE_DISPONIVEL = False


class Settings:
    """
    Classe de configuracao do projeto (similar a uma classe de Settings em Java/C#).
    Lê as credenciais do arquivo .env (no computador local) ou do Cofre de Segredos
    do Databricks (quando estiver rodando na nuvem Azure).
    """

    def __init__(self) -> None:
        # 1. Descobre a pasta raiz do projeto (2 niveis acima deste arquivo src/config.py)
        caminho_deste_arquivo = Path(__file__).resolve()
        self.diretorio_src = caminho_deste_arquivo.parent
        self.diretorio_raiz = self.diretorio_src.parent

        # 2. Verifica se existe o arquivo .env na raiz do projeto
        ambiente_execucao = os.environ.get("PYTHON_ENV")
        if ambiente_execucao == "local":
            nome_arquivo_env = ".env.local"
        else:
            nome_arquivo_env = ".env"

        caminho_arquivo_env = self.diretorio_raiz / nome_arquivo_env

        if DECOUPLE_DISPONIVEL is True and caminho_arquivo_env.exists() is True:
            self._leitor = Config(RepositoryEnv(str(caminho_arquivo_env)))
        elif DECOUPLE_DISPONIVEL is True:
            self._leitor = Config(os.environ)
        else:
            self._leitor = None

        # 3. Configuracoes de tempo limite, catalogos Unity Catalog (1 por camada) e Key Vault
        self.TIMEOUT_CONEXAO_SEGUNDOS = 60
        self.DATABRICKS_SECRET_SCOPE = "kv-portfolio-scope"
        self.DATABRICKS_SECRET_KEY = "adls-storage-key"
        self.DATABRICKS_CATALOG_BRONZE = self._obter_valor(
            nome_chave="DATABRICKS_CATALOG_BRONZE",
            valor_padrao="portfolio-lakehouse-bronze"
        )
        self.DATABRICKS_CATALOG_PRATA = self._obter_valor(
            nome_chave="DATABRICKS_CATALOG_PRATA",
            valor_padrao="portfolio-lakehouse-prata"
        )
        self.DATABRICKS_CATALOG_OURO = self._obter_valor(
            nome_chave="DATABRICKS_CATALOG_OURO",
            valor_padrao="portfolio-lakehouse-ouro"
        )

        # 4. Credenciais do Azure Data Lake Storage Gen2 (ADLS Gen2)
        self.AZURE_STORAGE_ACCOUNT_NAME = self._obter_valor(
            nome_chave="AZURE_STORAGE_ACCOUNT_NAME",
            valor_padrao="portfolioigordatalake"
        )
        self.AZURE_STORAGE_ACCOUNT_KEY = self._obter_chave_storage()

        # 5. Container fisico da camada Bronze (onde o Python grava o Parquet bruto da ANEEL)
        self.ADLS_CONTAINER_BRONZE = self._obter_valor(
            nome_chave="ADLS_CONTAINER_BRONZE",
            valor_padrao="portfolio-lakehouse-bronze"
        )

        # 6. Valida se as credenciais obrigatorias foram carregadas com sucesso
        self._validar_credenciais_obrigatorias()

    def esta_rodando_no_databricks(self) -> bool:
        """
        Verifica se o codigo esta sendo executado dentro de um cluster do Azure Databricks.
        """
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            return True
        else:
            return False

    def _obter_valor(self, nome_chave: str, valor_padrao: str = None) -> str:
        """
        Busca o valor de uma variavel no arquivo .env (via decouple) ou no os.environ.
        No Databricks, funciona nativamente mesmo sem a biblioteca python-decouple instalada.
        """
        try:
            if self._leitor is not None:
                if valor_padrao is not None:
                    valor_lido = self._leitor(nome_chave, default=valor_padrao)
                else:
                    valor_lido = self._leitor(nome_chave)
                return valor_lido
            else:
                if valor_padrao is not None:
                    return os.environ.get(nome_chave, valor_padrao)
                else:
                    return os.environ.get(nome_chave)
        except Exception:
            return valor_padrao

    def _obter_chave_storage(self) -> str:
        """
        Busca a senha do Data Lake primeiro no .env ou variavel de ambiente.
        Se nao encontrar (ex: rodando dentro do Databricks), busca no Secret Scope (Key Vault).
        """
        chave_ambiente = self._obter_valor(nome_chave="AZURE_STORAGE_ACCOUNT_KEY")

        if chave_ambiente is not None and str(chave_ambiente).strip() != "":
            return str(chave_ambiente).strip()

        try:
            from pyspark.sql import SparkSession
            from pyspark.dbutils import DBUtils

            sessao_spark = SparkSession.builder.getOrCreate()
            utilitario_db = DBUtils(sessao_spark)
            chave_cofre = utilitario_db.secrets.get(
                scope=self.DATABRICKS_SECRET_SCOPE,
                key=self.DATABRICKS_SECRET_KEY
            )
            return chave_cofre
        except Exception:
            return None

    def obter_abfss_uri(self, container_name: str, caminho_relativo: str = "") -> str:
        """
        Monta o endereco completo (URI abfss://) da pasta dentro do Azure Data Lake Gen2.
        """
        caminho_limpo = str(caminho_relativo).strip("/")
        uri_base = (
            f"abfss://{container_name}@"
            f"{self.AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
        )

        if caminho_limpo != "":
            uri_completa = f"{uri_base}/{caminho_limpo}"
            return uri_completa
        else:
            return uri_base

    def _validar_credenciais_obrigatorias(self) -> None:
        """
        Bloqueia a execucao imediatamente caso falte o nome ou a chave do Storage Account.
        """
        variaveis_obrigatorias = [
            ("AZURE_STORAGE_ACCOUNT_NAME", self.AZURE_STORAGE_ACCOUNT_NAME),
            ("AZURE_STORAGE_ACCOUNT_KEY", self.AZURE_STORAGE_ACCOUNT_KEY)
        ]

        for item in variaveis_obrigatorias:
            nome_campo = item[0]
            valor_campo = item[1]

            if valor_campo is None or str(valor_campo).strip() == "":
                raise ValueError(
                    f"[ERRO AMBIENTAL] A variavel obrigatoria '{nome_campo}' "
                    f"nao foi encontrada no .env nem no Secret Scope '{self.DATABRICKS_SECRET_SCOPE}'."
                )


# Objeto unico de configuracao compartilhado por todo o projeto
settings = Settings()
