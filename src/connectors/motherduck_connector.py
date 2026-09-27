import duckdb
from src.config import settings


class MotherDuckConnector:
    """
    Gerenciador de conexao e catalogacao no MotherDuck (DuckDB Cloud).
    Configura credenciais do Cloudflare R2 (via DuckDB Secret Manager)
    e executa DDLs para criacao de tabelas e views na camada analitica.
    """

    def __init__(self):
        # 1. Validacao defensiva do token
        if not settings.MOTHERDUCK_TOKEN:
            raise ValueError("[MotherDuckConnector] MOTHERDUCK_TOKEN nao fornecido no .env.")

        self.db_bronze = settings.MOTHERDUCK_DB_BRONZE
        self.conexao = None

    def _obter_conexao(self) -> duckdb.DuckDBPyConnection:
        """
        Conecta diretamente no MotherDuck conectando ao banco Bronze padrao.
        """
        if self.conexao is None:
            # String de conexao oficial MotherDuck com database inicial
            string_conexao = f"md:{self.db_bronze}?motherduck_token={settings.MOTHERDUCK_TOKEN}"
            self.conexao = duckdb.connect(string_conexao)
            self._configurar_secret_r2()
        return self.conexao

    def _configurar_secret_r2(self) -> None:
        """
        Cria o Secret seguro no DuckDB para autenticar no Cloudflare R2.
        """
        endpoint_limpo = (
            settings.R2_ENDPOINT_URL
            .replace("https://", "")
            .replace("http://", "")
            .strip("/")
        )

        sql_secret = f"""
        CREATE OR REPLACE SECRET r2_lakehouse_secret IN MOTHERDUCK (
            TYPE S3,
            KEY_ID '{settings.R2_ACCESS_KEY_ID}',
            SECRET '{settings.R2_SECRET_ACCESS_KEY}',
            ENDPOINT '{endpoint_limpo}',
            URL_STYLE 'path',
            REGION 'auto'
        );
        """
        self.conexao.execute(sql_secret)

    def registrar_tabela_bronze(
        self,
        schema_name: str,
        nome_tabela: str,
        origem_s3_padrao: str,
        criar_como_tabela_fisica: bool = False
    ) -> None:
        """
        Registra a tabela ou view dentro do database bronze e schema especificado.
        Exemplo resultante: bronze.gd.aneel_gd

        Parametros:
            schema_name (str): Nome do schema/dominio (ex: 'gd').
            nome_tabela (str): Nome da tabela/view (ex: 'aneel_gd').
            origem_s3_padrao (str): Caminho S3 com glob (ex: 's3://lakehouse-bronze/aneel/gd/**/*.parquet').
            criar_como_tabela_fisica (bool): False cria VIEW (Zero-Copy), True cria TABLE.
        """
        con = self._obter_conexao()
        tipo_objeto = "TABLE" if criar_como_tabela_fisica else "VIEW"
        
        # Garante que o schema dentro do banco bronze exista (ex: CREATE SCHEMA IF NOT EXISTS bronze.gd)
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {self.db_bronze}.{schema_name};")

        # Caminho totalmente qualificado: bronze.gd.aneel_gd
        identificador_completo = f"{self.db_bronze}.{schema_name}.{nome_tabela}"

        sql_ddl = f"""
        CREATE OR REPLACE {tipo_objeto} {identificador_completo} AS 
        SELECT * 
        FROM read_parquet('{origem_s3_padrao}', hive_partitioning = true);
        """

        print(f"--> [MotherDuck] Executando DDL: {tipo_objeto} '{identificador_completo}'...")
        con.execute(sql_ddl)
        print(f"--> [MotherDuck] Sucesso: '{identificador_completo}' disponivel para consulta!")

    def fechar_conexao(self) -> None:
        """Fecha a conexao com o MotherDuck."""
        if self.conexao is not None:
            self.conexao.close()
            self.conexao = None