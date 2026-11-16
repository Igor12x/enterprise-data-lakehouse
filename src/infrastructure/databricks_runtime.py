import importlib.metadata
import re
import subprocess
import sys
from typing import Dict, List
from pyspark.sql import SparkSession
from src.config import settings


class DatabricksRuntime:
    """
    Responsabilidade Unica (Infraestrutura de Runtime):
    Gerencia a sessao ativa do Spark no Databricks, descobre automaticamente as
    credenciais de execucao do cluster/warehouse (Serverless ou Classico) e garante
    que todas as dependencias do requirements.txt estejam instaladas no ambiente.
    """

    def __init__(self) -> None:
        self._sessao_spark: SparkSession = None

    def obter_sessao_spark(self) -> SparkSession:
        """
        Retorna a sessao ativa do Spark no Databricks.
        A autenticacao com os containers do Azure ADLS Gen2 (abfss://) e gerenciada
        nativamente pelas External Locations do Unity Catalog.
        """
        if self._sessao_spark is None:
            self._sessao_spark = SparkSession.builder.getOrCreate()

        return self._sessao_spark

    def ler_dependencias_requirements(self) -> List[str]:
        """
        Le o arquivo requirements.txt na raiz do projeto e retorna a lista de especificacoes
        de pacotes validas (ignorando linhas vazias e comentarios).
        """
        caminho_requirements = settings.diretorio_raiz / "requirements.txt"
        if caminho_requirements.exists() is False:
            raise FileNotFoundError(
                f"[DatabricksRuntime] Arquivo requirements.txt nao encontrado em: {caminho_requirements}"
            )

        conteudo = caminho_requirements.read_text(encoding="utf-8")
        linhas = conteudo.splitlines()
        dependencias: List[str] = []

        for linha in linhas:
            linha_limpa = linha.strip()
            if linha_limpa != "" and linha_limpa.startswith("#") is False:
                dependencias.append(linha_limpa)

        return dependencias

    def _extrair_nome_pacote(self, especificacao_pacote: str) -> str:
        """
        Extrai apenas o nome base do pacote (ex: 'dbt-databricks>=1.8.0' -> 'dbt-databricks').
        """
        partes = re.split(r"[><=!~\[\s]", especificacao_pacote, maxsplit=1)
        nome_pacote = partes[0].strip()
        return nome_pacote

    def _verificar_pacote_instalado(self, nome_pacote: str) -> bool:
        """
        Verifica se um pacote do requirements.txt ja esta instalado no ambiente Python atual.
        """
        try:
            importlib.metadata.version(nome_pacote)
            return True
        except importlib.metadata.PackageNotFoundError:
            nome_normalizado = nome_pacote.replace("-", "_")
            try:
                importlib.metadata.version(nome_normalizado)
                return True
            except importlib.metadata.PackageNotFoundError:
                return False

    def garantir_dependencias_requirements(self) -> List[str]:
        """
        Verifica todas as bibliotecas listadas no arquivo requirements.txt e instala
        automaticamente no Python atual qualquer pacote que estiver faltando no cluster.
        Retorna a lista de pacotes que precisaram ser instalados.
        """
        caminho_requirements = settings.diretorio_raiz / "requirements.txt"
        dependencias = self.ler_dependencias_requirements()
        pacotes_faltantes: List[str] = []

        for especificacao in dependencias:
            nome_pacote = self._extrair_nome_pacote(especificacao)
            esta_instalado = self._verificar_pacote_instalado(nome_pacote)
            if esta_instalado is False:
                pacotes_faltantes.append(especificacao)

        if len(pacotes_faltantes) > 0:
            print(
                f"--> [DatabricksRuntime] Instalando dependencias ausentes do requirements.txt: "
                f"{pacotes_faltantes}"
            )
            comando_pip = [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--quiet",
                "--disable-pip-version-check",
                "-r",
                str(caminho_requirements)
            ]
            resultado_pip = subprocess.run(
                comando_pip,
                capture_output=True,
                text=True,
                check=False
            )
            if resultado_pip.returncode != 0:
                if resultado_pip.stderr:
                    print(resultado_pip.stderr)
                raise RuntimeError(
                    f"[DatabricksRuntime] Falha ao instalar pacotes do requirements.txt "
                    f"(codigo {resultado_pip.returncode})."
                )
            print("--> [DatabricksRuntime] Todas as dependencias do requirements.txt foram instaladas!")

        return pacotes_faltantes

    def obter_credenciais_runtime_dbt(self) -> Dict[str, str]:
        """
        Descobre automaticamente em tempo de execucao o host, o http_path e o token
        do compute ativo no Databricks (seja Serverless ou um Cluster tradicional).
        """
        host_descoberto = ""
        http_path_descoberto = ""
        token_descoberto = ""

        # 1. Tenta descobrir Host e Token via Databricks SDK nativo do ambiente
        try:
            from databricks.sdk import WorkspaceClient

            cliente_ws = WorkspaceClient()

            if cliente_ws.config.host is not None:
                host_limpo = str(cliente_ws.config.host).replace("https://", "").replace("http://", "").strip("/")
                host_descoberto = host_limpo

            dados_autenticacao = cliente_ws.config.authenticate()
            if callable(dados_autenticacao) is True:
                cabecalhos_auth = dados_autenticacao()
            else:
                cabecalhos_auth = dados_autenticacao

            if isinstance(cabecalhos_auth, dict) is True and "Authorization" in cabecalhos_auth:
                valor_auth = str(cabecalhos_auth["Authorization"]).strip()
                if valor_auth.startswith("Bearer ") is True:
                    token_descoberto = valor_auth.replace("Bearer ", "", 1).strip()
                else:
                    token_descoberto = valor_auth
            elif cliente_ws.config.token is not None:
                token_descoberto = str(cliente_ws.config.token).strip()
        except Exception:
            pass

        spark = self.obter_sessao_spark()

        # 2. Fallback de Host via configuracao da sessao Spark
        if host_descoberto == "":
            try:
                url_workspace = spark.conf.get("spark.databricks.workspaceUrl")
                if url_workspace is not None:
                    host_descoberto = str(url_workspace).replace("https://", "").replace("http://", "").strip("/")
            except Exception:
                pass

        # 3. Fallback de Token via contexto do Notebook (para clusters classicos)
        if token_descoberto == "":
            try:
                from pyspark.dbutils import DBUtils

                utilitario_db = DBUtils(spark)
                contexto_nb = utilitario_db.notebook.entry_point.getDbutils().notebook().getContext()
                token_contexto = contexto_nb.apiToken().get()
                if token_contexto is not None:
                    token_descoberto = str(token_contexto).strip()
            except Exception:
                pass

        # 4. Descoberta automatica do HTTP_PATH (Cluster tradicional ou Serverless SQL Warehouse)
        try:
            cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
            try:
                org_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
            except Exception:
                org_id = spark.conf.get("spark.databricks.clusterUsageTags.orgId")

            if cluster_id is not None and org_id is not None:
                cluster_limpo = str(cluster_id).strip()
                org_limpo = str(org_id).strip()
                if cluster_limpo != "" and org_limpo != "":
                    http_path_descoberto = f"/sql/protocolv1/o/{org_limpo}/{cluster_limpo}"
        except Exception:
            pass

        if http_path_descoberto == "":
            try:
                from databricks.sdk import WorkspaceClient

                cliente_ws = WorkspaceClient()
                lista_warehouses = list(cliente_ws.warehouses.list())
                if len(lista_warehouses) > 0:
                    warehouse_ativo = lista_warehouses[0]
                    http_path_descoberto = f"/sql/1.0/warehouses/{warehouse_ativo.id}"
            except Exception:
                pass

        print(
            f"--> [DatabricksRuntime] Compute auto-descoberto para o dbt | "
            f"Host: {host_descoberto} | HTTP Path: {http_path_descoberto}"
        )

        credenciais = {
            "DATABRICKS_HOST": host_descoberto,
            "DATABRICKS_HTTP_PATH": http_path_descoberto,
            "DATABRICKS_TOKEN": token_descoberto
        }
        return credenciais
