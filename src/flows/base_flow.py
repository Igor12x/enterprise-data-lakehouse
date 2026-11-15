import abc
import importlib.metadata
import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List
from src.connectors.adls_databricks_connector import AdlsDatabricksConnector


class BaseFlow(abc.ABC):
    """
    Classe base abstrata para todos os orquestradores (Flows) do Lakehouse.
    Obriga a validacao e instalacao automatica das dependencias do arquivo requirements.txt
    na inicializacao de qualquer Flow e centraliza a execucao do dbt (Prata e Ouro).
    """

    def __init__(self, flow_name: str) -> None:
        if flow_name is None or str(flow_name).strip() == "":
            raise ValueError("[BaseFlow] O parametro 'flow_name' nao pode ser nulo ou vazio.")

        self.flow_name: str = str(flow_name).strip()
        self.diretorio_raiz: Path = Path(__file__).resolve().parent.parent.parent
        self.diretorio_dbt: Path = self.diretorio_raiz / "dbt_project"
        self.arquivo_requirements: Path = self.diretorio_raiz / "requirements.txt"

        # Garante obrigatoriamente todas as dependencias do requirements.txt ao instanciar qualquer Flow
        self.garantir_dependencias_requirements()

        self.conector_lakehouse = AdlsDatabricksConnector()
        self.tabelas_concluidas: List[str] = []

    def _extrair_nome_pacote(self, linha_requirement: str) -> str:
        """
        Extrai apenas o nome do pacote a partir de uma linha do requirements.txt
        (ex: 'dbt-databricks>=1.8.0' -> 'dbt-databricks').
        """
        separadores_versao = [">=", "<=", "==", "~=", "!=", ">", "<", "["]
        texto_trabalho = str(linha_requirement).strip()

        for separador in separadores_versao:
            if separador in texto_trabalho:
                partes = texto_trabalho.split(separador, 1)
                texto_trabalho = partes[0].strip()

        return texto_trabalho

    def _verificar_pacote_instalado(self, nome_pacote: str) -> bool:
        """
        Verifica se um pacote do requirements.txt ja esta instalado no ambiente Python atual.
        No Databricks Serverless, o motor Spark ja vem embutido via databricks-connect (pyspark).
        """
        nome_limpo = str(nome_pacote).strip().lower()

        if nome_limpo == "pyspark":
            especificacao_spark = importlib.util.find_spec("pyspark")
            if especificacao_spark is not None:
                return True

        try:
            importlib.metadata.version(nome_limpo)
            return True
        except importlib.metadata.PackageNotFoundError:
            return False

    def ler_dependencias_requirements(self) -> List[str]:
        """
        Lê o arquivo requirements.txt da raiz do projeto e retorna a lista de especificacoes
        validas (ignorando comentarios e linhas em branco).
        """
        if self.arquivo_requirements.exists() is False:
            raise FileNotFoundError(
                f"[BaseFlow] Arquivo requirements.txt nao encontrado em: {self.arquivo_requirements}"
            )

        linhas_brutas = self.arquivo_requirements.read_text(encoding="utf-8").splitlines()
        dependencias_validas: List[str] = []

        for linha in linhas_brutas:
            linha_limpa = str(linha).strip()
            if linha_limpa != "" and linha_limpa.startswith("#") is False:
                dependencias_validas.append(linha_limpa)

        return dependencias_validas

    def garantir_dependencias_requirements(self) -> List[str]:
        """
        Metodo obrigatorio executado por todos os Flows:
        Inspeciona o arquivo requirements.txt e instala automaticamente no Python ativo
        qualquer pacote que esteja faltando (ex: em ambientes Serverless recem-iniciados).
        """
        todas_dependencias = self.ler_dependencias_requirements()
        dependencias_faltantes: List[str] = []

        for especificacao in todas_dependencias:
            nome_pacote = self._extrair_nome_pacote(linha_requirement=especificacao)
            esta_instalado = self._verificar_pacote_instalado(nome_pacote=nome_pacote)

            if esta_instalado is False:
                dependencias_faltantes.append(especificacao)

        if len(dependencias_faltantes) > 0:
            lista_formatada = ", ".join(dependencias_faltantes)
            print(
                f"--> [BaseFlow:{self.flow_name}] Instalando dependencias faltantes do "
                f"requirements.txt: {lista_formatada}"
            )

            comando_pip: List[str] = [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--quiet"
            ]
            for pacote_faltante in dependencias_faltantes:
                comando_pip.append(pacote_faltante)

            resultado_pip = subprocess.run(comando_pip, check=False)
            if resultado_pip.returncode != 0:
                raise RuntimeError(
                    f"[BaseFlow:{self.flow_name}] Falha ao instalar dependencias do "
                    f"requirements.txt: {lista_formatada}"
                )

            importlib.invalidate_caches()
            print(f"--> [BaseFlow:{self.flow_name}] Dependencias instaladas com sucesso!")

        return dependencias_faltantes

    def montar_comando_dbt(self, seletor_modelo: str, modo_carga: str) -> List[str]:
        """
        Constroi a lista de argumentos para executar o dbt run usando o proprio
        interpretador Python ativo (sys.executable -m dbt.cli.main), evitando erro de PATH.
        """
        modo_validado = self.conector_lakehouse.validar_modo_carga(modo_carga=modo_carga)

        comando: List[str] = [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "run",
            "--project-dir",
            str(self.diretorio_dbt),
            "--profiles-dir",
            str(self.diretorio_dbt),
            "--select",
            seletor_modelo
        ]

        if modo_validado == "full":
            comando.append("--full-refresh")

        return comando

    def _obter_ambiente_execucao_dbt(self) -> Dict[str, str]:
        """
        Injeta automaticamente no subprocesso do dbt as credenciais de conexao
        descobertas a partir do compute ativo (Serverless ou Cluster tradicional),
        sem exigir configuracao manual por cluster.
        """
        ambiente_dbt = os.environ.copy()
        credenciais_runtime = self.conector_lakehouse.obter_credenciais_runtime_dbt()

        for chave_env, valor_descoberto in credenciais_runtime.items():
            valor_atual = ambiente_dbt.get(chave_env, "").strip()
            if valor_atual == "" and valor_descoberto != "":
                ambiente_dbt[chave_env] = valor_descoberto

        return ambiente_dbt

    @abc.abstractmethod
    def run_bronze(self, modo_carga: str = "full") -> Dict[str, str]:
        """Executa a ingestao da camada Bronze do fluxo."""
        pass

    @abc.abstractmethod
    def run_prata(self, modo_carga: str = "full") -> Dict[str, str]:
        """Executa a transformacao da camada Prata do fluxo."""
        pass

    @abc.abstractmethod
    def run_ouro(self, modo_carga: str = "full") -> Dict[str, str]:
        """Executa a transformacao da camada Ouro do fluxo."""
        pass

    @abc.abstractmethod
    def run(self, modo_carga: str = "full") -> Dict[str, str]:
        """Executa a pipeline completa (Bronze -> Prata -> Ouro) do fluxo."""
        pass
