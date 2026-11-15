import os
import subprocess
import sys
from typing import List
from src.config import settings
from src.infrastructure.databricks_runtime import DatabricksRuntime


class DbtRunner:
    """
    Responsabilidade Unica (Executor de Transformacoes SQL via dbt):
    Monta e executa os comandos do dbt CLI para os modelos das camadas Prata e Ouro,
    injetando automaticamente as credenciais do runtime ativo no Databricks.
    """

    def __init__(self, runtime: DatabricksRuntime = None) -> None:
        if runtime is not None:
            self.runtime = runtime
        else:
            self.runtime = DatabricksRuntime()

        self.diretorio_dbt = settings.diretorio_raiz / "dbt_project"

    def montar_comando_dbt(self, seletor_modelo: str, modo_carga: str = "full") -> List[str]:
        """
        Monta a lista de argumentos do dbt run invocando o modulo Python 'dbt.cli.main'
        diretamente pelo interpretador atual (sys.executable).
        """
        if seletor_modelo is None or str(seletor_modelo).strip() == "":
            raise ValueError("[DbtRunner] O seletor de modelo do dbt nao pode ser vazio.")

        modo_limpo = str(modo_carga).strip().lower()
        caminho_projeto = str(self.diretorio_dbt)

        comando = [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "run",
            "--project-dir",
            caminho_projeto,
            "--profiles-dir",
            caminho_projeto,
            "--select",
            str(seletor_modelo).strip()
        ]

        if modo_limpo == "full":
            comando.append("--full-refresh")

        return comando

    def executar_modelo_dbt(self, seletor_modelo: str, modo_carga: str = "full") -> None:
        """
        Executa um modelo SQL especifico no dbt respeitando a ordem de dependencia.
        """
        comando = self.montar_comando_dbt(
            seletor_modelo=seletor_modelo,
            modo_carga=modo_carga
        )
        variaveis_ambiente = os.environ.copy()

        precisa_descobrir_host = (
            "DATABRICKS_HOST" not in variaveis_ambiente
            or str(variaveis_ambiente.get("DATABRICKS_HOST", "")).strip() == ""
        )
        precisa_descobrir_path = (
            "DATABRICKS_HTTP_PATH" not in variaveis_ambiente
            or str(variaveis_ambiente.get("DATABRICKS_HTTP_PATH", "")).strip() == ""
        )
        precisa_descobrir_token = (
            "DATABRICKS_TOKEN" not in variaveis_ambiente
            or str(variaveis_ambiente.get("DATABRICKS_TOKEN", "")).strip() == ""
        )

        if precisa_descobrir_host or precisa_descobrir_path or precisa_descobrir_token:
            credenciais_runtime = self.runtime.obter_credenciais_runtime_dbt()
            for chave_env, valor_env in credenciais_runtime.items():
                if valor_env != "":
                    variaveis_ambiente[chave_env] = valor_env

        print(f"--> [DbtRunner] Gerando tabela '{seletor_modelo}': {' '.join(comando)}")

        resultado = subprocess.run(
            comando,
            cwd=str(self.diretorio_dbt),
            env=variaveis_ambiente,
            capture_output=True,
            text=True,
            check=False
        )

        if resultado.stdout:
            print(resultado.stdout)

        if resultado.returncode != 0:
            if resultado.stderr:
                print(resultado.stderr)
            raise RuntimeError(
                f"[DbtRunner] Falha ao gerar a tabela '{seletor_modelo}' no dbt run."
            )
