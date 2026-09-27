import os
from pathlib import Path
from decouple import Config, RepositoryEnv


class Settings:
    """
    Classe centralizadora de configuracoes corporativas.
    Implementa leitura hibrida: funciona com arquivos fisicos (.env / .env.local)
    em ambiente de desenvolvimento e diretamente com variaveis de ambiente
    do Sistema Operacional (os.environ) em VMs, Databricks e CI/CD.
    """

    def __init__(self):
        # 1. Resolucao do caminho raiz absoluto do projeto
        self.diretorio_atual = Path(__file__).resolve()
        self.diretorio_src = self.diretorio_atual.parent
        self.diretorio_raiz = self.diretorio_src.parent

        # 2. Identificacao do ambiente pelo Sistema Operacional
        self.ambiente_execucao = os.environ.get("PYTHON_ENV")

        if self.ambiente_execucao == "local":
            self.nome_arquivo_env = ".env.local"
        else:
            self.nome_arquivo_env = ".env"

        self.caminho_arquivo_env = self.diretorio_raiz / self.nome_arquivo_env

        # 3. Decisao de carregamento (Arquivo Fisico vs Variaveis de Sistema)
        self.arquivo_existe = self.caminho_arquivo_env.exists()

        if self.arquivo_existe:
            # Caso 1: Maquina Local ou VM com arquivo .env presente
            self._leitor = Config(RepositoryEnv(str(self.caminho_arquivo_env)))
            self.origem_dados = f"Arquivo fisico ({self.nome_arquivo_env})"
        else:
            # Caso 2: Databricks, Docker, Kubernetes ou CI/CD (variaveis direto no SO)
            # A classe Config sem RepositoryEnv le nativamente do os.environ
            self._leitor = Config(os.environ)
            self.origem_dados = "Sistema Operacional / Cloud Runtime (os.environ)"

        # -------------------------------------------------------------
        # METADADOS DE SISTEMA
        # -------------------------------------------------------------
        self.TIMEZONE_LOCAL = "America/Sao_Paulo"
        self.TIMEOUT_CONEXAO_SEGUNDOS = 30

        # -------------------------------------------------------------
        # CLOUDFLARE R2 (DATA LAKE - STORAGE OBJECTS)
        # -------------------------------------------------------------
        self.R2_ENDPOINT_URL = self._obter_valor("R2_ENDPOINT_URL")
        self.R2_ACCESS_KEY_ID = self._obter_valor("R2_ACCESS_KEY_ID")
        self.R2_SECRET_ACCESS_KEY = self._obter_valor("R2_SECRET_ACCESS_KEY")

        self.R2_BUCKET_BRONZE = self._obter_valor("R2_BUCKET_BRONZE", valor_padrao="lakehouse-bronze")
        self.R2_BUCKET_SILVER = self._obter_valor("R2_BUCKET_SILVER", valor_padrao="lakehouse-prata")
        self.R2_BUCKET_GOLD = self._obter_valor("R2_BUCKET_GOLD", valor_padrao="lakehouse-ouro")

        # -------------------------------------------------------------
        # MOTHERDUCK (DATA WAREHOUSE CLOUD)
        # -------------------------------------------------------------
        self.MOTHERDUCK_TOKEN = self._obter_valor("MOTHERDUCK_TOKEN")
        self.MOTHERDUCK_DATABASE_BRONZE = "bronze"
        self.MOTHERDUCK_DATABASE_PRATA = "prata"
        self.MOTHERDUCK_DATABASE_OURO = "ouro"

        # 4. Validacao defensiva (Fail-Fast)
        self._validar_credenciais_obrigatorias()

    def _obter_valor(self, nome_chave: str, valor_padrao: str = None) -> str:
        """
        Metodo auxiliar tradicional para buscar uma chave no leitor
        com fallback defensivo caso ela nao exista.
        """
        try:
            if valor_padrao is not None:
                valor = self._leitor(nome_chave, default=valor_padrao)
            else:
                valor = self._leitor(nome_chave)
            return valor
        except Exception:
            return valor_padrao

    def _validar_credenciais_obrigatorias(self):
        """
        Garante que nenhuma credencial vital esteja ausente,
        independentemente de ter vindo de um arquivo ou do SO.
        """
        variaveis_obrigatorias = [
            ("R2_ENDPOINT_URL", self.R2_ENDPOINT_URL),
            ("R2_ACCESS_KEY_ID", self.R2_ACCESS_KEY_ID),
            ("R2_SECRET_ACCESS_KEY", self.R2_SECRET_ACCESS_KEY),
            ("MOTHERDUCK_TOKEN", self.MOTHERDUCK_TOKEN)
        ]

        for item in variaveis_obrigatorias:
            nome_campo = item[0]
            valor_campo = item[1]

            if valor_campo is None:
                raise ValueError(
                    f"[ERRO AMBIENTAL] A variavel obrigatoria '{nome_campo}' nao foi encontrada nem em arquivo nem no Sistema Operacional."
                )

            texto_limpo = str(valor_campo).strip()
            if texto_limpo == "":
                raise ValueError(
                    f"[ERRO AMBIENTAL] A variavel obrigatoria '{nome_campo}' foi informada mas esta com valor vazio."
                )


# Instancia compartilhada (Singleton)
settings = Settings()