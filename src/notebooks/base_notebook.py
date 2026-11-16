from typing import Any, Dict, List
from src.catalog_dictionary import listar_tabelas_catalogo, obter_contrato_por_tabela
from src.infrastructure.databricks_runtime import DatabricksRuntime
from src.infrastructure.dbt_runner import DbtRunner
from src.ingestion.base_extractor import BaseExtractor


class BaseNotebook:
    """
    Contrato Base para todos os Notebooks do Databricks (Bronze, Prata e Ouro).
    Responsabilidades:
      1. Garantir as dependencias do requirements.txt via DatabricksRuntime.
      2. Buscar dinamicamente as tabelas da camada e produto/fonte no catalog_dictionary.py (SSOT).
      3. Padronizar os Widgets ('modo_carga' e 'tabela_alvo': 'todas' + tabelas do catalogo).
      4. Delegar a execucao para o Extrator (na Bronze) ou para o DbtRunner (na Prata e Ouro).
    """

    CAMADAS_PERMITIDAS: List[str] = ["bronze", "prata", "ouro"]
    MODOS_CARGA_PERMITIDOS: List[str] = ["full", "incremental"]

    def __init__(
        self,
        camada: str,
        dominio_ou_fonte: str,
        extrator_bronze: BaseExtractor = None
    ) -> None:
        if camada is None or str(camada).strip() == "":
            raise ValueError("[BaseNotebook] O parametro 'camada' nao pode ser vazio.")

        camada_limpa = str(camada).strip().lower()
        if camada_limpa not in self.CAMADAS_PERMITIDAS:
            opcoes = ", ".join(self.CAMADAS_PERMITIDAS)
            raise ValueError(
                f"[BaseNotebook] Camada '{camada}' invalida. Opcoes permitidas: [{opcoes}]."
            )

        if dominio_ou_fonte is None or str(dominio_ou_fonte).strip() == "":
            raise ValueError("[BaseNotebook] O parametro 'dominio_ou_fonte' nao pode ser vazio.")

        if camada_limpa == "bronze":
            if extrator_bronze is None or isinstance(extrator_bronze, BaseExtractor) is False:
                raise TypeError(
                    "[BaseNotebook] Notebooks da camada 'bronze' exigem um 'extrator_bronze' "
                    "que herde de BaseExtractor."
                )

        self.camada: str = camada_limpa
        self.dominio_ou_fonte: str = str(dominio_ou_fonte).strip().lower()
        self.extrator_bronze: BaseExtractor = extrator_bronze
        self.tabelas_concluidas: List[str] = []

        self.runtime = DatabricksRuntime()
        self.dbt_runner = DbtRunner(runtime=self.runtime)

        # Garante obrigatoriamente todas as dependencias do requirements.txt antes de iniciar
        self.runtime.garantir_dependencias_requirements()

        # Fonte Unica da Verdade (SSOT): busca as tabelas dinamicamente no catalog_dictionary.py
        self.lista_tabelas: List[str] = listar_tabelas_catalogo(
            camada=self.camada,
            dominio_ou_fonte=self.dominio_ou_fonte
        )

        self.opcoes_modo_carga: List[str] = list(self.MODOS_CARGA_PERMITIDOS)
        self.opcoes_tabela_alvo: List[str] = ["todas"]
        for nome_tabela in self.lista_tabelas:
            self.opcoes_tabela_alvo.append(nome_tabela)

    def configurar_widgets(self, dbutils_obj: Any) -> None:
        """
        Cria os filtros visuais (Dropdown Widgets) padronizados no topo do Notebook do Databricks.
        """
        dbutils_obj.widgets.dropdown(
            "modo_carga",
            "full",
            self.opcoes_modo_carga,
            "1. Modo de Carga"
        )
        dbutils_obj.widgets.dropdown(
            "tabela_alvo",
            "todas",
            self.opcoes_tabela_alvo,
            "2. Tabela Alvo"
        )

    def validar_selecao_tabela(self, tabela_alvo: str) -> List[str]:
        """
        Retorna a lista de tabelas que serao executadas ('todas' retorna todas na ordem do catalogo;
        nome especifico retorna apenas a tabela solicitada).
        """
        if tabela_alvo is None or str(tabela_alvo).strip() == "":
            raise ValueError("[BaseNotebook] O parametro 'tabela_alvo' nao pode ser vazio.")

        alvo_limpo = str(tabela_alvo).strip()

        if alvo_limpo.lower() == "todas":
            return list(self.lista_tabelas)

        for tabela_catalogo in self.lista_tabelas:
            if tabela_catalogo.lower() == alvo_limpo.lower():
                return [tabela_catalogo]

        opcoes_validas = ", ".join(["todas"] + self.lista_tabelas)
        raise ValueError(
            f"[BaseNotebook] Tabela '{tabela_alvo}' invalida para '{self.camada}/{self.dominio_ou_fonte}'. "
            f"Opcoes permitidas: [{opcoes_validas}]."
        )

    def validar_modo_carga(self, modo_carga: str) -> str:
        """
        Valida se o modo de carga escolhido no Notebook pertence as opcoes permitidas ('full' ou 'incremental').
        """
        if modo_carga is None:
            raise ValueError("[BaseNotebook] O parametro 'modo_carga' nao pode ser nulo.")

        modo_limpo = str(modo_carga).strip().lower()

        if modo_limpo in self.opcoes_modo_carga:
            return modo_limpo

        opcoes_texto = ", ".join(self.opcoes_modo_carga)
        raise ValueError(
            f"[BaseNotebook] Modo de carga '{modo_carga}' invalido. "
            f"Opcoes permitidas: [{opcoes_texto}]."
        )

    def validar_parametros(self, modo_carga: str, tabela_alvo: str) -> tuple:
        """
        Valida se o modo_carga e a tabela_alvo escolhidos pertencem a lista de opcoes deste notebook.
        """
        modo_validado = self.validar_modo_carga(modo_carga=modo_carga)

        if tabela_alvo is None or str(tabela_alvo).strip() == "":
            raise ValueError("[BaseNotebook] O parametro 'tabela_alvo' nao pode ser vazio.")

        alvo_limpo = str(tabela_alvo).strip()
        tabela_encontrada = None

        for opcao in self.opcoes_tabela_alvo:
            if opcao.lower() == alvo_limpo.lower():
                tabela_encontrada = opcao
                break

        if tabela_encontrada is None:
            opcoes_texto = ", ".join(self.opcoes_tabela_alvo)
            raise ValueError(
                f"[BaseNotebook] Tabela '{tabela_alvo}' invalida para o notebook "
                f"'{self.camada}/{self.dominio_ou_fonte}'. Opcoes validas: [{opcoes_texto}]."
            )

        return (modo_validado, tabela_encontrada)

    def executar(
        self,
        modo_carga: str = "full",
        tabela_alvo: str = "todas",
        usar_landing_ip_brasil: bool = True
    ) -> Dict[str, str]:
        """
        Executa todas as tabelas ('todas') ou apenas a tabela selecionada deste notebook.
        """
        parametros_validados = self.validar_parametros(
            modo_carga=modo_carga,
            tabela_alvo=tabela_alvo
        )
        modo_validado = parametros_validados[0]
        tabela_validada = parametros_validados[1]
        tabelas_para_executar = self.validar_selecao_tabela(tabela_alvo=tabela_validada)

        resultados: Dict[str, str] = {}

        try:
            print("=================================================================")
            print(
                f"INICIANDO NOTEBOOK | CAMADA: {self.camada.upper()} | "
                f"DOMINIO/FONTE: {self.dominio_ou_fonte.upper()} | "
                f"TABELA: {tabela_validada.upper()} | MODO: {modo_validado.upper()}"
            )
            print("=================================================================")

            for nome_tabela in tabelas_para_executar:
                contrato = obter_contrato_por_tabela(nome_tabela)

                if self.camada == "bronze":
                    if usar_landing_ip_brasil is True:
                        uri_gerada = self.extrator_bronze.run_landing_brasil(
                            contrato_tabela=contrato,
                            modo_carga=modo_validado
                        )
                    else:
                        uri_gerada = self.extrator_bronze.run_parquet_direct(
                            contrato_tabela=contrato,
                            modo_carga=modo_validado
                        )
                    resultados[contrato.full_table_identifier] = f"Sucesso ({uri_gerada})"
                else:
                    self.dbt_runner.executar_modelo_dbt(
                        seletor_modelo=nome_tabela,
                        modo_carga=modo_validado
                    )
                    resultados[contrato.full_table_identifier] = (
                        f"Sucesso via dbt ({modo_validado.upper()})"
                    )

                if nome_tabela not in self.tabelas_concluidas:
                    self.tabelas_concluidas.append(nome_tabela)

            print("\n[SUCESSO] Resumo das tabelas processadas:")
            for nome_tabela_saida, status_tabela in resultados.items():
                print(f"  - {nome_tabela_saida}: {status_tabela}")

            return resultados

        except Exception as erro_execucao:
            print(
                f"\n[ERRO CRITICO] Execucao interrompida em "
                f"{self.camada}/{self.dominio_ou_fonte}: {str(erro_execucao)}"
            )
            raise erro_execucao

    def executar_com_widgets(self, dbutils_obj: Any) -> Dict[str, str]:
        """
        Lê os parametros selecionados nos Widgets do Databricks e dispara a execucao.
        """
        modo_selecionado = dbutils_obj.widgets.get("modo_carga").strip().lower()
        tabela_selecionada = dbutils_obj.widgets.get("tabela_alvo").strip()

        return self.executar(
            modo_carga=modo_selecionado,
            tabela_alvo=tabela_selecionada
        )
