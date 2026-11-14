import os
import subprocess
from pathlib import Path
from typing import Dict, List
from src.catalog_dictionary import get_data_product, obter_contrato_por_tabela
from src.connectors.adls_databricks_connector import AdlsDatabricksConnector
from src.ingestion.extractors.aneel_gd_extractor import AneelGdExtractor


class FlowAneel:
    """
    Orquestrador central do Lakehouse (estilo classe de servico em Java/C#).
    Da ANEEL e extraida apenas 1 tabela na Bronze ('aneel_gd_empreendimentos'),
    lida diretamente em formato .parquet sem conversao intermediaria para Pandas,
    gerando as 8 tabelas Prata e as 5 tabelas Ouro (Star Schema para Power BI),
    respeitando rigorosamente a ordem de dependencia entre as tabelas.
    """

    # 1. Unica tabela extraida da ANEEL na camada Bronze
    TABELA_BRONZE_ANEEL: str = "aneel_gd_empreendimentos"

    # 2. Ordem estrita de execucao da Camada Prata (respeita hierarquia e JOINs)
    ORDEM_TABELAS_PRATA: List[str] = [
        "dim_calendario",
        "dim_regioes",
        "dim_ufs",
        "dim_distribuidoras",
        "dim_classes_consumo",
        "dim_municipios",
        "dim_localidade",
        "fato_empreendimentos_prata"
    ]

    # 3. Ordem estrita de execucao da Camada Ouro (Dimensoes primeiro, Fato por ultimo)
    ORDEM_TABELAS_OURO: List[str] = [
        "dim_enquadramento",
        "dim_titular",
        "dim_tecnologia",
        "dim_modalidade",
        "fato_empreendimentos"
    ]

    # 4. Mapa explicito de dependencias (impede gerar uma tabela sem que as tabelas mae existam)
    MAPA_DEPENDENCIAS: Dict[str, List[str]] = {
        "aneel_gd_empreendimentos": [],
        "dim_calendario": [],
        "dim_regioes": ["aneel_gd_empreendimentos"],
        "dim_ufs": ["aneel_gd_empreendimentos", "dim_regioes"],
        "dim_distribuidoras": ["aneel_gd_empreendimentos"],
        "dim_classes_consumo": ["aneel_gd_empreendimentos"],
        "dim_municipios": ["aneel_gd_empreendimentos", "dim_ufs", "dim_distribuidoras"],
        "dim_localidade": ["dim_regioes", "dim_ufs", "dim_distribuidoras", "dim_municipios"],
        "fato_empreendimentos_prata": [
            "aneel_gd_empreendimentos",
            "dim_municipios",
            "dim_distribuidoras",
            "dim_classes_consumo"
        ],
        "dim_enquadramento": ["dim_classes_consumo", "fato_empreendimentos_prata"],
        "dim_titular": ["fato_empreendimentos_prata"],
        "dim_tecnologia": ["fato_empreendimentos_prata"],
        "dim_modalidade": ["fato_empreendimentos_prata"],
        "fato_empreendimentos": [
            "dim_calendario",
            "dim_distribuidoras",
            "dim_localidade",
            "fato_empreendimentos_prata",
            "dim_enquadramento",
            "dim_titular",
            "dim_tecnologia",
            "dim_modalidade"
        ]
    }

    def __init__(self) -> None:
        self.produto_gd = get_data_product("gd")
        self.extrator_aneel = AneelGdExtractor()
        self.conector_lakehouse = AdlsDatabricksConnector()
        self.tabelas_concluidas: List[str] = []

        # Localiza a pasta dbt_project na raiz do repositorio
        diretorio_raiz = Path(__file__).resolve().parent.parent.parent
        self.diretorio_dbt = diretorio_raiz / "dbt_project"

    def validar_dependencias_tabela(self, nome_tabela: str) -> None:
        """
        Verifica se todas as tabelas mae das quais 'nome_tabela' depende
        ja foram executadas nesta sessao ou ja existem no catalogo do Databricks.
        """
        if nome_tabela not in self.MAPA_DEPENDENCIAS:
            raise KeyError(
                f"[Orquestrador] A tabela '{nome_tabela}' nao possui mapeamento de dependencia."
            )

        lista_tabelas_mae = self.MAPA_DEPENDENCIAS[nome_tabela]

        for nome_tabela_mae in lista_tabelas_mae:
            ja_rodou_nesta_sessao = nome_tabela_mae in self.tabelas_concluidas

            if ja_rodou_nesta_sessao is False:
                contrato_mae = obter_contrato_por_tabela(nome_tabela_mae)
                existe_no_catalogo = self.conector_lakehouse.verificar_se_tabela_existe(
                    identificador_completo_sql=contrato_mae.full_table_identifier
                )

                if existe_no_catalogo is False:
                    raise RuntimeError(
                        f"[DEPENDENCIA BLOQUEADA] A tabela '{nome_tabela}' nao pode ser gerada "
                        f"porque depende da tabela '{contrato_mae.full_table_identifier}', "
                        f"que ainda nao existe no catalogo."
                    )

    def montar_comando_dbt(self, seletor_modelo: str, modo_carga: str) -> List[str]:
        """
        Constroi a lista de argumentos para executar o dbt run em modo full ou incremental.
        """
        modo_validado = self.conector_lakehouse.validar_modo_carga(modo_carga=modo_carga)

        comando: List[str] = [
            "dbt",
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

    def executar_modelo_dbt(self, nome_modelo_ou_pasta: str, modo_carga: str = "full") -> str:
        """
        Valida as dependencias da tabela, executa a transformacao SQL no dbt
        e roda os testes de qualidade (chave primaria unica e nao nula).
        """
        if nome_modelo_ou_pasta in self.MAPA_DEPENDENCIAS:
            self.validar_dependencias_tabela(nome_tabela=nome_modelo_ou_pasta)

        ambiente_dbt = self._obter_ambiente_execucao_dbt()

        comando_run = self.montar_comando_dbt(
            seletor_modelo=nome_modelo_ou_pasta,
            modo_carga=modo_carga
        )

        print(f"--> [dbt] Gerando tabela '{nome_modelo_ou_pasta}': {' '.join(comando_run)}")
        resultado_run = subprocess.run(comando_run, env=ambiente_dbt, check=False)

        if resultado_run.returncode != 0:
            raise RuntimeError(
                f"[dbt] Falha ao gerar a tabela '{nome_modelo_ou_pasta}' no dbt run."
            )

        comando_test: List[str] = [
            "dbt",
            "test",
            "--project-dir",
            str(self.diretorio_dbt),
            "--profiles-dir",
            str(self.diretorio_dbt),
            "--select",
            nome_modelo_ou_pasta
        ]

        print(f"--> [dbt] Testando qualidade de '{nome_modelo_ou_pasta}': {' '.join(comando_test)}")
        resultado_test = subprocess.run(comando_test, env=ambiente_dbt, check=False)

        if resultado_test.returncode != 0:
            raise RuntimeError(
                f"[dbt] Os testes de qualidade falharam para a tabela '{nome_modelo_ou_pasta}'."
            )

        if nome_modelo_ou_pasta not in self.tabelas_concluidas:
            self.tabelas_concluidas.append(nome_modelo_ou_pasta)

        return f"Sucesso ({modo_carga})"

    def executar_tabela_bronze(
        self,
        chave_tabela: str = "aneel_gd_empreendimentos",
        modo_carga: str = "full",
        usar_landing_ip_brasil: bool = True
    ) -> str:
        """
        Executa a ingestao da tabela bruta da ANEEL para a camada Bronze.
        Permite escolher entre:
          - usar_landing_ip_brasil=True: Opcao para fontes que exigem IP no Brasil (lê da Landing Zone ADLS).
          - usar_landing_ip_brasil=False: Metodo tradicional de download direto do .parquet local.
        """
        contrato_bronze = self.produto_gd.get_bronze_table(chave_tabela)

        if usar_landing_ip_brasil is True:
            uri_gerada = self.extrator_aneel.run_landing_brasil(
                contrato_tabela=contrato_bronze,
                modo_carga=modo_carga
            )
        else:
            uri_gerada = self.extrator_aneel.run_parquet_direct(
                contrato_tabela=contrato_bronze,
                modo_carga=modo_carga
            )

        if chave_tabela not in self.tabelas_concluidas:
            self.tabelas_concluidas.append(chave_tabela)

        return uri_gerada

    def run_bronze(self, modo_carga: str = "full") -> Dict[str, str]:
        """
        Etapa 1: Extrai a tabela unica da ANEEL na camada Bronze (Parquet Direto).
        """
        resultados: Dict[str, str] = {}
        uri = self.executar_tabela_bronze(
            chave_tabela=self.TABELA_BRONZE_ANEEL,
            modo_carga=modo_carga
        )
        resultados[self.TABELA_BRONZE_ANEEL] = f"Sucesso ({uri})"
        return resultados

    def run_prata(self, modo_carga: str = "full") -> Dict[str, str]:
        """
        Etapa 2: Executa as 8 tabelas Prata uma por uma na ordem exata de dependencia.
        """
        resultados: Dict[str, str] = {}

        for nome_tabela_prata in self.ORDEM_TABELAS_PRATA:
            contrato = obter_contrato_por_tabela(nome_tabela_prata)
            status = self.executar_modelo_dbt(
                nome_modelo_ou_pasta=nome_tabela_prata,
                modo_carga=modo_carga
            )
            resultados[contrato.full_table_identifier] = status

        return resultados

    def run_ouro(self, modo_carga: str = "full") -> Dict[str, str]:
        """
        Etapa 3: Executa as 5 tabelas Ouro na ordem do Star Schema (Dimensoes -> Fato).
        """
        resultados: Dict[str, str] = {}

        for nome_tabela_ouro in self.ORDEM_TABELAS_OURO:
            contrato = obter_contrato_por_tabela(nome_tabela_ouro)
            status = self.executar_modelo_dbt(
                nome_modelo_ou_pasta=nome_tabela_ouro,
                modo_carga=modo_carga
            )
            resultados[contrato.full_table_identifier] = status

        return resultados

    def run(self, modo_carga: str = "full") -> Dict[str, str]:
        """
        Executa a pipeline completa de ponta a ponta respeitando todas as dependencias:
        1. Bronze (1 tabela ANEEL Parquet Direto) -> 2. Prata (8 tabelas) -> 3. Ouro (5 tabelas).
        """
        modo_validado = self.conector_lakehouse.validar_modo_carga(modo_carga=modo_carga)
        resultados_gerais: Dict[str, str] = {}

        resultados_bronze = self.run_bronze(modo_carga=modo_validado)
        for chave, valor in resultados_bronze.items():
            resultados_gerais[chave] = valor

        resultados_prata = self.run_prata(modo_carga=modo_validado)
        for chave, valor in resultados_prata.items():
            resultados_gerais[chave] = valor

        resultados_ouro = self.run_ouro(modo_carga=modo_validado)
        for chave, valor in resultados_ouro.items():
            resultados_gerais[chave] = valor

        return resultados_gerais
