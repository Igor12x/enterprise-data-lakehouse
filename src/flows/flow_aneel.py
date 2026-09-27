from typing import List, Dict, Any
from src.flows.base_flow import BaseFlow
from src.ingestion.extractors.aneel_gd_extractor import AneelGdExtractor
# Quando criar os extratores tecnicos adicionais, basta importar aqui:
# from src.ingestion.extractors.aneel_gd_tecnica_solar_extractor import AneelGdTecnicaSolarExtractor


class FlowAneel(BaseFlow):
    """
    Orquestrador de dominio para o ecossistema de dados da ANEEL GD.
    Coordena a extracao e catalogacao de multiplas fontes correlacionadas
    (empreendimentos, inversores, modulos tecnicos) para a camada Bronze.
    """

    SCHEMA_DESTINO = "gd"
    PREFIXO_STORAGE = "aneel/gd"

    def __init__(self):
        super().__init__(flow_name="flow_aneel_gd")

        # Registro declarativo das fontes pertencentes ao dominio ANEEL GD
        self.tarefas_ingestao: List[Dict[str, Any]] = [
            {
                "nome_tabela": "aneel_gd",
                "extrator": AneelGdExtractor(),
                "target_key": f"{self.PREFIXO_STORAGE}/empreendimentos_gd.parquet",
                "descricao": "Relacao Geral de Empreendimentos de GD"
            },
            # Exemplo de expansao imediata para novos datasets:
            # {
            #     "nome_tabela": "aneel_gd_tecnica_fotovoltaica",
            #     "extrator": AneelGdTecnicaSolarExtractor(),
            #     "target_key": f"{self.PREFIXO_STORAGE}/info_tecnica_fotovoltaica.parquet",
            #     "descricao": "Dados Tecnicos de Modulos e Inversores Solares"
            # }
        ]

    def run(self) -> Dict[str, str]:
        """
        Executa sequencialmente a ingestao de todas as tabelas registradas no dominio.
        Retorna um dicionario com o status de persistencia de cada tabela.
        """
        print(f"\n=======================================================")
        print(f"   INICIANDO ORQUESTRACAO DO DOMINIO: {self.flow_name.upper()}   ")
        print(f"   Total de tabelas configuradas: {len(self.tarefas_ingestao)}   ")
        print(f"=======================================================")

        resultados: Dict[str, str] = {}

        for tarefa in self.tarefas_ingestao:
            nome_tabela = tarefa["nome_tabela"]
            extrator = tarefa["extrator"]
            caminho_destino = tarefa["target_key"]
            descricao = tarefa["descricao"]

            print(f"\n--> [FlowAneel] Processando: {descricao} ({nome_tabela})")
            
            try:
                uri_gerada = extrator.run(
                    target_key=caminho_destino,
                    schema_name=self.SCHEMA_DESTINO,
                    criar_como_tabela_fisica=False
                )
                resultados[nome_tabela] = f"Sucesso ({uri_gerada})"
                print(f"--> [FlowAneel] {nome_tabela} concluida e catalogada!")
            except Exception as erro:
                mensagem_falha = f"Falha: {str(erro)}"
                resultados[nome_tabela] = mensagem_falha
                print(f"[ERRO CRITICO] Erro ao processar tabela {nome_tabela}: {str(erro)}")
                # Se falhar uma tabela, levanta o erro para notificar o orquestrador geral
                raise erro

        print(f"\n=======================================================")
        print(f"   DOMINIO {self.flow_name.upper()} FINALIZADO COM SUCESSO   ")
        print(f"=======================================================\n")

        return resultados