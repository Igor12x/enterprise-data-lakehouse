from src.flows.flow_aneel import FlowAneel


def main() -> None:
    # Lista com todos os fluxos que devem rodar na ingestao
    fluxos = [
        FlowAneel(),
        # Novos fluxos entram aqui facilmente:
        # FlowCcee(),
        # FlowOns(),
    ]

    for fluxo in fluxos:
        nome_fluxo = fluxo.__class__.__name__
        try:
            print(f"--> Iniciando execucao do {nome_fluxo}...")
            fluxo.run()
            print(f"[SUCESSO] {nome_fluxo} executado com exito!\n")
        except Exception as erro:
            print(f"[ERRO] Falha ao executar {nome_fluxo}: {str(erro)}\n")


if __name__ == "__main__":
    main()