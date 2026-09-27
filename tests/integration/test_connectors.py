import pandas as pd
from src.connectors.r2_connector import R2Connector


def test_r2_connector_ciclo_completo():
    """
    Testa o ciclo de vida completo de um arquivo no Cloudflare R2:
    1. Upload do DataFrame serializado em Parquet.
    2. Leitura e validacao da integridade dos dados recuperados.
    3. Expurgo do arquivo de teste (Teardown).
    """
    # 1. SETUP: Criacao do conector e do dataset de teste
    conector = R2Connector()
    caminho_objeto_teste = "gd/testes_ci/validacao_integracao_r2.parquet"
    
    dados_amostra = [
        {"codigo_usina": "UFV-TESTE-1", "sigla_uf": "SP", "potencia_kw": 50.0},
        {"codigo_usina": "UFV-TESTE-2", "sigla_uf": "MG", "potencia_kw": 100.0}
    ]
    df_origem = pd.DataFrame(dados_amostra)

    # 2. ACTION: Execucao do upload
    uri_retorno = conector.upload_parquet(
        df=df_origem,
        target_key=caminho_objeto_teste
    )

    # Assercao 1: Valida se a URI gerada segue o padrao s3://
    assert uri_retorno.startswith("s3://")
    assert caminho_objeto_teste in uri_retorno

    # 3. ASSERT: Leitura e comparacao do volume de dados
    df_lido = conector.read_parquet(target_key=caminho_objeto_teste)
    
    linhas_origem = len(df_origem)
    linhas_recuperadas = len(df_lido)
    assert linhas_origem == linhas_recuperadas

    # Validacao de integridade das colunas
    colunas_origem = list(df_origem.columns)
    colunas_recuperadas = list(df_lido.columns)
    assert colunas_origem == colunas_recuperadas

    # 4. TEARDOWN: Limpeza do objeto no storage
    delecao_concluida = conector.delete_file(target_key=caminho_objeto_teste)
    assert delecao_concluida is True