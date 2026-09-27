import pytest
import pandas as pd

from src.ingestion.extractors.aneel_gd_extractor import AneelGdExtractor
from src.connectors.r2_connector import R2Connector


def test_aneel_gd_extractor_fluxo_integracao():
    """
    Testa o ciclo de vida do extrator ANEEL ponta a ponta:
    1. Instancia o extrator com URL oficial de Parquet.
    2. Executa o template method run() persistindo na Bronze.
    3. Verifica se as colunas de auditoria foram injetadas.
    4. Valida se o arquivo chegou integro ao Cloudflare R2.
    5. TEARDOWN: Remove o arquivo do bucket para nao gerar custo ou residuo.
    """
    caminho_teste_bronze = "aneel/gd/testes_ci/teste_extracao_aneel.parquet"
    conector = R2Connector()
    extrator = AneelGdExtractor()

    try:
        # ACT: Executa a extracao e gravacao na camada Bronze
        uri_gerada = extrator.run(target_key=caminho_teste_bronze)

        # ASSERT: Validacoes tecnicas de persistencia e schema
        assert uri_gerada.startswith("s3://")
        assert caminho_teste_bronze in uri_gerada

        # Le o Parquet de volta do Cloudflare R2 para atestar integridade
        df_validacao = conector.read_parquet(target_key=caminho_teste_bronze)

        assert isinstance(df_validacao, pd.DataFrame)
        assert len(df_validacao) > 0
        assert "_ingestion_at" in df_validacao.columns
        assert "_source_name" in df_validacao.columns
        assert df_validacao["_source_name"].iloc[0] == "aneel_gd"

        print(f"\n--> Teste validado com sucesso: {len(df_validacao):,} linhas no R2.")

    finally:
        # TEARDOWN: Executa a remocao do arquivo temporario obrigatoriamente
        print("--> Executando Teardown: removendo arquivo de teste do Cloudflare R2...")
        conector.delete_file(target_key=caminho_teste_bronze)