from pathlib import Path
import pytest
import pandas as pd
from src.ingestion.base_extractor import BaseExtractor


class ExtratorIncompleto(BaseExtractor):
    """Classe de teste que propositalmente nao implementa o metodo extract()."""
    pass


class ExtratorValido(BaseExtractor):
    """Classe de teste que implementa tanto o contrato Pandas quanto o Parquet Direto."""

    def extract(self) -> pd.DataFrame:
        registros = [
            {"id": 1, "sistema": "GD Solar"},
            {"id": 2, "sistema": "GD Biogas"}
        ]
        dataframe_retorno = pd.DataFrame(registros)
        return dataframe_retorno


def test_base_extractor_bloqueia_instanciacao_direta():
    """Garante que a classe abstrata nao pode ser instanciada diretamente."""
    with pytest.raises(TypeError):
        BaseExtractor("origem_teste")


def test_base_extractor_bloqueia_filha_sem_extract():
    """Garante que classes filhas sem o metodo extract() sejam rejeitadas."""
    with pytest.raises(TypeError):
        ExtratorIncompleto("origem_incompleta")


def test_base_extractor_validacao_nome_origem():
    """Garante validacao defensiva para o source_name (nao nulo e nao vazio)."""
    with pytest.raises(ValueError):
        ExtratorValido(None)

    with pytest.raises(ValueError):
        ExtratorValido("   ")


def test_base_extractor_adiciona_metadados_auditoria():
    """Valida se as colunas de linhagem _ingestion_at e _source_name sao inseridas no fluxo Pandas."""
    extrator = ExtratorValido("teste_unitario_fonte")
    dataframe_amostra = pd.DataFrame([{"potencia": 100}])

    dataframe_resultado = extrator.add_audit_metadata(dataframe_bruto=dataframe_amostra)

    assert "_ingestion_at" in dataframe_resultado.columns
    assert "_source_name" in dataframe_resultado.columns
    assert dataframe_resultado["_source_name"].iloc[0] == "teste_unitario_fonte"
    assert len(dataframe_resultado) == 1


def test_base_extractor_validacao_arquivo_parquet_direto(tmp_path: Path):
    """Valida a verificacao de arquivo .parquet direto (inexistente, vazio e valido)."""
    extrator = ExtratorValido("teste_parquet_direto")

    with pytest.raises(ValueError):
        extrator._validate_parquet_file("")

    with pytest.raises(FileNotFoundError):
        extrator._validate_parquet_file(str(tmp_path / "nao_existe.parquet"))

    arquivo_vazio = tmp_path / "vazio.parquet"
    arquivo_vazio.write_bytes(b"")
    with pytest.raises(ValueError):
        extrator._validate_parquet_file(str(arquivo_vazio))

    arquivo_valido = tmp_path / "valido.parquet"
    arquivo_valido.write_bytes(b"PAR1_CONTEUDO_PARQUET_SIMULADO")
    extrator._validate_parquet_file(str(arquivo_valido))
