import pytest
import pandas as pd
from src.ingestion.base_extractor import BaseExtractor


class ExtratorIncompleto(BaseExtractor):
    """Classe de teste que propositalmente nao implementa o metodo extract()."""
    pass


class ExtratorValido(BaseExtractor):
    """Classe de teste que implementa o contrato corretamente."""
    def extract(self) -> pd.DataFrame:
        registros = [
            {"id": 1, "sistema": "GD Solar"},
            {"id": 2, "sistema": "GD Biogás"}
        ]
        return pd.DataFrame(registros)


def test_base_extractor_bloqueia_instanciacao_direta():
    """
    Garante que a classe abstrata nao pode ser instanciada sem implementar extract().
    """
    with pytest.raises(TypeError):
        # Nao deve permitir instanciar uma classe abstrata diretamente
        BaseExtractor("origem_teste")


def test_base_extractor_bloqueia_filha_sem_extract():
    """
    Garante que classes filhas sem o metodo extract() sejam rejeitadas pelo Python.
    """
    with pytest.raises(TypeError):
        ExtratorIncompleto("origem_incompleta")


def test_base_extractor_validacao_nome_origem():
    """
    Garante validacao defensiva para o source_name (nao nulo e nao vazio).
    """
    with pytest.raises(ValueError):
        ExtratorValido(None)

    with pytest.raises(ValueError):
        ExtratorValido("   ")


def test_base_extractor_adiciona_metadados_auditoria():
    """
    Valida se as colunas de linhagem _ingestion_at e _source_name sao inseridas.
    """
    extrator = ExtratorValido("teste_unitario_fonte")
    df_amostra = pd.DataFrame([{"potencia": 100}])

    df_resultado = extrator.add_audit_metadata(df_amostra)

    # Assercoes tradicionais
    assert "_ingestion_at" in df_resultado.columns
    assert "_source_name" in df_resultado.columns
    assert df_resultado["_source_name"].iloc[0] == "teste_unitario_fonte"
    assert len(df_resultado) == 1