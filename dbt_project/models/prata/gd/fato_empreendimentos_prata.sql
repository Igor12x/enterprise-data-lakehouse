{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='cod_empreendimento',
        file_format='delta'
    )
}}

SELECT
    TRIM(CodEmpreendimento) AS cod_empreendimento,
    CAST(CodMunicipioIbge AS INT) AS cod_municipio_ibge,
    LPAD(CAST(NumCNPJDistribuidora AS STRING), 14, '0') AS cnpj_distribuidora,
    TRIM(CodClasseConsumo) AS cod_classe_consumo,
    TRIM(REPLACE(CodCEP, '-', '')) AS cep,
    UPPER(TRIM(SigTipoConsumidor)) AS tipo_consumidor,
    CASE
        WHEN NomTitularEmpreendimento = '***' THEN 'ANONIMIZADO (LGPD)'
        ELSE TRIM(UPPER(NomTitularEmpreendimento))
    END AS nome_titular,
    CASE
        WHEN TRIM(NumCPFCNPJ) LIKE '%***%' THEN 'ANONIMIZADO (LGPD)'
        ELSE TRIM(NumCPFCNPJ)
    END AS documento_titular,
    CASE
        WHEN NomTitularEmpreendimento = '***' THEN TRUE
        ELSE FALSE
    END AS is_anonimizado_lgpd,
    TRIM(DscSubGrupoTarifario) AS subgrupo_tarifario,
    TRIM(DscModalidadeHabilitado) AS modalidade_compensacao,
    TRIM(SigTipoGeracao) AS sigla_tipo_geracao,
    TRIM(DscFonteGeracao) AS fonte_geracao,
    TRIM(DscPorte) AS porte_geracao,
    CAST(MdaPotenciaInstaladaKW AS DECIMAL(12, 3)) AS potencia_instalada_kw,
    ROUND(CAST(MdaPotenciaInstaladaKW AS DECIMAL(12, 3)) / 1000.0, 4) AS potencia_instalada_mw,
    COALESCE(CAST(QtdUCRecebeCredito AS INT), 1) AS qtd_uc_beneficiadas,
    CAST(DthAtualizaCadastralEmpreend AS DATE) AS data_conexao,
    CAST(DatGeracaoConjuntoDados AS DATE) AS data_extracao_conjunto,
    TRIM(AnmPeriodoReferencia) AS anm_periodo_referencia,
    CAST(_ingestion_at AS TIMESTAMP) AS _bronze_ingestion_at,
    CURRENT_TIMESTAMP() AS _silver_processed_at
FROM {{ source('bronze_gd', 'aneel_gd_empreendimentos') }}
WHERE CodEmpreendimento IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRIM(CodEmpreendimento)
    ORDER BY DthAtualizaCadastralEmpreend DESC, _ingestion_at DESC
) = 1
