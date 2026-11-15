{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='cod_municipio_ibge',
        file_format='delta'
    )
}}

WITH municipios_base AS (
    SELECT DISTINCT
        CAST(CodMunicipioIbge AS INT) AS cod_municipio_ibge,
        TRIM(UPPER(NomMunicipio)) AS nome_municipio,
        CAST(CodUFibge AS INT) AS cod_uf_ibge
    FROM {{ source('bronze_aneel', 'aneel_gd_empreendimentos') }}
    WHERE CodMunicipioIbge IS NOT NULL
),

contagem_municipio_distribuidora AS (
    SELECT
        CAST(CodMunicipioIbge AS INT) AS cod_municipio_ibge,
        LPAD(CAST(NumCNPJDistribuidora AS STRING), 14, '0') AS cnpj_distribuidora,
        COUNT(*) AS total_conexoes,
        MAX(_ingestion_at) AS ultima_ingestao
    FROM {{ source('bronze_aneel', 'aneel_gd_empreendimentos') }}
    WHERE CodMunicipioIbge IS NOT NULL
      AND NumCNPJDistribuidora IS NOT NULL
    GROUP BY
        CAST(CodMunicipioIbge AS INT),
        LPAD(CAST(NumCNPJDistribuidora AS STRING), 14, '0')
),

municipios_distribuidora_map AS (
    SELECT
        cod_municipio_ibge,
        cnpj_distribuidora,
        total_conexoes
    FROM contagem_municipio_distribuidora
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cod_municipio_ibge
        ORDER BY total_conexoes DESC, ultima_ingestao DESC
    ) = 1
)

SELECT
    municipios_base.cod_municipio_ibge,
    municipios_base.nome_municipio,
    municipios_base.cod_uf_ibge,
    municipios_distribuidora_map.cnpj_distribuidora,
    CURRENT_TIMESTAMP() AS _silver_processed_at
FROM municipios_base
LEFT JOIN municipios_distribuidora_map
    ON municipios_base.cod_municipio_ibge = municipios_distribuidora_map.cod_municipio_ibge
