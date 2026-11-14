{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='cod_uf_ibge',
        file_format='delta'
    )
}}

SELECT
    CAST(CodUFibge AS INT) AS cod_uf_ibge,
    TRIM(UPPER(SigUF)) AS sigla_uf,
    CAST(CodRegiao AS INT) AS cod_regiao,
    CASE
        WHEN TRIM(UPPER(SigUF)) = 'AC' THEN 'Acre'
        WHEN TRIM(UPPER(SigUF)) = 'AL' THEN 'Alagoas'
        WHEN TRIM(UPPER(SigUF)) = 'AP' THEN 'Amapa'
        WHEN TRIM(UPPER(SigUF)) = 'AM' THEN 'Amazonas'
        WHEN TRIM(UPPER(SigUF)) = 'BA' THEN 'Bahia'
        WHEN TRIM(UPPER(SigUF)) = 'CE' THEN 'Ceara'
        WHEN TRIM(UPPER(SigUF)) = 'DF' THEN 'Distrito Federal'
        WHEN TRIM(UPPER(SigUF)) = 'ES' THEN 'Espirito Santo'
        WHEN TRIM(UPPER(SigUF)) = 'GO' THEN 'Goias'
        WHEN TRIM(UPPER(SigUF)) = 'MA' THEN 'Maranhao'
        WHEN TRIM(UPPER(SigUF)) = 'MT' THEN 'Mato Grosso'
        WHEN TRIM(UPPER(SigUF)) = 'MS' THEN 'Mato Grosso do Sul'
        WHEN TRIM(UPPER(SigUF)) = 'MG' THEN 'Minas Gerais'
        WHEN TRIM(UPPER(SigUF)) = 'PA' THEN 'Para'
        WHEN TRIM(UPPER(SigUF)) = 'PB' THEN 'Paraiba'
        WHEN TRIM(UPPER(SigUF)) = 'PR' THEN 'Parana'
        WHEN TRIM(UPPER(SigUF)) = 'PE' THEN 'Pernambuco'
        WHEN TRIM(UPPER(SigUF)) = 'PI' THEN 'Piaui'
        WHEN TRIM(UPPER(SigUF)) = 'RJ' THEN 'Rio de Janeiro'
        WHEN TRIM(UPPER(SigUF)) = 'RN' THEN 'Rio Grande do Norte'
        WHEN TRIM(UPPER(SigUF)) = 'RS' THEN 'Rio Grande do Sul'
        WHEN TRIM(UPPER(SigUF)) = 'RO' THEN 'Rondonia'
        WHEN TRIM(UPPER(SigUF)) = 'RR' THEN 'Roraima'
        WHEN TRIM(UPPER(SigUF)) = 'SC' THEN 'Santa Catarina'
        WHEN TRIM(UPPER(SigUF)) = 'SP' THEN 'Sao Paulo'
        WHEN TRIM(UPPER(SigUF)) = 'SE' THEN 'Sergipe'
        WHEN TRIM(UPPER(SigUF)) = 'TO' THEN 'Tocantins'
        ELSE NULL
    END AS nome_estado,
    CURRENT_TIMESTAMP() AS _silver_processed_at
FROM {{ source('bronze_gd', 'aneel_gd_empreendimentos') }}
WHERE CodUFibge IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CAST(CodUFibge AS INT)
    ORDER BY _ingestion_at DESC
) = 1
