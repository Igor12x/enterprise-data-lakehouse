{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='cod_regiao',
        file_format='delta'
    )
}}

SELECT
    CAST(CodRegiao AS INT) AS cod_regiao,
    TRIM(UPPER(NomRegiao)) AS nome_regiao,
    CURRENT_TIMESTAMP() AS _silver_processed_at
FROM {{ source('bronze_gd', 'aneel_gd_empreendimentos') }}
WHERE CodRegiao IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CAST(CodRegiao AS INT)
    ORDER BY _ingestion_at DESC
) = 1
