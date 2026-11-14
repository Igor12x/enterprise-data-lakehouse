{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='cnpj_distribuidora',
        file_format='delta'
    )
}}

SELECT
    LPAD(CAST(NumCNPJDistribuidora AS STRING), 14, '0') AS cnpj_distribuidora,
    TRIM(SigAgente) AS sigla_distribuidora,
    TRIM(UPPER(NomAgente)) AS nome_distribuidora,
    CURRENT_TIMESTAMP() AS _silver_processed_at
FROM {{ source('bronze_gd', 'aneel_gd_empreendimentos') }}
WHERE NumCNPJDistribuidora IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY LPAD(CAST(NumCNPJDistribuidora AS STRING), 14, '0')
    ORDER BY _ingestion_at DESC
) = 1
