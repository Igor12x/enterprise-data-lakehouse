{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='cod_classe_consumo',
        file_format='delta'
    )
}}

SELECT
    TRIM(CodClasseConsumo) AS cod_classe_consumo,
    TRIM(DscClasseConsumo) AS descricao_classe,
    CURRENT_TIMESTAMP() AS _silver_processed_at
FROM {{ source('bronze_gd', 'aneel_gd_empreendimentos') }}
WHERE CodClasseConsumo IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRIM(CodClasseConsumo)
    ORDER BY _ingestion_at DESC
) = 1
