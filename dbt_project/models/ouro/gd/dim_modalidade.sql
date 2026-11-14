{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='sk_modalidade',
        file_format='delta'
    )
}}

SELECT DISTINCT
    xxhash64(COALESCE(modalidade_compensacao, '')) AS sk_modalidade,
    modalidade_compensacao,
    CURRENT_TIMESTAMP() AS _gold_processed_at
FROM {{ ref('fato_empreendimentos_prata') }}
