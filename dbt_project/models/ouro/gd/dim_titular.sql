{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='sk_titular',
        file_format='delta'
    )
}}

SELECT DISTINCT
    xxhash64(
        CONCAT_WS(
            '||',
            COALESCE(tipo_consumidor, ''),
            COALESCE(nome_titular, ''),
            COALESCE(documento_titular, '')
        )
    ) AS sk_titular,
    tipo_consumidor,
    nome_titular,
    documento_titular,
    is_anonimizado_lgpd,
    CURRENT_TIMESTAMP() AS _gold_processed_at
FROM {{ ref('fato_empreendimentos_prata') }}
