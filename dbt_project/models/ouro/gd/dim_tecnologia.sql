{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='sk_tecnologia',
        file_format='delta'
    )
}}

SELECT DISTINCT
    xxhash64(
        CONCAT_WS(
            '||',
            COALESCE(sigla_tipo_geracao, ''),
            COALESCE(fonte_geracao, ''),
            COALESCE(porte_geracao, '')
        )
    ) AS sk_tecnologia,
    sigla_tipo_geracao,
    fonte_geracao,
    porte_geracao,
    CURRENT_TIMESTAMP() AS _gold_processed_at
FROM {{ ref('fato_empreendimentos_prata') }}
