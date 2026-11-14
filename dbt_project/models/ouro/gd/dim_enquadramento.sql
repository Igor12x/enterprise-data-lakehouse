{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='sk_enquadramento',
        file_format='delta'
    )
}}

SELECT DISTINCT
    xxhash64(
        CONCAT_WS(
            '||',
            COALESCE(empreendimentos.cod_classe_consumo, ''),
            COALESCE(empreendimentos.subgrupo_tarifario, '')
        )
    ) AS sk_enquadramento,
    empreendimentos.cod_classe_consumo,
    classes_consumo.descricao_classe,
    empreendimentos.subgrupo_tarifario,
    CASE
        WHEN empreendimentos.subgrupo_tarifario LIKE 'A%' THEN 'Alta Tensão (Grupo A)'
        WHEN empreendimentos.subgrupo_tarifario LIKE 'B%' THEN 'Baixa Tensão (Grupo B)'
        ELSE 'Não Identificado'
    END AS grupo_tensao,
    CURRENT_TIMESTAMP() AS _gold_processed_at
FROM {{ ref('fato_empreendimentos_prata') }} AS empreendimentos
LEFT JOIN {{ ref('dim_classes_consumo') }} AS classes_consumo
    ON empreendimentos.cod_classe_consumo = classes_consumo.cod_classe_consumo
