{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='sk_empreendimento',
        file_format='delta'
    )
}}

SELECT
    xxhash64(cod_empreendimento) AS sk_empreendimento,
    cod_municipio_ibge,
    CAST(cnpj_distribuidora AS BIGINT) AS cnpj_distribuidora,
    xxhash64(
        CONCAT_WS(
            '||',
            COALESCE(cod_classe_consumo, ''),
            COALESCE(subgrupo_tarifario, '')
        )
    ) AS sk_enquadramento,
    xxhash64(
        CONCAT_WS(
            '||',
            COALESCE(tipo_consumidor, ''),
            COALESCE(nome_titular, ''),
            COALESCE(documento_titular, '')
        )
    ) AS sk_titular,
    xxhash64(
        CONCAT_WS(
            '||',
            COALESCE(sigla_tipo_geracao, ''),
            COALESCE(fonte_geracao, ''),
            COALESCE(porte_geracao, '')
        )
    ) AS sk_tecnologia,
    xxhash64(COALESCE(modalidade_compensacao, '')) AS sk_modalidade,
    CAST(DATE_FORMAT(data_conexao, 'yyyyMMdd') AS INT) AS id_data_conexao,
    data_conexao,
    potencia_instalada_kw,
    potencia_instalada_mw,
    qtd_uc_beneficiadas,
    CAST(potencia_instalada_kw / NULLIF(qtd_uc_beneficiadas, 0) AS DECIMAL(12, 3)) AS kw_por_uc,
    1 AS qtd_empreendimentos,
    CURRENT_TIMESTAMP() AS _gold_processed_at
FROM {{ ref('fato_empreendimentos_prata') }}
