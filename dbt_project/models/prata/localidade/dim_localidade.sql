{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='cod_municipio_ibge',
        file_format='delta'
    )
}}

SELECT
    municipios.cod_municipio_ibge,
    municipios.nome_municipio,
    municipios.cnpj_distribuidora,
    ufs.cod_uf_ibge,
    ufs.sigla_uf,
    ufs.nome_estado,
    regioes.cod_regiao,
    regioes.nome_regiao,
    distribuidoras.nome_distribuidora,
    distribuidoras.sigla_distribuidora,
    CURRENT_TIMESTAMP() AS _silver_processed_at
FROM {{ ref('dim_municipios') }} AS municipios
LEFT JOIN {{ ref('dim_ufs') }} AS ufs
    ON municipios.cod_uf_ibge = ufs.cod_uf_ibge
LEFT JOIN {{ ref('dim_regioes') }} AS regioes
    ON ufs.cod_regiao = regioes.cod_regiao
LEFT JOIN {{ ref('dim_distribuidoras') }} AS distribuidoras
    ON municipios.cnpj_distribuidora = distribuidoras.cnpj_distribuidora
