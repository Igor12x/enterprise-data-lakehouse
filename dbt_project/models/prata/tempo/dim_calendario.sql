{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='id_data',
        file_format='delta'
    )
}}

WITH limites_datas AS (
    SELECT
        CAST('2000-01-01' AS DATE) AS data_inicio,
        CAST(ADD_MONTHS(DATE_TRUNC('YEAR', CURRENT_DATE()), 24) - INTERVAL 1 DAY AS DATE) AS data_fim
),

serie_datas AS (
    SELECT
        EXPLODE(SEQUENCE(data_inicio, data_fim, INTERVAL 1 DAY)) AS data_calendario
    FROM limites_datas
)

SELECT
    CAST(DATE_FORMAT(data_calendario, 'yyyyMMdd') AS INT) AS id_data,
    data_calendario AS data,
    CAST(DATE_TRUNC('MONTH', data_calendario) AS DATE) AS data_inicio_mes,
    YEAR(data_calendario) AS ano,
    CASE
        WHEN MONTH(data_calendario) <= 6 THEN 1
        ELSE 2
    END AS semestre,
    CASE
        WHEN MONTH(data_calendario) <= 6 THEN '1º Semestre'
        ELSE '2º Semestre'
    END AS nome_semestre,
    QUARTER(data_calendario) AS trimestre,
    CONCAT(CAST(QUARTER(data_calendario) AS STRING), 'º Trimestre') AS trimestre_nome,
    MONTH(data_calendario) AS mes,
    CAST(DATE_FORMAT(data_calendario, 'yyyyMM') AS INT) AS ano_mes,
    DATE_FORMAT(data_calendario, 'MM/yyyy') AS mes_ano,
    CASE MONTH(data_calendario)
        WHEN 1 THEN 'Janeiro'
        WHEN 2 THEN 'Fevereiro'
        WHEN 3 THEN 'Março'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Maio'
        WHEN 6 THEN 'Junho'
        WHEN 7 THEN 'Julho'
        WHEN 8 THEN 'Agosto'
        WHEN 9 THEN 'Setembro'
        WHEN 10 THEN 'Outubro'
        WHEN 11 THEN 'Novembro'
        WHEN 12 THEN 'Dezembro'
    END AS nome_mes,
    CASE MONTH(data_calendario)
        WHEN 1 THEN 'Jan'
        WHEN 2 THEN 'Fev'
        WHEN 3 THEN 'Mar'
        WHEN 4 THEN 'Abr'
        WHEN 5 THEN 'Mai'
        WHEN 6 THEN 'Jun'
        WHEN 7 THEN 'Jul'
        WHEN 8 THEN 'Ago'
        WHEN 9 THEN 'Set'
        WHEN 10 THEN 'Out'
        WHEN 11 THEN 'Nov'
        WHEN 12 THEN 'Dez'
    END AS nome_mes_abrev,
    WEEKOFYEAR(data_calendario) AS semana_ano,
    DAY(data_calendario) AS dia,
    DAYOFYEAR(data_calendario) AS dia_ano,
    DAYOFWEEK(data_calendario) AS dia_semana,
    CASE DAYOFWEEK(data_calendario)
        WHEN 1 THEN 'Domingo'
        WHEN 2 THEN 'Segunda-feira'
        WHEN 3 THEN 'Terça-feira'
        WHEN 4 THEN 'Quarta-feira'
        WHEN 5 THEN 'Quinta-feira'
        WHEN 6 THEN 'Sexta-feira'
        WHEN 7 THEN 'Sábado'
    END AS nome_dia_semana,
    CASE
        WHEN DAYOFWEEK(data_calendario) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS is_fim_de_semana,
    CURRENT_TIMESTAMP() AS _silver_processed_at
FROM serie_datas
