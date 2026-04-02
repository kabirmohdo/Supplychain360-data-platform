{{
    config(
        materialized='incremental',
        unique_key='store_id'
    )
}}

WITH stg_stores_details AS (
    SELECT * FROM {{ ref('stg_stores_details') }}

    {% if is_incremental() %}
    WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY store_id 
            ORDER BY ingestion_timestamp DESC
        ) AS row_num
    FROM stg_stores_details
),

transformed AS (
    SELECT
        UPPER(TRIM(store_id)) AS store_id,

        TRIM(store_name) AS store_name,
        TRIM(city) AS city,
        TRIM(state) AS state,
        TRIM(region) AS region,
        
        CAST(store_open_date AS DATE) AS store_open_date,

        ingestion_timestamp,
        _ingested_at AS stg_ingested_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM transformed