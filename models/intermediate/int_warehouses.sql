{{
    config(
        materialized='incremental',
        unique_key='warehouse_id'
    )
}}

WITH bronze_warehouses AS (
    SELECT * FROM {{ ref('bronze_warehouses') }}

    {% if is_incremental() %}
    WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY warehouse_id 
            ORDER BY ingestion_timestamp DESC
        ) AS row_num
    FROM bronze_warehouses
),

transformed AS (
    SELECT
        UPPER(TRIM(warehouse_id)) AS warehouse_id,

        TRIM(city) AS city,
        TRIM(state) AS state,

        ingestion_timestamp,
        _ingested_at AS bronze_ingested_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM transformed