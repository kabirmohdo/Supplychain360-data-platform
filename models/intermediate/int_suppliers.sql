{{
    config(
        materialized='incremental',
        unique_key='supplier_id'
    )
}}

WITH bronze_suppliers AS (
    SELECT * FROM {{ ref('bronze_suppliers') }}

    {% if is_incremental() %}
    WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY supplier_id 
            ORDER BY ingestion_timestamp DESC
        ) AS row_num
    FROM bronze_suppliers
),

transformed AS (
    SELECT
        UPPER(TRIM(supplier_id)) AS supplier_id,

        TRIM(supplier_name) AS supplier_name,
        TRIM(category) AS category,
        TRIM(country) AS country,

        ingestion_timestamp,
        _ingested_at AS bronze_ingested_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM transformed