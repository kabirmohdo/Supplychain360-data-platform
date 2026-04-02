
{{
    config(
        materialized='incremental',
        unique_key='transaction_id'
    )
}}

WITH bronze_sales AS (
    SELECT * FROM {{ ref('stg_stores_sales') }}
    {% if is_incremental() %}
    WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id 
            ORDER BY ingestion_timestamp DESC
        ) AS row_num
    FROM stg_stores_sales
),

transformed AS (
    SELECT
        UPPER(TRIM(transaction_id)) AS transaction_id,
        UPPER(TRIM(store_id)) AS store_id,
        UPPER(TRIM(product_id)) AS product_id,

        CAST(quantity_sold AS INTEGER) AS quantity_sold,
        CAST(unit_price AS NUMERIC(12, 2)) AS unit_price,
        
        -- Handle null discounts as 0
        COALESCE(CAST(discount_pct AS NUMERIC(5, 2)), 0) AS discount_pct,
        
        -- Calculated Business Logic
        CAST(sale_amount AS NUMERIC(15, 2)) AS gross_sale_amount,
        
        -- Net amount logic: (Sale * (1 - Discount))
        CAST(
            sale_amount * (1 - COALESCE(discount_pct, 0)) 
            AS NUMERIC(15, 2)
        ) AS net_sale_amount,

        CAST(transaction_timestamp AS TIMESTAMP) AS transaction_at,
        CAST(transaction_timestamp AS DATE) AS transaction_date,

        ingestion_timestamp,
        _ingested_at AS stg_ingested_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM transformed