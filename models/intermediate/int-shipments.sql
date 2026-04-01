{{
    config(
        materialized='incremental',
        unique_key='shipment_id'
    )
}}

WITH bronze_shipments AS (
    SELECT * FROM {{ ref('bronze_shipments') }}

    {% if is_incremental() %}
    WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY shipment_id 
            ORDER BY ingestion_timestamp DESC
        ) AS row_num
    FROM bronze_shipments
),

transformed AS (
    SELECT
        -- 1. Standardize IDs (Trim and Uppercase)
        UPPER(TRIM(shipment_id)) AS shipment_id,
        UPPER(TRIM(warehouse_id)) AS warehouse_id,
        UPPER(TRIM(store_id)) AS store_id,
        UPPER(TRIM(product_id)) AS product_id,

        -- 2. Clean and Cast Metrics
        CAST(quantity_shipped AS INTEGER) AS quantity_shipped,
        TRIM(carrier) AS carrier,

        -- 3. Date Handling (Cast early to avoid "date - text" errors)
        CAST(shipment_date AS DATE) AS shipment_date,
        CAST(expected_delivery_date AS DATE) AS expected_delivery_date,
        CAST(actual_delivery_date AS DATE) AS actual_delivery_date,

        -- 4. Derived Business Logic: Delivery Performance
        CAST(
            CAST(actual_delivery_date AS DATE) - CAST(expected_delivery_date AS DATE) 
            AS INTEGER
        ) AS delivery_delay_days,
        
        CASE 
            WHEN actual_delivery_date IS NULL THEN 'IN_TRANSIT'
            WHEN CAST(actual_delivery_date AS DATE) <= CAST(expected_delivery_date AS DATE) THEN 'ON_TIME'
            ELSE 'LATE'
        END AS delivery_status,

        ingestion_timestamp,
        -- 5. Audit Metadata
        _ingested_at AS bronze_ingested_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM transformed