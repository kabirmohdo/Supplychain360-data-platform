{{
    config(
        materialized='incremental',
        unique_key='inventory_pk'
    )
}}

WITH bronze_inventories AS (
    SELECT * FROM {{ ref('bronze_inventories') }}

    {% if is_incremental() %}
    WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY warehouse_id, product_id, snapshot_date 
            ORDER BY ingestion_timestamp DESC
        ) AS row_num
    FROM bronze_inventories
),

transformed AS (
    SELECT
        -- 1. Standardize IDs using your Macro
        {{ generate_surrogate_key(['warehouse_id', 'product_id', 'snapshot_date']) }} AS inventory_pk,
        UPPER(TRIM(warehouse_id)) AS warehouse_id,
        UPPER(TRIM(product_id)) AS product_id,

        -- 2. Clean and Cast Metrics
        CAST(quantity_available AS INTEGER) AS quantity_available,
        CAST(reorder_threshold AS INTEGER) AS reorder_threshold,

        -- 3. Date Handling
        CAST(snapshot_date AS DATE) AS snapshot_date,

        -- 4. Derived Business Logic: Stock Status
        CASE 
            WHEN CAST(quantity_available AS INTEGER) = 0 THEN 'OUT_OF_STOCK'
            WHEN CAST(quantity_available AS INTEGER) <= CAST(reorder_threshold AS INTEGER) THEN 'LOW_STOCK'
            ELSE 'HEALTHY'
        END AS stock_status,

        ingestion_timestamp,
        -- 5. Audit Metadata
        _ingested_at AS bronze_ingested_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM transformed