with source as (
    SELECT * FROM {{ source('raw', 'stores_sales') }}
),

renamed as (
    SELECT
        transaction_id,
        store_id,
        product_id,
        quantity_sold,
        unit_price,
        discount_pct,
        sale_amount,
        transaction_timestamp,
        ingestion_timestamp,
        file_date,
        current_timestamp as _ingested_at,
        'raw.stores_sales' as _source_file_path
    FROM source
)

SELECT * FROM renamed