with source as (
    SELECT * FROM {{ source('raw', 'sales_details') }}
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
        current_timestamp as _ingested_at,
        'raw.sales' as _source_file_path
    FROM source
)

SELECT * FROM renamed