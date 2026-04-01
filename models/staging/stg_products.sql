with source as (
    SELECT * FROM {{ source('raw', 'products') }}
),

renamed as (
    SELECT
        product_id,
        product_name,
        category,
        brand,
        supplier_id,
        unit_price,
        ingestion_timestamp,
        current_timestamp as _ingested_at,
        'raw.products' as _source_file_path
    FROM source
)

SELECT * FROM renamed