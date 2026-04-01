with source as (
    SELECT * FROM {{ source('raw', 'inventory') }}
),

renamed as (
    SELECT
        warehouse_id,
        product_id,
        quantity_available,
        reorder_threshold,
        snapshot_date,
        ingestion_timestamp,
        current_timestamp as _ingested_at,
        'raw.inventories' as _source_file_path
    FROM source
)

SELECT * FROM renamed