with source as (
    SELECT * FROM {{ source('raw', 'warehouses') }}
),

renamed as (
    SELECT
        warehouse_id,
        city,
        state,
        ingestion_timestamp,
        current_timestamp as _ingested_at,
        'raw.warehouses' as _source_file_path
    FROM source
)

SELECT * FROM renamed