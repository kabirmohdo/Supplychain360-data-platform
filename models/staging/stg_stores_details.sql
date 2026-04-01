with source as (
    SELECT * FROM {{ source('raw', 'stores_details') }}
),

renamed as (
    SELECT
        store_id,
        store_name,
        city,
        state,
        region,
        store_open_date,
        ingestion_timestamp,
        current_timestamp as _ingested_at,
        'raw.store_locations' as _source_file_path
    FROM source
)

SELECT * FROM renamed