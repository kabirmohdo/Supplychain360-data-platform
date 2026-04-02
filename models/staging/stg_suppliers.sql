with source as (
    SELECT * FROM {{ source('raw', 'suppliers') }}
),

renamed as (
    SELECT
        supplier_id,
        supplier_name,
        category,
        country,
        ingestion_timestamp,
        current_timestamp as _ingested_at,
        'raw.suppliers' as _source_file_path
    FROM source
)

SELECT * FROM renamed