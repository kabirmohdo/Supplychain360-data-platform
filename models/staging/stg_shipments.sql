with source as (
    SELECT * FROM {{ source('raw', 'shipments') }}
),

renamed as (
    SELECT
        shipment_id,
        warehouse_id,
        store_id,
        product_id,
        quantity_shipped,
        shipment_date,
        expected_delivery_date,
        actual_delivery_date,
        carrier,
        file_date,
        ingestion_timestamp,
        current_timestamp as _ingested_at,
        'raw.shipments' as _source_file_path
    FROM source
)

SELECT * FROM renamed