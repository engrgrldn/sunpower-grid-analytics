{{
    config(
        materialized='view',
        tags=['staging', 'telemetry']
    )
}}

/*
stg_telemetry.sql
=================
Staging model: Clean and standardize raw telemetry data

Input: Raw Parquet files from PySpark ingestion
Output: Cleaned, typed, validated telemetry records

Data Quality Rules:
- Remove nulls in critical fields (system_id, timestamp)
- Cast types correctly
- Add data quality flags
- Deduplicate on (system_id, timestamp)

Equivalent to AVEVA PI System data cleaning layer at TSCnet.
*/

with source_data as (
    
    select
        system_id,
        timestamp,
        solar_production_kw,
        consumption_kw,
        grid_import_kw,
        grid_export_kw,
        battery_soc_pct,
        battery_power_kw,
        voltage_v,
        frequency_hz,
        inverter_temp_c,
        inverter_efficiency_pct,
        power_factor,
        ingestion_timestamp
        
    from {{ source('raw', 'telemetry') }}
    
    where system_id is not null
      and timestamp is not null
      and timestamp >= '{{ var("start_date") }}'

),

deduplicated as (
    
    select * from (
        select
            *,
            row_number() over (
                partition by system_id, timestamp 
                order by ingestion_timestamp desc
            ) as row_num
        from source_data
    )
    where row_num = 1

),

with_data_quality_flags as (
    
    select
        -- Identifiers
        system_id,
        timestamp,
        date(timestamp) as date,
        extract(hour from timestamp) as hour_of_day,
        
        -- Power measurements
        solar_production_kw,
        consumption_kw,
        grid_import_kw,
        grid_export_kw,
        
        -- Battery
        battery_soc_pct,
        battery_power_kw,
        
        -- Power quality
        voltage_v,
        frequency_hz,
        power_factor,
        
        -- Inverter
        inverter_temp_c,
        inverter_efficiency_pct,
        
        -- Derived metrics
        case 
            when solar_production_kw > 0 
            then (solar_production_kw - grid_export_kw) / solar_production_kw * 100
            else null
        end as self_consumption_rate_pct,
        
        -- Data quality flags
        case when voltage_v is null then 1 else 0 end as voltage_missing_flag,
        case when frequency_hz is null then 1 else 0 end as frequency_missing_flag,
        case when solar_production_kw < 0 then 1 else 0 end as solar_negative_flag,
        
        -- Metadata
        ingestion_timestamp
        
    from deduplicated

)

select * from with_data_quality_flags
