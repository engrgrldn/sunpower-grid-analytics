{{
    config(
        materialized='table',
        tags=['cpuc', 'compliance', 'regulatory']
    )
}}

/*
grid_compliance_violations.sql
==============================
Data mart: CPUC Rule 21 & IEEE 1547-2018 grid code compliance

Business Purpose:
- Track voltage/frequency violations for CPUC monthly reporting
- Identify systems requiring corrective action
- Support regulatory compliance audits

Regulatory Standards:
- CPUC Rule 21: Voltage ±8% (220.8V - 259.2V for 240V nominal)
- IEEE 1547-2018: Frequency ±0.3Hz (59.7Hz - 60.3Hz for 60Hz nominal)
- IEEE 1547-2018: Power factor >0.95 at full output

Equivalent at TSCnet: ENTSO-E grid code compliance monitoring
*/

with telemetry as (
    
    select * from {{ ref('stg_telemetry') }}

),

voltage_compliance as (
    
    select
        system_id,
        date,
        timestamp,
        voltage_v,
        
        -- CPUC Rule 21 voltage limits
        {{ var('voltage_nominal_v') }} as voltage_nominal_v,
        {{ var('voltage_nominal_v') }} * (1 - {{ var('voltage_tolerance_pct') }} / 100.0) as voltage_min_v,
        {{ var('voltage_nominal_v') }} * (1 + {{ var('voltage_tolerance_pct') }} / 100.0) as voltage_max_v,
        
        case
            when voltage_v < {{ var('voltage_nominal_v') }} * (1 - {{ var('voltage_tolerance_pct') }} / 100.0)
            then 'VOLTAGE_LOW'
            when voltage_v > {{ var('voltage_nominal_v') }} * (1 + {{ var('voltage_tolerance_pct') }} / 100.0)
            then 'VOLTAGE_HIGH'
            else null
        end as violation_type,
        
        abs(voltage_v - {{ var('voltage_nominal_v') }}) / {{ var('voltage_nominal_v') }} * 100 as deviation_pct,
        
        case
            when abs(voltage_v - {{ var('voltage_nominal_v') }}) / {{ var('voltage_nominal_v') }} > 0.10
            then 'SEVERE'
            when abs(voltage_v - {{ var('voltage_nominal_v') }}) / {{ var('voltage_nominal_v') }} > 0.08
            then 'MODERATE'
            else null
        end as severity
        
    from telemetry
    where voltage_v is not null

),

frequency_compliance as (
    
    select
        system_id,
        date,
        timestamp,
        frequency_hz,
        
        -- IEEE 1547-2018 frequency limits
        {{ var('frequency_nominal_hz') }} as frequency_nominal_hz,
        {{ var('frequency_nominal_hz') }} - {{ var('frequency_tolerance_hz') }} as frequency_min_hz,
        {{ var('frequency_nominal_hz') }} + {{ var('frequency_tolerance_hz') }} as frequency_max_hz,
        
        case
            when frequency_hz < {{ var('frequency_nominal_hz') }} - {{ var('frequency_tolerance_hz') }}
            then 'FREQUENCY_LOW'
            when frequency_hz > {{ var('frequency_nominal_hz') }} + {{ var('frequency_tolerance_hz') }}
            then 'FREQUENCY_HIGH'
            else null
        end as violation_type,
        
        abs(frequency_hz - {{ var('frequency_nominal_hz') }}) as deviation_hz,
        
        'CRITICAL' as severity  -- Frequency deviations always critical
        
    from telemetry
    where frequency_hz is not null

),

power_factor_compliance as (
    
    select
        system_id,
        date,
        timestamp,
        power_factor,
        
        case
            when power_factor < {{ var('power_factor_min') }}
            then 'POWER_FACTOR_LOW'
            else null
        end as violation_type,
        
        {{ var('power_factor_min') }} - power_factor as deviation,
        
        'MODERATE' as severity
        
    from telemetry
    where power_factor is not null
      and solar_production_kw > 1.0  -- Only check PF at significant output

),

all_violations as (
    
    -- Voltage violations
    select
        system_id,
        date,
        timestamp,
        violation_type,
        voltage_v as measured_value,
        voltage_min_v as limit_min,
        voltage_max_v as limit_max,
        deviation_pct as deviation,
        severity,
        'CPUC Rule 21 Section 3.2.1' as regulatory_reference
    from voltage_compliance
    where violation_type is not null
    
    union all
    
    -- Frequency violations
    select
        system_id,
        date,
        timestamp,
        violation_type,
        frequency_hz as measured_value,
        frequency_min_hz as limit_min,
        frequency_max_hz as limit_max,
        deviation_hz as deviation,
        severity,
        'IEEE 1547-2018 Table 7' as regulatory_reference
    from frequency_compliance
    where violation_type is not null
    
    union all
    
    -- Power factor violations
    select
        system_id,
        date,
        timestamp,
        violation_type,
        power_factor as measured_value,
        {{ var('power_factor_min') }} as limit_min,
        null as limit_max,
        deviation,
        severity,
        'IEEE 1547-2018 Section 5.2' as regulatory_reference
    from power_factor_compliance
    where violation_type is not null

)

select
    system_id,
    date,
    timestamp,
    violation_type,
    measured_value,
    limit_min,
    limit_max,
    deviation,
    severity,
    regulatory_reference,
    
    -- Action guidance
    case
        when violation_type like 'VOLTAGE%' then 'Report to utility if sustained >5min per IEEE 1547'
        when violation_type like 'FREQUENCY%' then 'Immediate disconnection if >0.5Hz per CAISO tariff'
        when violation_type = 'POWER_FACTOR_LOW' then 'Enable reactive power support per CPUC Rule 21'
    end as action_required,
    
    current_timestamp() as analysis_timestamp

from all_violations

/*
OUTPUT SCHEMA:
==============
system_id            STRING    - System identifier (e.g., CA-SV-0001)
date                 DATE      - Calendar date of violation
timestamp            TIMESTAMP - Exact time of violation
violation_type       STRING    - VOLTAGE_LOW | VOLTAGE_HIGH | FREQUENCY_LOW | FREQUENCY_HIGH | POWER_FACTOR_LOW
measured_value       DOUBLE    - Actual measured value
limit_min            DOUBLE    - Lower compliance limit
limit_max            DOUBLE    - Upper compliance limit (null for PF)
deviation            DOUBLE    - Magnitude of deviation
severity             STRING    - MODERATE | SEVERE | CRITICAL
regulatory_reference STRING    - CPUC/IEEE standard reference
action_required      STRING    - Remediation guidance
analysis_timestamp   TIMESTAMP - When this analysis ran

USAGE:
======
-- Count violations by system
SELECT system_id, COUNT(*) as violation_count
FROM {{ this }}
GROUP BY system_id
ORDER BY violation_count DESC;

-- CPUC monthly report: systems with >10 violations
SELECT system_id, date, violation_type, COUNT(*) as count
FROM {{ this }}
WHERE date >= DATE_TRUNC('month', CURRENT_DATE)
GROUP BY system_id, date, violation_type
HAVING COUNT(*) > 10;
*/
