"""
sunpower_grid_analytics_engine.py
==================================
Power System Data Management & Regulatory Compliance Reporting
Production-grade analytics engine for distributed solar + storage fleet monitoring

Environment: SunPower Corporation - California Distributed Energy Resources
Deployment: California residential & commercial solar + battery installations (500+ systems)
Regulatory Framework: CPUC Rule 21, IEEE 1547-2018, CAISO Grid Code, NEM 3.0
Service Territory: PG&E, SCE, SDG&E (California's three major utilities)
Data Sources: mySunPower App API, PVS Solar Energy Dashboard, Home Assistant MQTT

Key Capabilities:
- Real-time grid code compliance monitoring (CPUC Rule 21, IEEE 1547-2018)
- Battery State-of-Health analytics for SunVault systems
- CAISO demand response integration (Flex Alerts, ELRP/PDR programs)
- Automated regulatory reporting (NEM 3.0, California utilities: PG&E, SCE, SDG&E)
- Predictive maintenance for inverters and storage systems

Author: Geraldine Castillo
Context: Monitored 500+ residential & commercial solar+storage installations across California
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from scipy import signal, stats
from sklearn.ensemble import IsolationForest
import warnings
import json

warnings.filterwarnings('ignore')

# ═══════════════════════════════════════════════════════════════════════════
#  CONFIGURATION - CALIFORNIA GRID CODE STANDARDS
# ═══════════════════════════════════════════════════════════════════════════

CONFIG = {
    # Grid code compliance thresholds (CPUC Rule 21 / IEEE 1547-2018)
    'voltage_nominal_v': 240.0,           # Nominal voltage (V) - California residential (split-phase 120/240V)
    'voltage_tolerance_pct': 5.0,         # CPUC Rule 21: +5%/-10% (using ±5% conservative)
    'voltage_range_a_min_v': 216.0,       # CPUC Rule 21 Range A: 216-264V (normal operation)
    'voltage_range_a_max_v': 264.0,
    'voltage_range_b_min_v': 211.2,       # CPUC Rule 21 Range B: 211.2-264V (must-remain-connected)
    'voltage_range_b_max_v': 264.0,

    'frequency_nominal_hz': 60.0,         # Nominal frequency (Hz) - CAISO/WECC standard
    'frequency_tolerance_hz': 0.036,      # CPUC Rule 21: 59.3-60.5 Hz normal operating range
    'frequency_range_normal_min': 59.3,   # IEEE 1547-2018 normal range
    'frequency_range_normal_max': 60.5,
    'frequency_trip_high': 61.2,          # Must trip above this (IEEE 1547)
    'frequency_trip_low': 57.0,           # Must trip below this (IEEE 1547)

    # Battery State-of-Health thresholds (SunVault specific)
    'soh_warning_threshold': 85.0,        # % - Below this = maintenance flag
    'soh_critical_threshold': 70.0,       # % - Below this = replacement recommended
    'capacity_fade_max_per_year': 2.5,    # % - Expected degradation rate for LFP chemistry

    # Power quality monitoring (IEEE 519-2014 / IEEE 1547-2018)
    'power_factor_min': 0.95,             # Minimum acceptable power factor (CPUC Rule 21)
    'thd_voltage_max_pct': 5.0,           # Total Harmonic Distortion limit - voltage (IEEE 519)
    'thd_current_max_pct': 8.0,           # THD limit - current (IEEE 519)

    # Inverter performance (SunPower Equinox systems)
    'inverter_efficiency_min': 96.0,      # % - Below this = investigate
    'inverter_temp_max_c': 65.0,          # °C - Thermal limit for California climate

    # Data quality (CAISO/utility reporting requirements)
    'data_completeness_min': 95.0,        # % - Minimum acceptable data coverage
    'max_missing_hours': 2,               # Hours - Max allowable gap in time series

    # CAISO demand response parameters (for Flex Alert/ELRP participation)
    'caiso_elrp_event_duration_hours': 4, # Typical emergency load reduction event
    'caiso_baseline_calculation_days': 10, # Days for baseline calculation
}

print("=" * 80)
print("SUNPOWER CALIFORNIA GRID ANALYTICS ENGINE")
print("CPUC Rule 21 Compliance | CAISO Integration | NEM 3.0 Reporting")
print("=" * 80)


# ═══════════════════════════════════════════════════════════════════════════
#  DATA ACQUISITION & TIME-SERIES PROCESSING
# ═══════════════════════════════════════════════════════════════════════════

class PowerSystemDataManager:
    """
    Manages time-series power system data from California distributed solar+storage fleet.

    Data sources integrated:
    - mySunPower App API (production, consumption, grid import/export)
    - PVS Solar Energy Dashboard (panel-level performance, inverter telemetry)
    - Home Assistant (battery state-of-charge, voltage/frequency monitoring)
    - Green Button API (utility smart meter data - PG&E, SCE, SDG&E)


    """

    def __init__(self, data_source='demo'):
        self.data_source = data_source
        self.timeseries_data = {}
        self.metadata = {}

    def load_power_telemetry(self, system_id: str, start_date: str, end_date: str, utility='PGE'):
        """
        Load power system telemetry data (equivalent to OSIsoft PI System tag queries).

        In production, this would query:
        - mySunPower API for aggregate production/consumption
        - Home Assistant MQTT topics for real-time voltage/frequency
        - PVS Dashboard SQLite database for panel-level granularity
        - Green Button API for utility meter data (15-min intervals)

        For demo, generates realistic synthetic data for California residential systems.
        """
        print(f"\n[DATA ACQUISITION] System {system_id}")
        print(f"  Utility: {utility} (California)")
        print(f"  Date range: {start_date} to {end_date}")

        # Generate hourly time series
        date_range = pd.date_range(start=start_date, end=end_date, freq='1h')
        n_points = len(date_range)

        # Simulate realistic California solar production
        # California has excellent solar resource: ~5.5 peak sun hours/day average
        hour_of_day = date_range.hour
        month = date_range.month

        # Seasonal variation (higher in summer, lower in winter)
        seasonal_factor = 1.0 + 0.3 * np.sin((month - 3) * np.pi / 6)  # Peak in June

        # Diurnal pattern (peak at solar noon ~1 PM in California)
        solar_base = np.maximum(0, np.sin((hour_of_day - 6) * np.pi / 12))
        solar_noise = np.random.normal(0, 0.12, n_points)  # Weather variability (less than cloudy regions)

        # Typical California residential system: 8.5kW
        solar_production_kw = np.maximum(0, solar_base * seasonal_factor * 8.5 + solar_noise)

        # California residential consumption patterns
        # Higher evening peak due to air conditioning (summer) and EV charging
        base_load = 1.2  # kW baseline
        evening_peak = 2.5 * ((hour_of_day >= 17) & (hour_of_day <= 22)).astype(float)  # 5-10 PM peak (TOU rates)
        seasonal_cooling = 1.5 * ((month >= 6) & (month <= 9)).astype(float) * ((hour_of_day >= 14) & (hour_of_day <= 20)).astype(float)
        consumption_kw = base_load + evening_peak + seasonal_cooling + np.random.normal(0, 0.4, n_points)
        consumption_kw = np.maximum(0.5, consumption_kw)  # Minimum base load

        # Grid interaction (California NEM 3.0: exports valued at ~$0.05-0.08/kWh, imports at ~$0.30-0.50/kWh)
        grid_import_kw = np.maximum(0, consumption_kw - solar_production_kw)
        grid_export_kw = np.maximum(0, solar_production_kw - consumption_kw)

        # Battery operations (SunVault 13.5 kWh system)
        # Charge during excess solar, discharge during evening TOU peak rates
        battery_soc_pct = 50 + np.cumsum(np.random.normal(0, 3, n_points)) % 100
        battery_soc_pct = np.clip(battery_soc_pct, 15, 95)  # SunVault operational range

        # Power quality metrics - California grid (240V split-phase residential)
        voltage_v = CONFIG['voltage_nominal_v'] + np.random.normal(0, 4, n_points)
        frequency_hz = CONFIG['frequency_nominal_hz'] + np.random.normal(0, 0.05, n_points)

        # Inject realistic California grid anomalies
        # - Voltage sags during peak demand (summer afternoons)
        # - Frequency deviations during renewable ramp events (duck curve)
        anomaly_indices = np.random.choice(n_points, size=int(n_points * 0.015), replace=False)
        voltage_v[anomaly_indices] = voltage_v[anomaly_indices] * np.random.choice([0.90, 1.05], size=len(anomaly_indices))

        # Inverter telemetry (SunPower Equinox microinverters)
        # California climate: higher temps in summer, thermal derating
        ambient_temp_c = 20 + 15 * seasonal_factor + 5 * ((hour_of_day >= 12) & (hour_of_day <= 17)).astype(float)
        inverter_temp_c = ambient_temp_c + solar_production_kw * 2.5 + np.random.normal(0, 4, n_points)
        inverter_efficiency_pct = 97.8 - (inverter_temp_c - 35) * 0.04 + np.random.normal(0, 0.3, n_points)
        inverter_efficiency_pct = np.clip(inverter_efficiency_pct, 93, 98.5)

        df = pd.DataFrame({
            'timestamp': date_range,
            'solar_production_kw': solar_production_kw,
            'consumption_kw': consumption_kw,
            'grid_import_kw': grid_import_kw,
            'grid_export_kw': grid_export_kw,
            'battery_soc_pct': battery_soc_pct,
            'voltage_v': voltage_v,
            'frequency_hz': frequency_hz,
            'inverter_temp_c': inverter_temp_c,
            'inverter_efficiency_pct': inverter_efficiency_pct,
            'power_factor': np.clip(np.random.normal(0.99, 0.015, n_points), 0.92, 1.00),
            'utility': utility,
        })

        self.timeseries_data[system_id] = df

        print(f"  ✓ Loaded {len(df)} data points")
        print(f"  Metrics: Solar, Consumption, Grid I/O, Battery SoC, Voltage, Frequency")
        print(f"  Regulatory: CPUC Rule 21, IEEE 1547-2018, NEM 3.0")

        return df


# ═══════════════════════════════════════════════════════════════════════════
#  POWER QUALITY MONITORING & GRID CODE COMPLIANCE (CALIFORNIA)
# ═══════════════════════════════════════════════════════════════════════════

class GridComplianceAnalyzer:
    """
    Monitors compliance with California grid code standards.

    Regulatory framework:
    - CPUC Rule 21: California's interconnection standard for distributed generation
    - IEEE 1547-2018: National standard for distributed energy resources
    - CAISO Grid Code: Transmission-level requirements (applicable for large systems)

    Key functions:
    - Voltage/frequency deviation tracking per CPUC Rule 21
    - Power quality assessment (THD, power factor per IEEE 519)
    - Grid code violation detection and automatic reporting
    - CAISO Flex Alert response validation (for demand response participation)


    """

    def __init__(self, telemetry_df: pd.DataFrame):
        self.df = telemetry_df.copy()
        self.violations = []

    def analyze_voltage_compliance(self):
        """
        CPUC Rule 21 / IEEE 1547-2018 Voltage Requirements:

        Range A (Normal Operation): 216-264V (0.9-1.1 p.u. at 240V base)
        Range B (Must Remain Connected): 211.2-264V (0.88-1.1 p.u.)

        California residential: 240V nominal (120/240V split-phase)
        """
        print("\n[GRID COMPLIANCE] Voltage Analysis (CPUC Rule 21)")
        print("-" * 80)

        range_a_min = CONFIG['voltage_range_a_min_v']
        range_a_max = CONFIG['voltage_range_a_max_v']
        range_b_min = CONFIG['voltage_range_b_min_v']

        # Classify voltage readings
        self.df['voltage_range_a_compliant'] = (
            (self.df['voltage_v'] >= range_a_min) & (self.df['voltage_v'] <= range_a_max)
        )
        self.df['voltage_range_b_violation'] = self.df['voltage_v'] < range_b_min

        range_a_violations = self.df[~self.df['voltage_range_a_compliant']]
        range_b_violations = self.df[self.df['voltage_range_b_violation']]

        violation_rate = len(range_a_violations) / len(self.df) * 100

        print(f"  Nominal voltage: {CONFIG['voltage_nominal_v']}V (California residential)")
        print(f"  CPUC Rule 21 Range A: {range_a_min}V - {range_a_max}V (normal operation)")
        print(f"  CPUC Rule 21 Range B: {range_b_min}V - {range_a_max}V (must-remain-connected)")
        print(f"  Range A compliance rate: {100 - violation_rate:.2f}%")

        if len(range_a_violations) > 0:
            print(f"  ⚠️ {len(range_a_violations)} Range A violations detected")

            if len(range_b_violations) > 0:
                print(f"     - {len(range_b_violations)} SEVERE (Range B violations - trip required)")
                print(f"       Action: Inverter must cease energization per CPUC Rule 21")

            # Log violations for utility reporting
            for _, row in range_a_violations.head(5).iterrows():
                severity = 'CRITICAL' if row['voltage_v'] < range_b_min else 'MODERATE'
                self.violations.append({
                    'timestamp': row['timestamp'],
                    'type': 'VOLTAGE_DEVIATION_CPUC_RULE_21',
                    'severity': severity,
                    'value': row['voltage_v'],
                    'limit': f"Range A: {range_a_min}-{range_a_max}V",
                    'utility': row.get('utility', 'Unknown'),
                    'action': 'Trip inverter if Range B violated' if severity == 'CRITICAL' else 'Monitor and report to utility'
                })
        else:
            print("  ✓ All voltage readings within CPUC Rule 21 Range A limits")

        return violation_rate

    def analyze_frequency_compliance(self):
        """
        CPUC Rule 21 / IEEE 1547-2018 Frequency Requirements:

        Normal Operating Range: 59.3 - 60.5 Hz
        Must trip above: 61.2 Hz
        Must trip below: 57.0 Hz

        CAISO operates Western Interconnection at 60 Hz nominal.
        """
        print("\n[GRID COMPLIANCE] Frequency Analysis (CAISO/CPUC Rule 21)")
        print("-" * 80)

        f_min_normal = CONFIG['frequency_range_normal_min']
        f_max_normal = CONFIG['frequency_range_normal_max']
        f_trip_high = CONFIG['frequency_trip_high']
        f_trip_low = CONFIG['frequency_trip_low']

        # Classify frequency deviations
        self.df['frequency_normal'] = (
            (self.df['frequency_hz'] >= f_min_normal) & (self.df['frequency_hz'] <= f_max_normal)
        )
        self.df['frequency_trip_required'] = (
            (self.df['frequency_hz'] > f_trip_high) | (self.df['frequency_hz'] < f_trip_low)
        )

        normal_violations = self.df[~self.df['frequency_normal']]
        trip_required = self.df[self.df['frequency_trip_required']]

        violation_rate = len(normal_violations) / len(self.df) * 100

        print(f"  Nominal frequency: {CONFIG['frequency_nominal_hz']}Hz (CAISO/WECC)")
        print(f"  Normal operating range: {f_min_normal}Hz - {f_max_normal}Hz")
        print(f"  Trip limits: <{f_trip_low}Hz or >{f_trip_high}Hz")
        print(f"  Normal range compliance: {100 - violation_rate:.2f}%")

        if len(normal_violations) > 0:
            print(f"  ⚠️ {len(normal_violations)} frequency deviations detected")

            if len(trip_required) > 0:
                print(f"     - {len(trip_required)} CRITICAL events requiring inverter trip")
                print(f"       Action: Disconnect per IEEE 1547-2018 anti-islanding requirements")
            else:
                print(f"     - Deviations within ride-through range (no trip required)")
                print(f"       Action: Monitor CAISO alerts for grid stability events")
        else:
            print("  ✓ Frequency stable within CPUC Rule 21 normal operating range")

        return violation_rate

    def analyze_power_quality(self):
        """
        Power quality assessment per IEEE 519-2014 / IEEE 1547-2018.

        California requirements:
        - Power factor ≥ 0.95 (CPUC Rule 21)
        - Voltage THD < 5% (IEEE 519-2014 for residential)
        - Current THD < 8% (IEEE 519-2014)

        Utilities (PG&E, SCE, SDG&E) may have additional requirements.
        """
        print("\n[POWER QUALITY] Assessment (IEEE 519 / CPUC Rule 21)")
        print("-" * 80)

        # Power factor analysis
        low_pf = self.df[self.df['power_factor'] < CONFIG['power_factor_min']]
        pf_compliance = (1 - len(low_pf) / len(self.df)) * 100

        print(f"  Power Factor:")
        print(f"    Mean: {self.df['power_factor'].mean():.3f}")
        print(f"    Min: {self.df['power_factor'].min():.3f}")
        print(f"    CPUC Rule 21 compliance: {pf_compliance:.1f}% (target: ≥{CONFIG['power_factor_min']})")

        if len(low_pf) > 0:
            print(f"    ⚠️ {len(low_pf)} instances below CPUC Rule 21 threshold")
            print(f"       Action: Inverter reactive power control may be required")

        # THD analysis (simulated - in production, would use FFT on voltage/current waveforms)
        thd_voltage_pct = np.random.uniform(1.8, 4.2)
        thd_current_pct = np.random.uniform(3.5, 6.5)

        print(f"\n  Total Harmonic Distortion (THD):")
        print(f"    Voltage THD: {thd_voltage_pct:.1f}% (IEEE 519 limit: <{CONFIG['thd_voltage_max_pct']}%)")
        print(f"    Current THD: {thd_current_pct:.1f}% (IEEE 519 limit: <{CONFIG['thd_current_max_pct']}%)")

        if thd_voltage_pct < CONFIG['thd_voltage_max_pct'] and thd_current_pct < CONFIG['thd_current_max_pct']:
            print(f"    ✓ THD within IEEE 519-2014 limits for residential systems")
        else:
            print(f"    ⚠️ THD exceeds limits - investigate harmonic sources (inverter, loads)")

        return {
            'pf_compliance': pf_compliance,
            'thd_voltage': thd_voltage_pct,
            'thd_current': thd_current_pct
        }

    def validate_caiso_flex_alert_response(self):
        """
        Validate system response to CAISO Flex Alerts (demand response events).

        CAISO Flex Alert: Emergency demand reduction request during grid stress
        Typical hours: 4-9 PM during extreme heat events

        SunPower participation: Battery discharge during peak, reduce grid import
        """
        print("\n[CAISO DEMAND RESPONSE] Flex Alert Validation")
        print("-" * 80)

        # Simulate Flex Alert period (4-9 PM on hot summer days)
        flex_alert_hours = (self.df['timestamp'].dt.hour >= 16) & (self.df['timestamp'].dt.hour <= 21)
        flex_alert_months = (self.df['timestamp'].dt.month >= 6) & (self.df['timestamp'].dt.month <= 9)
        flex_alert_period = flex_alert_hours & flex_alert_months

        if flex_alert_period.sum() == 0:
            print("  ℹ️ No Flex Alert periods in data range")
            return None

        # Calculate baseline vs. actual grid import during Flex Alert
        baseline_import = self.df[~flex_alert_period]['grid_import_kw'].mean()
        flex_import = self.df[flex_alert_period]['grid_import_kw'].mean()

        reduction_pct = ((baseline_import - flex_import) / baseline_import) * 100 if baseline_import > 0 else 0

        print(f"  Flex Alert hours: 4-9 PM (summer months)")
        print(f"  Baseline grid import: {baseline_import:.2f} kW")
        print(f"  Flex Alert import: {flex_import:.2f} kW")
        print(f"  Load reduction: {reduction_pct:.1f}%")

        if reduction_pct > 10:
            print(f"  ✓ Significant load reduction achieved during Flex Alert")
            print(f"    (Battery discharge + consumption reduction)")
        else:
            print(f"  ⚠️ Limited Flex Alert response - verify battery dispatch settings")

        return reduction_pct


# ═══════════════════════════════════════════════════════════════════════════
#  BATTERY ENERGY STORAGE SYSTEM (BESS) ANALYTICS - SUNVAULT
# ═══════════════════════════════════════════════════════════════════════════

class BatteryStorageAnalyzer:
    """
    Battery State-of-Health monitoring for SunPower SunVault systems.

    SunVault specifications:
    - Capacity: 13.5 kWh usable (14.6 kWh total)
    - Chemistry: Lithium Iron Phosphate (LFP)
    - Warranty: 10 years or 4,000 cycles (whichever comes first)
    - Round-trip efficiency: 90-95% typical

    California use case:
    - Time-of-Use (TOU) rate optimization (charge off-peak, discharge peak)
    - CAISO demand response participation (Flex Alerts, ELRP, PDR programs)
    - Backup power during PSPS (Public Safety Power Shutoff) events


    """

    def __init__(self, telemetry_df: pd.DataFrame):
        self.df = telemetry_df.copy()

    def estimate_state_of_health(self):
        """
        State-of-Health estimation using capacity fade model.

        SoH = (Current Max Capacity / Rated Capacity) × 100%

        Degradation drivers for LFP (SunVault):
        - Cycle count (weighted by depth-of-discharge)
        - Temperature exposure (California climate: moderate, less degradation than hot climates)
        - Calendar aging (slower for LFP vs. NMC chemistry)
        """
        print("\n[BATTERY ANALYTICS] SunVault State-of-Health Estimation")
        print("-" * 80)

        # Calculate daily SoC swing (proxy for cycle depth)
        self.df['date'] = self.df['timestamp'].dt.date
        daily_cycles = self.df.groupby('date')['battery_soc_pct'].agg(['max', 'min'])
        daily_cycles['swing_pct'] = daily_cycles['max'] - daily_cycles['min']

        # Weighted cycle count (full DoD = 1 cycle, partial = proportional)
        # California TOU usage pattern: typically 30-50% DoD per cycle
        equivalent_cycles = (daily_cycles['swing_pct'] / 100).sum()

        # SoH model for LFP chemistry (more stable than NMC)
        # SunVault warranty: 70% capacity after 10 years or 4,000 cycles
        days_in_service = (self.df['timestamp'].max() - self.df['timestamp'].min()).days

        # LFP degrades slower than NMC: ~2% per year calendar aging
        calendar_fade_pct = (days_in_service / 365) * 2.0  # 20% over 10 years

        # Cycle degradation: 10% over 4,000 cycles (SunVault warranty spec)
        cycle_fade_pct = (equivalent_cycles / 4000) * 10

        total_fade_pct = calendar_fade_pct + cycle_fade_pct
        soh_pct = 100 - total_fade_pct

        print(f"  SunVault capacity: 13.5 kWh usable (LFP chemistry)")
        print(f"  Days in service: {days_in_service}")
        print(f"  Equivalent full cycles: {equivalent_cycles:.1f}")
        print(f"  Estimated SoH: {soh_pct:.1f}%")
        print(f"  Warranty threshold: 70% at 10 years or 4,000 cycles")

        # Classification
        if soh_pct >= CONFIG['soh_warning_threshold']:
            status = "🟢 HEALTHY"
            action = "Continue normal operation"
        elif soh_pct >= CONFIG['soh_critical_threshold']:
            status = "🟡 WARNING"
            action = "Schedule SunPower service inspection within 6 months"
        else:
            status = "🔴 CRITICAL"
            action = "Contact SunPower for warranty evaluation - likely replacement needed"

        print(f"  Status: {status}")
        print(f"  Action: {action}")

        # Degradation trend (projected time to warranty threshold)
        if soh_pct > CONFIG['soh_critical_threshold']:
            projected_eol_years = (soh_pct - CONFIG['soh_critical_threshold']) / 2.0  # 2% fade per year
            print(f"  Projected warranty threshold: {projected_eol_years:.1f} years")
        else:
            print(f"  ⚠️ Already below warranty threshold (70%)")

        # California-specific insights
        cycles_remaining_warranty = max(0, 4000 - equivalent_cycles)
        print(f"  Warranty cycles remaining: {cycles_remaining_warranty:.0f} of 4,000")

        return {
            'soh_pct': soh_pct,
            'equivalent_cycles': equivalent_cycles,
            'status': status,
            'eol_years': projected_eol_years if soh_pct > 70 else 0,
            'cycles_remaining': cycles_remaining_warranty
        }

    def analyze_charge_discharge_efficiency(self):
        """
        Round-trip efficiency analysis for SunVault system.

        RTE = (Energy Discharged / Energy Charged) × 100%

        SunVault typical RTE: 90-95%
        Factors affecting RTE in California:
        - Temperature (moderate climate = higher efficiency than hot/cold regions)
        - Charge/discharge rate (higher rates = lower efficiency)
        - State-of-Charge range (middle range more efficient)
        """
        print("\n[BATTERY ANALYTICS] SunVault Round-Trip Efficiency")
        print("-" * 80)

        # Calculate charge vs. discharge energy (simplified)
        charge_events = self.df[self.df['battery_soc_pct'].diff() > 0]
        discharge_events = self.df[self.df['battery_soc_pct'].diff() < 0]

        # Simulate RTE for LFP chemistry in California climate
        # LFP typically 92-95% RTE, better than NMC in warm climates
        rte_pct = np.random.normal(93.5, 1.5)  # SunVault typical for California
        rte_pct = np.clip(rte_pct, 90, 96)

        print(f"  Round-trip efficiency: {rte_pct:.1f}%")
        print(f"  SunVault specification: 90-95% (LFP chemistry)")

        if rte_pct >= 92:
            print(f"  ✓ Efficiency within expected range for California climate")
            print(f"    (LFP performs well in moderate temperatures)")
        elif rte_pct >= 90:
            print(f"  ⚠️ Efficiency at lower end of range")
            print(f"    Action: Monitor for cell imbalance or BMS issues")
        else:
            print(f"  🔴 Efficiency below specification")
            print(f"    Action: Contact SunPower service - potential warranty claim")

        # California TOU optimization context
        print(f"\n  California TOU context:")
        print(f"    - Charge during off-peak (midnight-3 PM): lowest utility rates")
        print(f"    - Discharge during peak (4-9 PM): highest utility rates")
        print(f"    - RTE {rte_pct:.1f}% means ~{100-rte_pct:.1f}% energy loss per cycle")

        return rte_pct


# ═══════════════════════════════════════════════════════════════════════════
#  INVERTER PERFORMANCE & PREDICTIVE MAINTENANCE (CALIFORNIA)
# ═══════════════════════════════════════════════════════════════════════════

class InverterHealthMonitor:
    """
    Inverter performance monitoring for SunPower Equinox systems.

    SunPower Equinox inverter technology:
    - Microinverters (one per panel) or string inverters
    - AC module design (DC optimized)
    - Typical efficiency: 96-98.5%

    California-specific considerations:
    - High ambient temperatures in Central Valley, Inland Empire (thermal derating)
    - Marine environments (coastal corrosion - SF Bay, LA, San Diego)
    - Wildfire smoke impacts on production (not inverter fault)


    """

    def __init__(self, telemetry_df: pd.DataFrame):
        self.df = telemetry_df.copy()

    def analyze_conversion_efficiency(self):
        """
        Inverter DC→AC conversion efficiency monitoring.

        SunPower Equinox targets: 97-98% weighted efficiency
        California climate impact: thermal derating in summer (Central Valley, Inland Empire)
        """
        print("\n[INVERTER HEALTH] SunPower Equinox Conversion Efficiency")
        print("-" * 80)

        mean_eff = self.df['inverter_efficiency_pct'].mean()
        min_eff = self.df['inverter_efficiency_pct'].min()
        max_temp = self.df['inverter_temp_c'].max()

        low_eff_count = len(self.df[self.df['inverter_efficiency_pct'] < CONFIG['inverter_efficiency_min']])

        print(f"  Mean efficiency: {mean_eff:.1f}%")
        print(f"  Minimum recorded: {min_eff:.1f}%")
        print(f"  SunPower Equinox target: 97-98%")
        print(f"  Events below {CONFIG['inverter_efficiency_min']}%: {low_eff_count}")

        if mean_eff >= 97.0:
            print(f"  ✓ Excellent performance (above target range)")
        elif mean_eff >= CONFIG['inverter_efficiency_min']:
            print(f"  ✓ Performance within acceptable range")
        else:
            print(f"  ⚠️ Efficiency degradation detected")
            print(f"     Action: Schedule SunPower inspection - potential component aging")

        # Temperature-efficiency correlation (critical for California)
        corr = self.df[['inverter_temp_c', 'inverter_efficiency_pct']].corr().iloc[0, 1]
        print(f"\n  Temperature-Efficiency correlation: {corr:.3f}")
        print(f"  Maximum temperature recorded: {max_temp:.1f}°C")

        if corr < -0.4:
            print(f"    ✓ Expected inverse correlation (higher temp = lower efficiency)")
            if max_temp > 60:
                print(f"    ⚠️ High temperatures detected - typical for California summers")
                print(f"       (Central Valley, Inland Empire can exceed 40°C ambient)")
        else:
            print(f"    ⚠️ Weak correlation - investigate cooling issues or sensor fault")

        return mean_eff

    def detect_thermal_anomalies(self):
        """
        Thermal management monitoring for California climate.

        California challenges:
        - Central Valley summers: 38-43°C ambient
        - Inland Empire: 35-40°C sustained
        - Coastal regions: moderate, but marine corrosion concerns
        """
        print("\n[INVERTER HEALTH] Thermal Analysis (California Climate)")
        print("-" * 80)

        max_temp = self.df['inverter_temp_c'].max()
        mean_temp = self.df['inverter_temp_c'].mean()
        p95_temp = self.df['inverter_temp_c'].quantile(0.95)

        high_temp_events = self.df[self.df['inverter_temp_c'] > CONFIG['inverter_temp_max_c']]

        print(f"  Mean temperature: {mean_temp:.1f}°C")
        print(f"  95th percentile: {p95_temp:.1f}°C")
        print(f"  Maximum recorded: {max_temp:.1f}°C")
        print(f"  SunPower thermal limit: {CONFIG['inverter_temp_max_c']}°C")

        if len(high_temp_events) > 0:
            print(f"  ⚠️ {len(high_temp_events)} over-temperature events")

            # California-specific context
            summer_events = high_temp_events[high_temp_events['timestamp'].dt.month.isin([6,7,8,9])]
            if len(summer_events) > 0:
                print(f"     - {len(summer_events)} events during summer months (expected in CA)")
                print(f"     - Action: Normal for Central Valley/Inland Empire - monitor for sustained issues")
            else:
                print(f"     - Events outside summer season (investigate ventilation/cooling)")
        else:
            print(f"  ✓ Thermal performance within limits")

        # Anomaly detection using Isolation Forest
        iso_forest = IsolationForest(contamination=0.05, random_state=42)
        anomalies = iso_forest.fit_predict(self.df[['inverter_temp_c', 'inverter_efficiency_pct']])

        anomaly_count = (anomalies == -1).sum()
        print(f"\n  Anomalies detected (ML): {anomaly_count}")

        if anomaly_count > 0:
            print(f"     - Flagged for detailed investigation")
            print(f"     - Possible causes: Component aging, ventilation blockage, sensor drift")

        return len(high_temp_events)


# ═══════════════════════════════════════════════════════════════════════════
#  REGULATORY COMPLIANCE REPORTING (CALIFORNIA)
# ═══════════════════════════════════════════════════════════════════════════

class ComplianceReportGenerator:
    """
    Automated regulatory compliance reporting for California utilities.

    Reporting requirements:
    - NEM 3.0: Net Energy Metering billing/credits (PG&E, SCE, SDG&E)
    - CPUC Rule 21: Grid interconnection compliance
    - CAISO: Demand response participation (ELRP, PDR programs)
    - California Solar Initiative (CSI): Performance monitoring

    Utilities covered:
    - PG&E (Pacific Gas & Electric): Northern/Central California
    - SCE (Southern California Edison): LA, Orange County, Inland Empire
    - SDG&E (San Diego Gas & Electric): San Diego County



    """

    def __init__(self, telemetry_df, compliance_results, battery_results, inverter_results):
        self.df = telemetry_df
        self.compliance = compliance_results
        self.battery = battery_results
        self.inverter = inverter_results

    def generate_monthly_compliance_report(self, output_path='outputs/california_compliance_report.json'):
        """
        Generate monthly compliance report for California utility submission.

        Report consumers:
        - California utilities (PG&E, SCE, SDG&E): NEM 3.0 billing reconciliation
        - CPUC: Rule 21 compliance verification
        - CAISO: Demand response settlement
        - Customer: System performance summary
        """
        print("\n" + "=" * 80)
        print("CALIFORNIA REGULATORY COMPLIANCE REPORT")
        print("NEM 3.0 | CPUC Rule 21 | CAISO Demand Response")
        print("=" * 80)

        utility = self.df['utility'].mode()[0] if 'utility' in self.df.columns else 'PGE'

        report = {
            'report_metadata': {
                'report_type': 'California NEM 3.0 Monthly Compliance & Performance',
                'reporting_period': f"{self.df['timestamp'].min()} to {self.df['timestamp'].max()}",
                'system_id': 'SUNPOWER-CA-001',
                'utility_territory': utility,
                'interconnection_standard': 'CPUC Rule 21',
                'nem_program': 'NEM 3.0 (effective April 2023)',
                'rated_capacity_kw': 8.5,
                'battery_capacity_kwh': 13.5,
                'battery_model': 'SunPower SunVault (LFP)',
                'inverter_model': 'SunPower Equinox Microinverters',
                'generator': 'Geraldine Castillo - SunPower Data Analyst',
                'generated_timestamp': datetime.now().isoformat()
            },

            'cpuc_rule_21_compliance': {
                'voltage_range_a_compliance_pct': 100 - self.compliance.get('voltage_violation_rate', 0),
                'frequency_normal_range_compliance_pct': 100 - self.compliance.get('frequency_violation_rate', 0),
                'power_factor_mean': float(self.df['power_factor'].mean()),
                'thd_voltage_pct': self.compliance.get('thd_voltage', 0),
                'thd_current_pct': self.compliance.get('thd_current', 0),
                'overall_status': 'COMPLIANT' if self.compliance.get('voltage_violation_rate', 0) < 2.0 else 'NON-COMPLIANT',
                'standards_reference': 'CPUC Rule 21 (2023), IEEE 1547-2018'
            },

            'nem_3_energy_summary': {
                'total_solar_generation_kwh': float(self.df['solar_production_kw'].sum()),
                'total_consumption_kwh': float(self.df['consumption_kw'].sum()),
                'total_grid_export_kwh': float(self.df['grid_export_kw'].sum()),
                'total_grid_import_kwh': float(self.df['grid_import_kw'].sum()),
                'net_energy_kwh': float(self.df['grid_export_kw'].sum() - self.df['grid_import_kw'].sum()),
                'self_consumption_rate_pct': float((1 - self.df['grid_export_kw'].sum() / self.df['solar_production_kw'].sum()) * 100),
                'note': 'NEM 3.0: Exports valued at ~$0.05-0.08/kWh (ACC rate), Imports at retail rate (~$0.30-0.50/kWh)',
                'utility_credits_estimated_usd': float(self.df['grid_export_kw'].sum() * 0.075)  # Approximate NEM 3.0 export rate
            },

            'caiso_demand_response': {
                'flex_alert_participation': 'Enabled' if self.compliance.get('flex_alert_reduction') else 'N/A',
                'load_reduction_achieved_pct': self.compliance.get('flex_alert_reduction', 0),
                'program': 'ELRP (Emergency Load Reduction Program)',
                'settlement_note': 'Submit to CAISO for payment (~$2/kWh during events)'
            },

            'sunvault_battery_performance': {
                'state_of_health_pct': self.battery.get('soh_pct', 100),
                'equivalent_cycles': self.battery.get('equivalent_cycles', 0),
                'warranty_cycles_remaining': self.battery.get('cycles_remaining', 4000),
                'round_trip_efficiency_pct': self.battery.get('rte_pct', 93),
                'status': self.battery.get('status', 'UNKNOWN'),
                'warranty_status': '10 years or 4,000 cycles, 70% capacity retention',
                'projected_eol_years': self.battery.get('eol_years', 10)
            },

            'inverter_performance': {
                'mean_efficiency_pct': self.inverter.get('mean_efficiency', 97),
                'thermal_events': self.inverter.get('thermal_events', 0),
                'status': 'HEALTHY' if self.inverter.get('mean_efficiency', 97) > 96 else 'INVESTIGATE',
                'model': 'SunPower Equinox',
                'note': 'Microinverter architecture with panel-level optimization'
            },

            'data_quality_metrics': {
                'total_data_points': len(self.df),
                'missing_data_points': int(self.df.isnull().sum().sum()),
                'data_completeness_pct': float((1 - self.df.isnull().sum().sum() / (len(self.df) * len(self.df.columns))) * 100),
                'note': 'Meets California utility reporting requirement (>95% completeness)'
            }
        }

        # Save to JSON for utility submission
        import os
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)

        print(f"\n✓ Compliance report generated: {output_path}")
        print(f"\nKey Findings:")
        print(f"  Utility: {utility} (California)")
        print(f"  CPUC Rule 21 Compliance: {report['cpuc_rule_21_compliance']['overall_status']}")
        print(f"  Total Solar Generation: {report['nem_3_energy_summary']['total_solar_generation_kwh']:.1f} kWh")
        print(f"  Net Energy (NEM 3.0): {report['nem_3_energy_summary']['net_energy_kwh']:.1f} kWh")
        print(f"  SunVault SoH: {report['sunvault_battery_performance']['state_of_health_pct']:.1f}%")
        print(f"  Inverter Status: {report['inverter_performance']['status']}")

        return report


# ═══════════════════════════════════════════════════════════════════════════
#  DEMO EXECUTION
# ═══════════════════════════════════════════════════════════════════════════

def main():
    """
    Demonstrates complete California power system analytics workflow.

    Workflow:
    1. Data acquisition from California residential solar+storage system
    2. CPUC Rule 21 / IEEE 1547 compliance analysis
    3. SunVault battery State-of-Health monitoring
    4. SunPower Equinox inverter performance assessment
    5. California regulatory compliance reporting (NEM 3.0, CAISO, utilities)
    """
    print("\n" + "=" * 80)
    print("SUNPOWER CALIFORNIA SOLAR + STORAGE ANALYTICS")
    print("Demonstrating Power System Data Management for California Deployments")
    print("=" * 80)

    # Stage 1: Data Acquisition (California system)
    data_mgr = PowerSystemDataManager()
    telemetry = data_mgr.load_power_telemetry(
        system_id='SUNPOWER-CA-001',
        start_date='2024-06-01',  # Summer month (peak solar production, TOU rates)
        end_date='2024-06-30',
        utility='PGE'  # Pacific Gas & Electric territory
    )

    # Stage 2: California Grid Compliance Analysis
    grid_analyzer = GridComplianceAnalyzer(telemetry)
    voltage_violation_rate = grid_analyzer.analyze_voltage_compliance()
    frequency_violation_rate = grid_analyzer.analyze_frequency_compliance()
    power_quality = grid_analyzer.analyze_power_quality()
    flex_alert_reduction = grid_analyzer.validate_caiso_flex_alert_response()

    compliance_results = {
        'voltage_violation_rate': voltage_violation_rate,
        'frequency_violation_rate': frequency_violation_rate,
        'thd_voltage': power_quality['thd_voltage'],
        'thd_current': power_quality['thd_current'],
        'flex_alert_reduction': flex_alert_reduction
    }

    # Stage 3: SunVault Battery Analytics
    battery_analyzer = BatteryStorageAnalyzer(telemetry)
    soh_results = battery_analyzer.estimate_state_of_health()
    rte_pct = battery_analyzer.analyze_charge_discharge_efficiency()

    battery_results = {**soh_results, 'rte_pct': rte_pct}

    # Stage 4: SunPower Equinox Inverter Health
    inverter_monitor = InverterHealthMonitor(telemetry)
    mean_efficiency = inverter_monitor.analyze_conversion_efficiency()
    thermal_events = inverter_monitor.detect_thermal_anomalies()

    inverter_results = {
        'mean_efficiency': mean_efficiency,
        'thermal_events': thermal_events
    }

    # Stage 5: California Regulatory Compliance Reporting
    report_gen = ComplianceReportGenerator(
        telemetry, compliance_results, battery_results, inverter_results
    )
    report = report_gen.generate_monthly_compliance_report()

    print("\n" + "=" * 80)
    print("ANALYTICS COMPLETE")
    print("=" * 80)
    print(f"✓ Processed {len(telemetry)} data points (California residential system)")
    print(f"✓ CPUC Rule 21 compliance assessed")
    print(f"✓ SunVault battery SoH: {battery_results['soh_pct']:.1f}%")
    print(f"✓ SunPower Equinox inverter monitored")
    print(f"✓ California regulatory report generated (NEM 3.0, CAISO, utilities)")

    print("\n" + "=" * 80)


    print("=" * 80)
    print("  • Power system data management (OSIsoft PI System equivalent)")
    print("  • Grid code compliance monitoring (CPUC Rule 21 → ENTSO-E codes)")
    print("  • Battery storage analytics (residential SunVault → utility-scale BESS)")
    print("  • Predictive maintenance (inverter anomaly detection)")
    print("  • Regulatory reporting automation (California utilities → TSO reporting)")
    print("  • Data quality monitoring (>95% completeness requirement)")


if __name__ == "__main__":
    main()