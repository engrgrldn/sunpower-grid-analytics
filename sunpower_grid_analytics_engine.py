"""
Power System Data Management & Reporting Analytics


Author: Geraldine Castillo

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
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

CONFIG = {
    # Grid code compliance thresholds (IEEE 1547-2018 / Philippine grid standards)
    'voltage_nominal_v': 230.0,           # Nominal voltage (V) - Philippines residential
    'voltage_tolerance_pct': 10.0,        # ±10% voltage variation limit

    'frequency_nominal_hz': 60.0,         # Nominal frequency (Hz) - Philippines
    'frequency_tolerance_hz': 0.5,        # ±0.5 Hz deviation limit
    
    # Battery State-of-Health thresholds
    'soh_warning_threshold': 85.0,        # % - Below this = maintenance flag
    'soh_critical_threshold': 70.0,       # % - Below this = replacement recommended
    'capacity_fade_max_per_year': 2.5,    # % - Expected degradation rate
    
    # Power quality monitoring
    'power_factor_min': 0.95,             # Minimum acceptable power factor
    'thd_voltage_max_pct': 5.0,           # Total Harmonic Distortion limit (%)
    'thd_current_max_pct': 8.0,
    
    # Inverter performance
    'inverter_efficiency_min': 96.0,      # % - Below this = investigate
    'inverter_temp_max_c': 65.0,          # °C - Thermal limit
    
    # Data quality
    'data_completeness_min': 95.0,        # % - Minimum acceptable data coverage
    'max_missing_hours': 2,               # Hours - Max allowable gap in time series
}

print("=" * 80)
print("SUNPOWER GRID ANALYTICS ENGINE")
print("Power System Data Management | Battery Storage Analytics | Grid Compliance")
print("=" * 80)


# ═══════════════════════════════════════════════════════════════════════════
#  DATA ACQUISITION & TIME-SERIES PROCESSING
# ═══════════════════════════════════════════════════════════════════════════

class PowerSystemDataManager:
    """
    Manages time-series power system data from distributed solar+storage fleet.
    
    Data sources integrated:
    - mySunPower App API (production, consumption, grid import/export)
    - PVS Solar Energy Dashboard (panel-level performance, inverter telemetry)
    - Home Assistant (battery state-of-charge, voltage/frequency monitoring)

    """
    
    def __init__(self, data_source='demo'):
        self.data_source = data_source
        self.timeseries_data = {}
        self.metadata = {}
        
    def load_power_telemetry(self, system_id: str, start_date: str, end_date: str):
        """
        Load power system telemetry data (equivalent to PI System tag queries).
        
        In production, this would query:
        - mySunPower API for aggregate production/consumption
        - Home Assistant MQTT topics for real-time voltage/frequency
        - PVS Dashboard SQLite database for panel-level granularity
        
        For demo, generates realistic synthetic data.
        """
        print(f"\n[DATA ACQUISITION] System {system_id}")
        print(f"  Date range: {start_date} to {end_date}")
        
        # Generate hourly time series
        date_range = pd.date_range(start=start_date, end=end_date, freq='1h')
        n_points = len(date_range)
        
        # Simulate realistic solar production (diurnal pattern + weather variability)
        hour_of_day = date_range.hour
        solar_base = np.maximum(0, np.sin((hour_of_day - 6) * np.pi / 12))  # Peak at noon
        solar_noise = np.random.normal(0, 0.15, n_points)  # Weather variability
        solar_production_kw = np.maximum(0, solar_base * 8.5 + solar_noise)  # 8.5kW system
        
        # Simulate consumption (base load + random variation)
        consumption_base = 1.5 + 0.8 * (hour_of_day >= 18).astype(float)  # Evening peak
        consumption_kw = consumption_base + np.random.normal(0, 0.3, n_points)
        
        # Grid interaction (import when consumption > production)
        grid_import_kw = np.maximum(0, consumption_kw - solar_production_kw)
        grid_export_kw = np.maximum(0, solar_production_kw - consumption_kw)
        
        # Battery operations (charge during excess solar, discharge during evening)
        battery_soc_pct = 50 + np.cumsum(np.random.normal(0, 2, n_points)) % 100
        battery_soc_pct = np.clip(battery_soc_pct, 10, 95)
        
        # Power quality metrics (voltage/frequency monitoring)
        voltage_v = CONFIG['voltage_nominal_v'] + np.random.normal(0, 5, n_points)
        frequency_hz = CONFIG['frequency_nominal_hz'] + np.random.normal(0, 0.1, n_points)
        
        # Inject some anomalies for detection demo
        anomaly_indices = np.random.choice(n_points, size=int(n_points * 0.02), replace=False)
        voltage_v[anomaly_indices] = voltage_v[anomaly_indices] * np.random.choice([0.85, 1.15], size=len(anomaly_indices))
        
        # Inverter telemetry
        inverter_temp_c = 35 + solar_production_kw * 2 + np.random.normal(0, 3, n_points)
        inverter_efficiency_pct = 97.5 - (inverter_temp_c - 35) * 0.05 + np.random.normal(0, 0.5, n_points)
        
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
            'power_factor': np.clip(np.random.normal(0.98, 0.02, n_points), 0.90, 1.00),
        })
        
        self.timeseries_data[system_id] = df
        
        print(f"  ✓ Loaded {len(df)} data points")
        print(f"  Metrics: Solar, Consumption, Grid I/O, Battery SoC, Voltage, Frequency")
        
        return df


# ═══════════════════════════════════════════════════════════════════════════
#  POWER QUALITY MONITORING & GRID CODE COMPLIANCE
# ═══════════════════════════════════════════════════════════════════════════

class GridComplianceAnalyzer:
    """
    Monitors compliance with grid code standards (IEEE 1547, Philippine ERC regulations).
    
    Key functions:
    - Voltage/frequency deviation tracking
    - Power quality assessment (THD, power factor)
    - Grid code violation detection and reporting
    
    Maps to TSCnet role requirements: "Grid code compliance monitoring"
    """
    
    def __init__(self, telemetry_df: pd.DataFrame):
        self.df = telemetry_df.copy()
        self.violations = []
        
    def analyze_voltage_compliance(self):
        """
        IEEE 1547-2018 / Philippine Grid Code:
        Voltage must remain within ±10% of nominal (230V ±23V = 207-253V)
        """
        print("\n[GRID COMPLIANCE] Voltage Analysis")
        print("-" * 80)
        
        v_min = CONFIG['voltage_nominal_v'] * (1 - CONFIG['voltage_tolerance_pct'] / 100)
        v_max = CONFIG['voltage_nominal_v'] * (1 + CONFIG['voltage_tolerance_pct'] / 100)
        
        self.df['voltage_violation'] = (
            (self.df['voltage_v'] < v_min) | (self.df['voltage_v'] > v_max)
        )
        
        violations = self.df[self.df['voltage_violation']]
        violation_rate = len(violations) / len(self.df) * 100
        
        print(f"  Nominal voltage: {CONFIG['voltage_nominal_v']}V")
        print(f"  Allowable range: {v_min:.1f}V - {v_max:.1f}V")
        print(f"  Violation rate: {violation_rate:.2f}%")
        
        if len(violations) > 0:
            print(f"  ⚠️ {len(violations)} voltage violations detected")
            severe = violations[
                (violations['voltage_v'] < v_min * 0.95) | 
                (violations['voltage_v'] > v_max * 1.05)
            ]
            if len(severe) > 0:
                print(f"     - {len(severe)} SEVERE (>15% deviation)")
                
            # Log violations
            for _, row in violations.head(5).iterrows():
                self.violations.append({
                    'timestamp': row['timestamp'],
                    'type': 'VOLTAGE_DEVIATION',
                    'severity': 'SEVERE' if abs(row['voltage_v'] - CONFIG['voltage_nominal_v']) > 30 else 'MODERATE',
                    'value': row['voltage_v'],
                    'limit': f"{v_min:.1f}-{v_max:.1f}V",
                    'action': 'Report to grid operator if sustained >5min'
                })
        else:
            print("  ✓ All voltage readings within grid code limits")
        
        return violation_rate
    
    def analyze_frequency_compliance(self):
        """
        Philippine Grid Code: Frequency must remain within 60Hz ±0.5Hz
        Extended deviation (>0.5Hz for >30s) requires load shedding trigger
        """
        print("\n[GRID COMPLIANCE] Frequency Analysis")
        print("-" * 80)
        
        f_min = CONFIG['frequency_nominal_hz'] - CONFIG['frequency_tolerance_hz']
        f_max = CONFIG['frequency_nominal_hz'] + CONFIG['frequency_tolerance_hz']
        
        self.df['frequency_violation'] = (
            (self.df['frequency_hz'] < f_min) | (self.df['frequency_hz'] > f_max)
        )
        
        violations = self.df[self.df['frequency_violation']]
        violation_rate = len(violations) / len(self.df) * 100
        
        print(f"  Nominal frequency: {CONFIG['frequency_nominal_hz']}Hz")
        print(f"  Allowable range: {f_min:.2f}Hz - {f_max:.2f}Hz")
        print(f"  Violation rate: {violation_rate:.2f}%")
        
        if len(violations) > 0:
            print(f"  ⚠️ {len(violations)} frequency violations detected")
            
            # Detect sustained violations (>30 seconds = >0.5 consecutive hourly samples)
            # In production, would check minute-level data
            print(f"     - Requires load shedding coordination with grid operator")
        else:
            print("  ✓ Frequency stable within grid code limits")
        
        return violation_rate
    
    def analyze_power_quality(self):
        """
        Power quality assessment: power factor, THD (simulated).
        
        Standards:
        - Power factor > 0.95 (IEEE 1547)
        - Voltage THD < 5% (IEEE 519)
        - Current THD < 8%
        """
        print("\n[POWER QUALITY] Assessment")
        print("-" * 80)
        
        # Power factor analysis
        low_pf = self.df[self.df['power_factor'] < CONFIG['power_factor_min']]
        pf_compliance = (1 - len(low_pf) / len(self.df)) * 100
        
        print(f"  Power Factor:")
        print(f"    Mean: {self.df['power_factor'].mean():.3f}")
        print(f"    Min: {self.df['power_factor'].min():.3f}")
        print(f"    Compliance rate: {pf_compliance:.1f}% (target: {CONFIG['power_factor_min']})")
        
        if len(low_pf) > 0:
            print(f"    ⚠️ {len(low_pf)} instances below threshold")
            print(f"       Action: Check for reactive power compensation needs")
        
        # THD would be calculated from harmonic analysis in production
        # Simulating here for demonstration
        thd_voltage_pct = np.random.uniform(1.5, 4.5)
        thd_current_pct = np.random.uniform(3.0, 7.0)
        
        print(f"\n  Total Harmonic Distortion (THD):")
        print(f"    Voltage THD: {thd_voltage_pct:.1f}% (limit: {CONFIG['thd_voltage_max_pct']}%)")
        print(f"    Current THD: {thd_current_pct:.1f}% (limit: {CONFIG['thd_current_max_pct']}%)")
        
        if thd_voltage_pct < CONFIG['thd_voltage_max_pct'] and thd_current_pct < CONFIG['thd_current_max_pct']:
            print(f"    ✓ THD within IEEE 519 limits")
        else:
            print(f"    ⚠️ THD exceeds limits - investigate harmonic sources")
        
        return {
            'pf_compliance': pf_compliance,
            'thd_voltage': thd_voltage_pct,
            'thd_current': thd_current_pct
        }


# ═══════════════════════════════════════════════════════════════════════════
#  BATTERY ENERGY STORAGE SYSTEM (BESS) ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════

class BatteryStorageAnalyzer:
    """
    Battery State-of-Health monitoring and degradation modeling.
    
    SunPower SunVault storage system analytics:
    - State-of-Health (SoH) estimation
    - Capacity fade tracking
    - Cycle counting and lifetime prediction
    - Thermal management monitoring
    
    Maps to TSCnet role: "Battery storage system monitoring and optimization"
    """
    
    def __init__(self, telemetry_df: pd.DataFrame):
        self.df = telemetry_df.copy()
        
    def estimate_state_of_health(self):
        """
        State-of-Health estimation using capacity fade model.
        
        SoH = (Current Max Capacity / Rated Capacity) × 100%
        
        Degradation drivers:
        - Cycle count (depth-of-discharge weighted)
        - Temperature exposure
        - Time (calendar aging)
        """
        print("\n[BATTERY ANALYTICS] State-of-Health Estimation")
        print("-" * 80)
        
        # Calculate daily SoC swing (proxy for cycle depth)
        self.df['date'] = self.df['timestamp'].dt.date
        daily_cycles = self.df.groupby('date')['battery_soc_pct'].agg(['max', 'min'])
        daily_cycles['swing_pct'] = daily_cycles['max'] - daily_cycles['min']
        
        # Weighted cycle count (full DoD = 1 cycle, partial = proportional)
        equivalent_cycles = (daily_cycles['swing_pct'] / 100).sum()
        
        # Simplified SoH model (in production, would use electrochemical models)
        # Assume 2.5% capacity loss per year, accelerated by high cycle count
        days_in_service = (self.df['timestamp'].max() - self.df['timestamp'].min()).days
        calendar_fade_pct = (days_in_service / 365) * CONFIG['capacity_fade_max_per_year']
        cycle_fade_pct = (equivalent_cycles / 3650) * 10  # 10% after 10 years @ 1 cycle/day
        
        total_fade_pct = calendar_fade_pct + cycle_fade_pct
        soh_pct = 100 - total_fade_pct
        
        print(f"  Days in service: {days_in_service}")
        print(f"  Equivalent full cycles: {equivalent_cycles:.1f}")
        print(f"  Estimated SoH: {soh_pct:.1f}%")
        
        # Classification
        if soh_pct >= CONFIG['soh_warning_threshold']:
            status = "🟢 HEALTHY"
            action = "Continue normal operation"
        elif soh_pct >= CONFIG['soh_critical_threshold']:
            status = "🟡 WARNING"
            action = "Schedule preventive maintenance within 6 months"
        else:
            status = "🔴 CRITICAL"
            action = "Replacement recommended - contact SunPower service"
        
        print(f"  Status: {status}")
        print(f"  Action: {action}")
        
        # Degradation trend
        projected_eol_years = (CONFIG['soh_critical_threshold'] - soh_pct) / CONFIG['capacity_fade_max_per_year']
        print(f"  Projected end-of-life: {projected_eol_years:.1f} years")
        
        return {
            'soh_pct': soh_pct,
            'equivalent_cycles': equivalent_cycles,
            'status': status,
            'eol_years': projected_eol_years
        }
    
    def analyze_charge_discharge_efficiency(self):
        """
        Round-trip efficiency analysis for battery system.
        
        RTE = (Energy Out / Energy In) × 100%
        
        Typical SunVault RTE: 90-95%
        Degradation or BMS issues cause efficiency drop
        """
        print("\n[BATTERY ANALYTICS] Round-Trip Efficiency")
        print("-" * 80)
        
        # Calculate energy flows (simplified - would integrate power × time in production)
        charge_events = self.df[self.df['battery_soc_pct'].diff() > 0]
        discharge_events = self.df[self.df['battery_soc_pct'].diff() < 0]
        
        # Simulate round-trip efficiency (would calculate from actual energy measurements)
        rte_pct = np.random.normal(92, 2)  # SunVault typical
        
        print(f"  Round-trip efficiency: {rte_pct:.1f}%")
        
        if rte_pct >= 90:
            print(f"  ✓ Efficiency within normal range (90-95%)")
        else:
            print(f"  ⚠️ Efficiency below expected - investigate BMS/cell imbalance")
        
        return rte_pct


# ═══════════════════════════════════════════════════════════════════════════
#  INVERTER PERFORMANCE & PREDICTIVE MAINTENANCE
# ═══════════════════════════════════════════════════════════════════════════

class InverterHealthMonitor:
    """
    Inverter performance monitoring and predictive maintenance.
    
    Key indicators:
    - Conversion efficiency (DC → AC)
    - Thermal performance (junction temperature)
    - Anomaly detection (efficiency drops, thermal runaway)
    
    Maps to TSCnet role: "Equipment health monitoring and maintenance planning"
    """
    
    def __init__(self, telemetry_df: pd.DataFrame):
        self.df = telemetry_df.copy()
        
    def analyze_conversion_efficiency(self):
        """
        Inverter efficiency should be 96-98% for modern equipment.
        Degradation indicates component aging or fault.
        """
        print("\n[INVERTER HEALTH] Conversion Efficiency")
        print("-" * 80)
        
        mean_eff = self.df['inverter_efficiency_pct'].mean()
        min_eff = self.df['inverter_efficiency_pct'].min()
        
        low_eff_count = len(self.df[self.df['inverter_efficiency_pct'] < CONFIG['inverter_efficiency_min']])
        
        print(f"  Mean efficiency: {mean_eff:.1f}%")
        print(f"  Minimum recorded: {min_eff:.1f}%")
        print(f"  Events below {CONFIG['inverter_efficiency_min']}%: {low_eff_count}")
        
        if mean_eff >= CONFIG['inverter_efficiency_min']:
            print(f"  ✓ Performance within expected range")
        else:
            print(f"  ⚠️ Efficiency degradation detected - schedule inspection")
        
        # Efficiency vs. temperature correlation
        corr = self.df[['inverter_temp_c', 'inverter_efficiency_pct']].corr().iloc[0, 1]
        print(f"\n  Temperature-Efficiency correlation: {corr:.3f}")
        
        if corr < -0.5:
            print(f"    Strong inverse correlation - thermal management optimal")
        else:
            print(f"    Weak correlation - investigate cooling system")
        
        return mean_eff
    
    def detect_thermal_anomalies(self):
        """
        Thermal runaway detection using statistical anomaly detection.
        
        Sustained high temperature (>65°C) indicates cooling issues or overload.
        """
        print("\n[INVERTER HEALTH] Thermal Analysis")
        print("-" * 80)
        
        max_temp = self.df['inverter_temp_c'].max()
        mean_temp = self.df['inverter_temp_c'].mean()
        
        high_temp_events = self.df[self.df['inverter_temp_c'] > CONFIG['inverter_temp_max_c']]
        
        print(f"  Mean temperature: {mean_temp:.1f}°C")
        print(f"  Maximum recorded: {max_temp:.1f}°C")
        print(f"  Thermal limit: {CONFIG['inverter_temp_max_c']}°C")
        
        if len(high_temp_events) > 0:
            print(f"  ⚠️ {len(high_temp_events)} over-temperature events")
            print(f"     Action: Check ventilation, reduce load if persistent")
        else:
            print(f"  ✓ Thermal performance normal")
        
        # Anomaly detection using Isolation Forest
        iso_forest = IsolationForest(contamination=0.05, random_state=42)
        anomalies = iso_forest.fit_predict(self.df[['inverter_temp_c', 'inverter_efficiency_pct']])
        
        anomaly_count = (anomalies == -1).sum()
        print(f"\n  Anomalies detected (ML): {anomaly_count}")
        
        if anomaly_count > 0:
            print(f"     - Flagged for detailed investigation")
        
        return len(high_temp_events)


# ═══════════════════════════════════════════════════════════════════════════
#  REGULATORY COMPLIANCE REPORTING
# ═══════════════════════════════════════════════════════════════════════════

class ComplianceReportGenerator:
    """
    Automated regulatory compliance reporting.
    
    Reports generated:
    - Grid code compliance summary (voltage/frequency adherence)
    - Renewable energy credit (REC) documentation
    - Power quality assessment (IEEE 519, 1547)
    - Battery storage safety compliance
    
    Maps to TSCnet role: "Regulatory reporting and documentation"
    """
    
    def __init__(self, telemetry_df, compliance_results, battery_results, inverter_results):
        self.df = telemetry_df
        self.compliance = compliance_results
        self.battery = battery_results
        self.inverter = inverter_results
        
    def generate_monthly_compliance_report(self, output_path='outputs/compliance_report.json'):
        """
        Generate monthly compliance report for regulatory submission.
        
        Philippine Energy Regulatory Commission (ERC) requirements:
        - Distributed generation compliance
        - Net metering documentation
        - Power quality standards adherence
        """
        print("\n" + "=" * 80)
        print("REGULATORY COMPLIANCE REPORT GENERATION")
        print("=" * 80)
        
        report = {
            'report_metadata': {
                'report_type': 'Monthly Distributed Solar Generation Compliance',
                'reporting_period': f"{self.df['timestamp'].min()} to {self.df['timestamp'].max()}",
                'system_id': 'SUNPOWER-PH-001',
                'rated_capacity_kw': 8.5,
                'battery_capacity_kwh': 13.5,  # SunVault typical
                'generator': 'Geraldine Castillo - Data Analyst',
                'generated_timestamp': datetime.now().isoformat()
            },
            
            'grid_code_compliance': {
                'voltage_compliance_pct': 100 - self.compliance.get('voltage_violation_rate', 0),
                'frequency_compliance_pct': 100 - self.compliance.get('frequency_violation_rate', 0),
                'power_factor_mean': float(self.df['power_factor'].mean()),
                'thd_voltage_pct': self.compliance.get('thd_voltage', 0),
                'thd_current_pct': self.compliance.get('thd_current', 0),
                'status': 'COMPLIANT' if self.compliance.get('voltage_violation_rate', 0) < 1.0 else 'NON-COMPLIANT'
            },
            
            'energy_generation_summary': {
                'total_solar_generation_kwh': float(self.df['solar_production_kw'].sum()),
                'total_consumption_kwh': float(self.df['consumption_kw'].sum()),
                'total_grid_export_kwh': float(self.df['grid_export_kw'].sum()),
                'total_grid_import_kwh': float(self.df['grid_import_kw'].sum()),
                'self_consumption_rate_pct': float((1 - self.df['grid_export_kw'].sum() / self.df['solar_production_kw'].sum()) * 100),
                'renewable_energy_credits_kwh': float(self.df['solar_production_kw'].sum())  # REC eligible
            },
            
            'battery_storage_performance': {
                'state_of_health_pct': self.battery.get('soh_pct', 100),
                'equivalent_cycles': self.battery.get('equivalent_cycles', 0),
                'round_trip_efficiency_pct': self.battery.get('rte_pct', 92),
                'status': self.battery.get('status', 'UNKNOWN'),
                'projected_eol_years': self.battery.get('eol_years', 10)
            },
            
            'inverter_performance': {
                'mean_efficiency_pct': self.inverter.get('mean_efficiency', 97),
                'thermal_events': self.inverter.get('thermal_events', 0),
                'status': 'HEALTHY' if self.inverter.get('mean_efficiency', 97) > 96 else 'DEGRADED'
            },
            
            'data_quality_metrics': {
                'total_data_points': len(self.df),
                'missing_data_points': int(self.df.isnull().sum().sum()),
                'data_completeness_pct': float((1 - self.df.isnull().sum().sum() / (len(self.df) * len(self.df.columns))) * 100)
            }
        }
        
        # Save to JSON
        import os
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n✓ Compliance report generated: {output_path}")
        print(f"\nKey Findings:")
        print(f"  Grid Code Compliance: {report['grid_code_compliance']['status']}")
        print(f"  Total Solar Generation: {report['energy_generation_summary']['total_solar_generation_kwh']:.1f} kWh")
        print(f"  Battery SoH: {report['battery_storage_performance']['state_of_health_pct']:.1f}%")
        print(f"  Inverter Status: {report['inverter_performance']['status']}")
        
        return report


# ═══════════════════════════════════════════════════════════════════════════
#  DEMO EXECUTION
# ═══════════════════════════════════════════════════════════════════════════

def main():
    """
    Demonstrates complete power system analytics workflow.
    
    Workflow:
    1. Data acquisition (mySunPower API / Home Assistant equivalent)
    2. Grid code compliance analysis
    3. Battery storage health monitoring
    4. Inverter performance assessment
    5. Regulatory compliance reporting
    """
    print("\n" + "=" * 80)
    print("SUNPOWER DISTRIBUTED SOLAR + STORAGE ANALYTICS")
    print("Demonstrating Power System Data Management Capabilities")
    print("=" * 80)
    
    # Stage 1: Data Acquisition
    data_mgr = PowerSystemDataManager()
    telemetry = data_mgr.load_power_telemetry(
        system_id='SUNPOWER-PH-001',
        start_date='2024-01-01',
        end_date='2024-01-31'
    )
    
    # Stage 2: Grid Compliance Analysis
    grid_analyzer = GridComplianceAnalyzer(telemetry)
    voltage_violation_rate = grid_analyzer.analyze_voltage_compliance()
    frequency_violation_rate = grid_analyzer.analyze_frequency_compliance()
    power_quality = grid_analyzer.analyze_power_quality()
    
    compliance_results = {
        'voltage_violation_rate': voltage_violation_rate,
        'frequency_violation_rate': frequency_violation_rate,
        'thd_voltage': power_quality['thd_voltage'],
        'thd_current': power_quality['thd_current']
    }
    
    # Stage 3: Battery Analytics
    battery_analyzer = BatteryStorageAnalyzer(telemetry)
    soh_results = battery_analyzer.estimate_state_of_health()
    rte_pct = battery_analyzer.analyze_charge_discharge_efficiency()
    
    battery_results = {**soh_results, 'rte_pct': rte_pct}
    
    # Stage 4: Inverter Health
    inverter_monitor = InverterHealthMonitor(telemetry)
    mean_efficiency = inverter_monitor.analyze_conversion_efficiency()
    thermal_events = inverter_monitor.detect_thermal_anomalies()
    
    inverter_results = {
        'mean_efficiency': mean_efficiency,
        'thermal_events': thermal_events
    }
    
    # Stage 5: Compliance Reporting
    report_gen = ComplianceReportGenerator(
        telemetry, compliance_results, battery_results, inverter_results
    )
    report = report_gen.generate_monthly_compliance_report()
    
    print("\n" + "=" * 80)
    print("ANALYTICS COMPLETE")
    print("=" * 80)
    print(f"✓ Processed {len(telemetry)} data points")
    print(f"✓ Grid compliance assessed")
    print(f"✓ Battery SoH estimated: {battery_results['soh_pct']:.1f}%")
    print(f"✓ Inverter performance monitored")
    print(f"✓ Regulatory report generated")
    print("\nRelevance to TSCnet Role:")
    print("  • Power system data management (equivalent to AVEVA PI System)")
    print("  • Grid code compliance monitoring (IEEE 1547, voltage/frequency)")
    print("  • Battery storage analytics (SoH, degradation modeling)")
    print("  • Predictive maintenance (inverter anomaly detection)")
    print("  • Regulatory reporting automation (ERC compliance)")


if __name__ == "__main__":
    main()
