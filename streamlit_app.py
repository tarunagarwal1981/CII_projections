import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import urllib.parse
from datetime import date

# Database configuration
DB_CONFIG = {
    'host': 'aws-0-ap-south-1.pooler.supabase.com',
    'database': 'postgres',
    'user': 'postgres.conrxbcvuogbzfysomov',
    'password': 'wXAryCC8@iwNvj#',
    'port': '6543'
}

# Mapping of vessel types to IMO ship types
VESSEL_TYPE_MAPPING = {
    'ASPHALT/BITUMEN TANKER': 'tanker',
    'BULK CARRIER': 'bulk_carrier',
    'CEMENT CARRIER': 'bulk_carrier',
    'CHEM/PROD TANKER': 'tanker',
    'CHEMICAL TANKER': 'tanker',
    'Chemical/Products Tanker': 'tanker',
    'Combination Carrier': 'combination_carrier',
    'CONTAINER': 'container_ship',
    'Container Ship': 'container_ship',
    'Container/Ro-Ro Ship': 'ro_ro_cargo_ship',
    'Crude Oil Tanker': 'tanker',
    'Diving support vessel': None,
    'Gas Carrier': 'gas_carrier',
    'General Cargo Ship': 'general_cargo_ship',
    'LNG CARRIER': 'lng_carrier',
    'LPG CARRIER': 'gas_carrier',
    'LPG Tanker': 'gas_carrier',
    'Offshore Support Vessel': None,
    'OIL TANKER': 'tanker',
    'Other Ship Type': None,
    'Passenger Ship': 'cruise_passenger_ship',
    'Products Tanker': 'tanker',
    'Refrigerated Cargo Ship': 'refrigerated_cargo_carrier',
    'Ro-ro passenger ship': 'ro_ro_passenger_ship',
    'Ro-Ro Ship': 'ro_ro_cargo_ship',
    'Vehicle Carrier': 'ro_ro_cargo_ship_vc'
}

def get_db_engine():
    encoded_password = urllib.parse.quote(DB_CONFIG['password'])
    db_url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{encoded_password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(db_url)
    return engine

def get_vessel_data(engine, vessel_name, year):
    query = text("""
    SELECT 
        t1.vessel_name AS Vessel,
        t1.vessel_imo as IMO,
        SUM(distance_travelled_actual) AS total_distance,
        coalesce((SUM(fuel_consumption_hfo) - sum(fc_fuel_consumption_hfo)) * 3.114, 0) + 
        coalesce((SUM(fuel_consumption_lfo) - sum(fc_fuel_consumption_lfo)) * 3.151, 0) + 
        coalesce((SUM(fuel_consumption_go_do) - sum(fc_fuel_consumption_go_do)) * 3.206, 0) + 
        coalesce((SUM(fuel_consumption_lng) - sum(fc_fuel_consumption_lng)) * 2.75, 0) + 
        coalesce((SUM(fuel_consumption_lpg) - sum(fc_fuel_consumption_lpg)) * 3.00, 0) + 
        coalesce((SUM(fuel_consumption_methanol) - sum(fc_fuel_consumption_methanol)) * 1.375, 0) + 
        coalesce((SUM(fuel_consumption_ethanol) - sum(fc_fuel_consumption_ethanol)) * 1.913, 0) as CO2Emission,
        t2.deadweight,
        t2.vessel_type,
        round(SUM(distance_travelled_actual) * t2.deadweight, 2) as Transportwork,
        CASE 
            WHEN round(SUM(distance_travelled_actual) * t2.deadweight, 2) <> 0 
            THEN round((coalesce((SUM(fuel_consumption_hfo) - sum(fc_fuel_consumption_hfo)) * 3.114, 0) + 
                        coalesce((SUM(fuel_consumption_lfo) - sum(fc_fuel_consumption_lfo)) * 3.151, 0) + 
                        coalesce((SUM(fuel_consumption_go_do) - sum(fc_fuel_consumption_go_do)) * 3.206, 0) + 
                        coalesce((SUM(fuel_consumption_lng) - sum(fc_fuel_consumption_lng)) * 2.75, 0) + 
                        coalesce((SUM(fuel_consumption_lpg) - sum(fc_fuel_consumption_lpg)) * 3.00, 0) + 
                        coalesce((SUM(fuel_consumption_methanol) - sum(fc_fuel_consumption_methanol)) * 1.375, 0) + 
                        coalesce((SUM(fuel_consumption_ethanol) - sum(fc_fuel_consumption_ethanol)) * 1.913, 0)) * 1000000 / 
                        (SUM(distance_travelled_actual) * t2.deadweight), 2)
            ELSE NULL
        END as Attained_AER,
        MIN(report_date) as Startdate,
        MAX(report_date) as Enddate
    FROM 
        sf_consumption_logs AS t1
    LEFT JOIN 
        vessel_particulars AS t2 ON t1.vessel_imo = t2.vessel_imo
    WHERE 
        t1.vessel_name = :vessel_name
        AND EXTRACT(YEAR FROM report_date) = :year
    GROUP BY 
        t1.vessel_name, t1.vessel_imo, t2.deadweight, t2.vessel_type
    """)
    
    df = pd.read_sql(query, engine, params={'vessel_name': vessel_name, 'year': year})
    return df

def calculate_reference_cii(capacity, ship_type):
    params = {
        'bulk_carrier': [
            {'capacity_threshold': 279000, 'a': 4745, 'c': 0.622, 'use_dwt': True},
            {'capacity_threshold': float('inf'), 'a': 4745, 'c': 0.622, 'use_dwt': False}
        ],
        'gas_carrier': [
            {'capacity_threshold': 65000, 'a': 144050000000, 'c': 2.071, 'use_dwt': True},
            {'capacity_threshold': float('inf'), 'a': 8104, 'c': 0.639, 'use_dwt': True}
        ],
        'tanker': [{'capacity_threshold': float('inf'), 'a': 5247, 'c': 0.61, 'use_dwt': True}],
        'container_ship': [{'capacity_threshold': float('inf'), 'a': 1984, 'c': 0.489, 'use_dwt': True}],
        'general_cargo_ship': [
            {'capacity_threshold': 20000, 'a': 31948, 'c': 0.792, 'use_dwt': True},
            {'capacity_threshold': float('inf'), 'a': 588, 'c': 0.3885, 'use_dwt': True}
        ],
        'refrigerated_cargo_carrier': [{'capacity_threshold': float('inf'), 'a': 4600, 'c': 0.557, 'use_dwt': True}],
        'combination_carrier': [{'capacity_threshold': float('inf'), 'a': 40853, 'c': 0.812, 'use_dwt': True}],
        'lng_carrier': [
            {'capacity_threshold': 100000, 'a': 144790000000000, 'c': 2.673, 'use_dwt': True},
            {'capacity_threshold': 65000, 'a': 144790000000000, 'c': 2.673, 'use_dwt': True},
            {'capacity_threshold': float('inf'), 'a': 9.827, 'c': 0, 'use_dwt': True}
        ],
        'ro_ro_cargo_ship_vc': [{'capacity_threshold': float('inf'), 'a': 5739, 'c': 0.631, 'use_dwt': False}],
        'ro_ro_cargo_ship': [{'capacity_threshold': float('inf'), 'a': 10952, 'c': 0.637, 'use_dwt': True}],
        'ro_ro_passenger_ship': [{'capacity_threshold': float('inf'), 'a': 7540, 'c': 0.587, 'use_dwt': False}],
        'cruise_passenger_ship': [{'capacity_threshold': float('inf'), 'a': 930, 'c': 0.383, 'use_dwt': False}]
    }

    ship_params = params.get(ship_type.lower())
    if not ship_params:
        raise ValueError(f"Unknown ship type: {ship_type}")

    for param in ship_params:
        if capacity <= param['capacity_threshold']:
            a, c = param['a'], param['c']
            used_capacity = capacity if param['use_dwt'] else param['capacity_threshold']
            return a * (used_capacity ** -c)

    raise ValueError(f"Capacity {capacity} is out of range for ship type {ship_type}")

def calculate_required_cii(reference_cii, year):
    reduction_factors = {2023: 0.95, 2024: 0.93, 2025: 0.91, 2026: 0.89}
    return reference_cii * reduction_factors.get(year, 1.0)

def calculate_cii_rating(attained_cii, required_cii, ship_type):
    dd_vectors = {
        'bulk_carrier': [
            {'capacity_threshold': 297000, 'd': [0.86, 0.94, 1.06, 1.18]},
            {'capacity_threshold': float('inf'), 'd': [0.86, 0.94, 1.06, 1.18]}
        ],
        'tanker': [{'capacity_threshold': float('inf'), 'd': [0.82, 0.93, 1.08, 1.28]}],
        'container_ship': [{'capacity_threshold': float('inf'), 'd': [0.83, 0.94, 1.07, 1.19]}],
        'gas_carrier': [
            {'capacity_threshold': 65000, 'd': [0.85, 0.95, 1.06, 1.25]},
            {'capacity_threshold': float('inf'), 'd': [0.81, 0.91, 1.12, 1.44]}
        ],
        'lng_carrier': [
            {'capacity_threshold': 65000, 'd': [0.78, 0.92, 1.10, 1.37]},
            {'capacity_threshold': 100000, 'd': [0.78, 0.92, 1.10, 1.37]},
            {'capacity_threshold': float('inf'), 'd': [0.89, 0.98, 1.06, 1.13]}
        ],
        'ro_ro_cargo_ship': [{'capacity_threshold': float('inf'), 'd': [0.66, 0.90, 1.11, 1.37]}],
        'general_cargo_ship': [
            {'capacity_threshold': 20000, 'd': [0.83, 0.94, 1.06, 1.19]},
            {'capacity_threshold': float('inf'), 'd': [0.83, 0.94, 1.06, 1.19]}
        ],
        'refrigerated_cargo_carrier': [{'capacity_threshold': float('inf'), 'd': [0.78, 0.91, 1.07, 1.20]}],
        'combination_carrier': [{'capacity_threshold': float('inf'), 'd': [0.87, 0.96, 1.06, 1.14]}],
        'cruise_passenger_ship': [{'capacity_threshold': float('inf'), 'd': [0.87, 0.95, 1.06, 1.16]}],
        'ro_ro_cargo_ship_vc': [{'capacity_threshold': float('inf'), 'd': [0.86, 0.94, 1.06, 1.16]}],
        'ro_ro_passenger_ship': [{'capacity_threshold': float('inf'), 'd': [0.72, 0.90, 1.12, 1.41]}]
    }

    if ship_type is None:
        raise ValueError("Ship type is None, cannot proceed with CII rating calculation.")

    ship_params = dd_vectors.get(ship_type.lower())
    if not ship_params:
        raise ValueError(f"Unknown ship type: {ship_type}")

    for param in ship_params:
        if capacity <= param['capacity_threshold']:
            d1, d2, d3, d4 = param['d']
            break
    else:
        raise ValueError(f"Capacity {capacity} is out of range for ship type {ship_type}")

    superior = np.exp(d1) * required_cii
    lower = np.exp(d2) * required_cii
    upper = np.exp(d3) * required_cii
    inferior = np.exp(d4) * required_cii

    if attained_cii <= superior:
        return 'A'
    elif attained_cii <= lower:
        return 'B'
    elif attained_cii <= upper:
        return 'C'
    elif attained_cii <= inferior:
        return 'D'
    else:
        return 'E'

def main():
    st.title('Vessel CII Calculator')

    # Get database connection
    engine = get_db_engine()

    # User input for vessel name and year
    vessel_name = st.text_input("Enter Vessel Name")
    year = st.number_input('Year for CII Calculation', min_value=2023, max_value=date.today().year, value=date.today().year)

    if vessel_name:
        # Fetch vessel data
        df = get_vessel_data(engine, vessel_name, year)

        if not df.empty:
            vessel_type = df['vessel_type'].iloc[0]
            imo_ship_type = VESSEL_TYPE_MAPPING.get(vessel_type)
            deadweight = df['deadweight'].iloc[0]
            attained_aer = df['Attained_AER'].iloc[0]

            if imo_ship_type and attained_aer is not None:
                reference_cii = calculate_reference_cii(deadweight, imo_ship_type)
                required_cii = calculate_required_cii(reference_cii, year)
                cii_rating = calculate_cii_rating(attained_aer, required_cii, imo_ship_type)

                # Display results
                st.subheader(f'CII Results for Year {year}')
                st.write(f"Vessel Name: {vessel_name}")
                st.write(f"Vessel Type: {vessel_type}")
                col1, col2, col3 = st.columns(3)
                col1.metric('Attained AER', f'{attained_aer:.4f}')
                col2.metric('Required CII', f'{required_cii:.4f}')
                col3.metric('CII Rating', cii_rating)
            else:
                if imo_ship_type is None:
                    st.error(f"The vessel type '{vessel_type}' is not supported for CII calculations.")
                if attained_aer is None:
                    st.error("Unable to calculate Attained AER. Please check the vessel's data.")
        else:
            st.error(f"No data found for vessel {vessel_name} in year {year}")

if __name__ == '__main__':
    main()
