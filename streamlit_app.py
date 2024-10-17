import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import urllib.parse
from datetime import date
import folium
from streamlit_folium import st_folium
import searoute as sr
from fuzzywuzzy import process

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

# Streamlit page config
st.set_page_config(page_title="CII Calculator", layout="wide", page_icon="🚢")

# Apply custom CSS for light theme and sleek UI
st.markdown("""
    <style>
    .stApp {
        background-color: #F0F2F6;
    }
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    h1, h2, h3 {
        color: #1E3A8A;
    }
    .stButton > button {
        background-color: #3B82F6;
        color: white;
        border-radius: 0.375rem;
        padding: 0.5rem 1rem;
        border: none;
    }
    .stTextInput > div > div > input {
        background-color: white;
        color: #1F2937;
        border-radius: 0.375rem;
        border: 1px solid #D1D5DB;
    }
    .stNumberInput > div > div > input {
        background-color: white;
        color: #1F2937;
        border-radius: 0.375rem;
        border: 1px solid #D1D5DB;
    }
    .css-1kyxreq {
        justify-content: center;
    }
    </style>
    """, unsafe_allow_html=True)

def get_db_engine():
    encoded_password = urllib.parse.quote(DB_CONFIG['password'])
    db_url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{encoded_password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(db_url)
    return engine

def get_vessel_data(engine, vessel_name, year):
    query = text("""
    SELECT 
        t1."VESSEL_NAME" AS "Vessel",
        t1."VESSEL_IMO" AS "IMO",
        SUM("DISTANCE_TRAVELLED_ACTUAL") AS "total_distance",
        COALESCE((SUM("FUEL_CONSUMPTION_HFO") - SUM("FC_FUEL_CONSUMPTION_HFO")) * 3.114, 0) + 
        COALESCE((SUM("FUEL_CONSUMPTION_LFO") - SUM("FC_FUEL_CONSUMPTION_LFO")) * 3.151, 0) + 
        COALESCE((SUM("FUEL_CONSUMPTION_GO_DO") - SUM("FC_FUEL_CONSUMPTION_GO_DO")) * 3.206, 0) + 
        COALESCE((SUM("FUEL_CONSUMPTION_LNG") - SUM("FC_FUEL_CONSUMPTION_LNG")) * 2.75, 0) + 
        COALESCE((SUM("FUEL_CONSUMPTION_LPG") - SUM("FC_FUEL_CONSUMPTION_LPG")) * 3.00, 0) + 
        COALESCE((SUM("FUEL_CONSUMPTION_METHANOL") - SUM("FC_FUEL_CONSUMPTION_METHANOL")) * 1.375, 0) + 
        COALESCE((SUM("FUEL_CONSUMPTION_ETHANOL") - SUM("FC_FUEL_CONSUMPTION_ETHANOL")) * 1.913, 0) AS "CO2Emission",
        t2."deadweight" AS "capacity",
        t2."vessel_type",
        ROUND(CAST(SUM("DISTANCE_TRAVELLED_ACTUAL") * t2."deadweight" AS NUMERIC), 2) AS "Transportwork",
        CASE 
            WHEN ROUND(CAST(SUM("DISTANCE_TRAVELLED_ACTUAL") * t2."deadweight" AS NUMERIC), 2) <> 0 
            THEN ROUND(CAST((
                COALESCE((SUM("FUEL_CONSUMPTION_HFO") - SUM("FC_FUEL_CONSUMPTION_HFO")) * 3.114, 0) + 
                COALESCE((SUM("FUEL_CONSUMPTION_LFO") - SUM("FC_FUEL_CONSUMPTION_LFO")) * 3.151, 0) + 
                COALESCE((SUM("FUEL_CONSUMPTION_GO_DO") - SUM("FC_FUEL_CONSUMPTION_GO_DO")) * 3.206, 0) + 
                COALESCE((SUM("FUEL_CONSUMPTION_LNG") - SUM("FC_FUEL_CONSUMPTION_LNG")) * 2.75, 0) + 
                COALESCE((SUM("FUEL_CONSUMPTION_LPG") - SUM("FC_FUEL_CONSUMPTION_LPG")) * 3.00, 0) + 
                COALESCE((SUM("FUEL_CONSUMPTION_METHANOL") - SUM("FC_FUEL_CONSUMPTION_METHANOL")) * 1.375, 0) + 
                COALESCE((SUM("FUEL_CONSUMPTION_ETHANOL") - SUM("FC_FUEL_CONSUMPTION_ETHANOL")) * 1.913, 0)
            ) * 1000000 / (SUM("DISTANCE_TRAVELLED_ACTUAL") * t2."deadweight") AS NUMERIC), 2)
            ELSE NULL
        END AS "Attained_AER",
        MIN("REPORT_DATE") AS "Startdate",
        MAX("REPORT_DATE") AS "Enddate"
    FROM 
        "sf_consumption_logs" AS t1
    LEFT JOIN 
        "vessel_particulars" AS t2 ON t1."VESSEL_IMO" = t2."vessel_imo"
    WHERE 
        t1."VESSEL_NAME" = :vessel_name
        AND EXTRACT(YEAR FROM "REPORT_DATE") = :year
    GROUP BY 
        t1."VESSEL_NAME", t1."VESSEL_IMO", t2."deadweight", t2."vessel_type"
    """)
    
    try:
        df = pd.read_sql(query, engine, params={'vessel_name': vessel_name, 'year': year})
        return df
    except Exception as e:
        st.error(f"Error executing SQL query: {str(e)}")
        return pd.DataFrame()

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

def calculate_cii_rating(attained_cii, required_cii, ship_type, capacity):
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

    ship_params = dd_vectors
