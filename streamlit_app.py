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

# Streamlit page config
st.set_page_config(page_title="CII Calculator", layout="wide", page_icon="🚢")

# Apply custom CSS for a sleek UI
st.markdown("""
    <style>
    .stApp {
        max-width: 1200px;
        margin: 0 auto;
    }
    .stButton > button {
        background-color: #4CAF50;
        color: white;
    }
    </style>
    """, unsafe_allow_html=True)

# Initialize session state for CII calculations
if 'cii_data' not in st.session_state:
    st.session_state.cii_data = None

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
            THEN ROUND(CAST((COALESCE((SUM("FUEL_CONSUMPTION_HFO") - SUM("FC_FUEL_CONSUMPTION_HFO")) * 3.114, 0) + 
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
    # (same as before)

def calculate_required_cii(reference_cii, year):
    reduction_factors = {2023: 0.95, 2024: 0.93, 2025: 0.91, 2026: 0.89}
    return reference_cii * reduction_factors.get(year, 1.0)

def calculate_cii_rating(attained_cii, required_cii, ship_type, capacity):
    # (same as before)

def main():
    st.title('🚢 CII Calculator')

    # User input for vessel name, year, and calculate button in a single line with 5 columns
    col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])
    with col1:
        vessel_name = st.text_input("Enter Vessel Name")
    with col2:
        year = st.number_input('Year for CII Calculation', min_value=2023, max_value=date.today().year, value=date.today().year)
    with col3:
        calculate_clicked = st.button('Calculate CII')

    # Get database connection
    engine = get_db_engine()

    if calculate_clicked and vessel_name:
        # Fetch vessel data and calculate CII
        df = get_vessel_data(engine, vessel_name, year)
        if not df.empty:
            vessel_type = df['vessel_type'].iloc[0]
            imo_ship_type = VESSEL_TYPE_MAPPING.get(vessel_type)
            capacity = df['capacity'].iloc[0]
            attained_aer = df['Attained_AER'].iloc[0]

            if imo_ship_type and attained_aer is not None:
                reference_cii = calculate_reference_cii(capacity, imo_ship_type)
                required_cii = calculate_required_cii(reference_cii, year)
                cii_rating = calculate_cii_rating(attained_aer, required_cii, imo_ship_type, capacity)
                
                # Store CII data in session state to retain across re-runs
                st.session_state.cii_data = {
                    'attained_aer': attained_aer,
                    'required_cii': required_cii,
                    'cii_rating': cii_rating
                }
            else:
                if imo_ship_type is None:
                    st.error(f"The vessel type '{vessel_type}' is not supported for CII calculations.")
                if attained_aer is None:
                    st.error("Unable to calculate Attained AER. Please check the vessel's data.")
        else:
            st.error(f"No data found for vessel {vessel_name} in year {year}")

    # Display stored CII results if available
    if st.session_state.cii_data:
        col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])
        with col1:
            st.metric('Attained AER', f'{st.session_state.cii_data["attained_aer"]:.4f}')
        with col2:
            st.metric('Required CII', f'{st.session_state.cii_data["required_cii"]:.4f}')
        with col3:
            st.metric('CII Rating', st.session_state.cii_data["cii_rating"])

    # Additional code for route plotting and projections

if __name__ == '__main__':
    main()
