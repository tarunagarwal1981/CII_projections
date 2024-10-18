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
    # (same as before)
}

def get_db_engine():
    encoded_password = urllib.parse.quote(DB_CONFIG['password'])
    db_url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{encoded_password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(db_url)
    return engine

def get_vessel_data(engine, vessel_name, year):
    query = text("""
    # (SQL query as before)
    """)
    try:
        df = pd.read_sql(query, engine, params={'vessel_name': vessel_name, 'year': year})
        return df
    except Exception as e:
        st.error(f"Error executing SQL query: {str(e)}")
        return pd.DataFrame()

def calculate_reference_cii(capacity, ship_type):
    # (same as before)
    pass

def calculate_required_cii(reference_cii, year):
    reduction_factors = {2023: 0.95, 2024: 0.93, 2025: 0.91, 2026: 0.89}
    return reference_cii * reduction_factors.get(year, 1.0)

def calculate_cii_rating(attained_cii, required_cii, ship_type, capacity):
    # (same as before)
    pass

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

if __name__ == '__main__':
    main()
