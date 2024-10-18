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
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.grid_options_builder import GridOptionsBuilder

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

# Function to calculate reference CII
def calculate_reference_cii(capacity, ship_type):
    params = {
        'bulk_carrier': [{'capacity_threshold': 279000, 'a': 4745, 'c': 0.622}],
        'gas_carrier': [{'capacity_threshold': 65000, 'a': 144050000000, 'c': 2.071}],
        'tanker': [{'capacity_threshold': float('inf'), 'a': 5247, 'c': 0.61}],
        'container_ship': [{'capacity_threshold': float('inf'), 'a': 1984, 'c': 0.489}],
        'general_cargo_ship': [{'capacity_threshold': float('inf'), 'a': 31948, 'c': 0.792}],
        'refrigerated_cargo_carrier': [{'capacity_threshold': float('inf'), 'a': 4600, 'c': 0.557}],
        'lng_carrier': [{'capacity_threshold': 100000, 'a': 144790000000000, 'c': 2.673}],
    }
    ship_params = params.get(ship_type.lower())
    if not ship_params:
        raise ValueError(f"Unknown ship type: {ship_type}")
    
    a, c = ship_params[0]['a'], ship_params[0]['c']
    return a * (capacity ** -c)

# Function to calculate required CII
def calculate_required_cii(reference_cii, year):
    reduction_factors = {2023: 0.95, 2024: 0.93, 2025: 0.91, 2026: 0.89}
    return reference_cii * reduction_factors.get(year, 1.0)

# Function to calculate CII rating
def calculate_cii_rating(attained_cii, required_cii):
    if attained_cii <= required_cii:
        return 'A'
    elif attained_cii <= 1.05 * required_cii:
        return 'B'
    elif attained_cii <= 1.1 * required_cii:
        return 'C'
    elif attained_cii <= 1.15 * required_cii:
        return 'D'
    else:
        return 'E'

# Load world ports data
@st.cache_data
def load_world_ports():
    df = pd.read_csv("UpdatedPub150.csv")
    return df['Main Port Name'].tolist()
world_ports_data = load_world_ports()

def port_autocomplete(port_names, default_value="", key=None):
    gb = GridOptionsBuilder.from_dict({
        "rowData": [{"port": name} for name in port_names],
        "columnDefs": [{"field": "port"}],
        "rowSelection": "single",
    })
    gb.configure_default_column(filterable=True, sorteable=True)
    grid_options = gb.build()
    
    grid_response = AgGrid(
        pd.DataFrame({"port": port_names}),
        gridOptions=grid_options,
        data_return_mode="FILTERED_AND_SORTED",
        update_mode="MODEL_CHANGED",
        fit_columns_on_grid_load=True,
        theme="streamlit",
        enable_enterprise_modules=False,
        height=200,
        width="100%",
        reload_data=False,
        key=key
    )
    
    selected_rows = grid_response["selected_rows"]
    if selected_rows:
        return selected_rows[0]["port"]
    return default_value

# Find best matching port
def world_port_index(port_to_match):
    best_match = process.extractOne(port_to_match, world_ports_data['Main Port Name'])
    return world_ports_data[world_ports_data['Main Port Name'] == best_match[0]].iloc[0]

# Calculate route distance
def route_distance(origin, destination):
    try:
        origin_port = world_port_index(origin)
        destination_port = world_port_index(destination)
        origin_coords = [float(origin_port['Longitude']), float(origin_port['Latitude'])]
        destination_coords = [float(destination_port['Longitude']), float(destination_port['Latitude'])]
        sea_route = sr.searoute(origin_coords, destination_coords, units="naut")
        return int(sea_route['properties']['length'])
    except Exception as e:
        st.error(f"Error calculating distance between {origin} and {destination}: {str(e)}")
        return 0

# Plot route on the map
def plot_route(ports):
    m = folium.Map(location=[0, 0], zoom_start=2)
    if len(ports) >= 2 and all(ports):
        coordinates = []
        for i in range(len(ports) - 1):
            try:
                start_port = world_port_index(ports[i])
                end_port = world_port_index(ports[i+1])
                start_coords = [float(start_port['Latitude']), float(start_port['Longitude'])]
                end_coords = [float(end_port['Latitude']), float(end_port['Longitude'])]
                
                folium.Marker(start_coords, popup=ports[i]).add_to(m)
                if i == len(ports) - 2:
                    folium.Marker(end_coords, popup=ports[i+1]).add_to(m)
                
                route = sr.searoute(start_coords[::-1], end_coords[::-1])
                folium.PolyLine(locations=[list(reversed(coord)) for coord in route['geometry']['coordinates']], 
                                color="red", weight=2, opacity=0.8).add_to(m)
                
                coordinates.extend([start_coords, end_coords])
            except Exception as e:
                st.error(f"Error plotting route for {ports[i]} to {ports[i+1]}: {str(e)}")
        
        if coordinates:
            m.fit_bounds(coordinates)
    
    return m

def main():
    st.title('🚢 CII Calculator')

    # User input for vessel name and year
    col1, col2, col3 = st.columns(3)
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

            total_distance = df['total_distance'].iloc[0] if 'total_distance' in df.columns else None
            co2_emission = df['CO2Emission'].iloc[0] if 'CO2Emission' in df.columns else None

            if imo_ship_type and attained_aer is not None:
                reference_cii = calculate_reference_cii(capacity, imo_ship_type)
                required_cii = calculate_required_cii(reference_cii, year)
                cii_rating = calculate_cii_rating(attained_aer, required_cii)
                
                st.session_state.cii_data = {
                    'attained_aer': attained_aer,
                    'required_cii': required_cii,
                    'cii_rating': cii_rating,
                    'total_distance': total_distance,
                    'co2_emission': co2_emission,
                    'capacity': capacity
                }
            else:
                if imo_ship_type is None:
                    st.error(f"The vessel type '{vessel_type}' is not supported for CII calculations.")
                if attained_aer is None:
                    st.error("Unable to calculate Attained AER. Please check the vessel's data.")
        else:
            st.error(f"No data found for vessel {vessel_name} in year {year}")

    # Display stored CII results and additional metrics if available
    if st.session_state.cii_data:
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric('Attained AER', f'{st.session_state.cii_data["attained_aer"]:.4f}')
        with col2:
            st.metric('Required CII', f'{st.session_state.cii_data["required_cii"]:.4f}')
        with col3:
            st.metric('CII Rating', st.session_state.cii_data["cii_rating"])
        with col4:
            st.metric('Total Distance (NM)', f'{st.session_state.cii_data["total_distance"]:.2f}' if st.session_state.cii_data.get('total_distance') is not None else "N/A")
        with col5:
            st.metric('CO2 Emission (Tonnes)', f'{st.session_state.cii_data["co2_emission"]:.2f}' if st.session_state.cii_data.get('co2_emission') is not None else "N/A")

    # CII Projections based on route
    st.subheader('CII Projections based on Route')

    # Create a 2-column layout: left for inputs, right for map
    left_col, right_col = st.columns([3, 3])

    # Load port names
    port_names = load_world_ports()

    # Input fields in the left column
    with left_col:
        speed = st.number_input('Speed (knots)', min_value=0.0, value=12.0, step=0.1)
        daily_foc = st.number_input('Daily FOC (mT/d)', min_value=0.0, value=30.0, step=0.1)
        num_ports = st.number_input('Number of Ports', min_value=2, max_value=10, value=2)
        
        ports = []
        for i in range(num_ports):
            port = port_autocomplete(port_names, key=f'port_{i}')
            if port:
                ports.append(port)
                st.write(f"Port {i+1}: {port}")

    # Map in the right column
    with right_col:
        if len(ports) >= 2 and all(ports):
            m = plot_route(ports)
        else:
            m = folium.Map(location=[0, 0], zoom_start=2)
        st_folium(m, width=None, height=400)  # width=None allows it to fill the column

    # Project CII button and calculations
    if st.button('Project CII'):
        if len(ports) >= 2 and all(ports):
            total_new_distance = sum(route_distance(ports[i], ports[i+1]) for i in range(len(ports) - 1))
            st.write(f"Total new distance: {total_new_distance} nautical miles")

            total_existing_distance = st.session_state.cii_data['total_distance']
            co2_emission = st.session_state.cii_data['co2_emission']
            capacity = st.session_state.cii_data['capacity']

            projected_aer = (co2_emission + (total_new_distance / (speed * 24)) * daily_foc * 3.114) * 1000000 / (
                    (total_existing_distance + total_new_distance) * capacity)

            required_cii = st.session_state.cii_data['required_cii']
            projected_cii_rating = calculate_cii_rating(projected_aer, required_cii)

            st.session_state.projected_aer = projected_aer
            st.session_state.projected_cii_rating = projected_cii_rating

    # Display Projected AER and CII Rating if available
    if 'projected_aer' in st.session_state and 'projected_cii_rating' in st.session_state:
        st.write(f"Projected AER: {st.session_state.projected_aer:.4f}")
        st.write(f"Projected CII Rating: {st.session_state.projected_cii_rating}")

if __name__ == '__main__':
    main()
