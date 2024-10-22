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

# Apply custom CSS
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

# Initialize session state
if 'cii_data' not in st.session_state:
    st.session_state.cii_data = {}
if 'port_table_data' not in st.session_state:
    st.session_state.port_table_data = []

# Vessel type mapping
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
    """Create and return database engine"""
    encoded_password = urllib.parse.quote(DB_CONFIG['password'])
    db_url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{encoded_password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(db_url)

def get_vessel_data(engine, vessel_name, year):
    """Fetch vessel data from database"""
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
        return pd.read_sql(query, engine, params={'vessel_name': vessel_name, 'year': year})
    except Exception as e:
        st.error(f"Error executing SQL query: {str(e)}")
        return pd.DataFrame()

def calculate_reference_cii(capacity, ship_type):
    """Calculate reference CII based on capacity and ship type"""
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

def calculate_required_cii(reference_cii, year):
    """Calculate required CII based on reference CII and year"""
    reduction_factors = {2023: 0.95, 2024: 0.93, 2025: 0.91, 2026: 0.89}
    return reference_cii * reduction_factors.get(year, 1.0)

def calculate_cii_rating(attained_cii, required_cii):
    """Calculate CII rating based on attained and required CII"""
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

@st.cache_data
def load_world_ports():
    """Load and cache world ports data"""
    return pd.read_csv("UpdatedPub150.csv")

def world_port_index(port_to_match):
    """Find best matching port from world ports data"""
    best_match = process.extractOne(port_to_match, world_ports_data['Main Port Name'])
    return world_ports_data[world_ports_data['Main Port Name'] == best_match[0]].iloc[0]

def route_distance(origin, destination):
    """Calculate route distance between two ports"""
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

def plot_route(ports):
    """Plot route on a Folium map"""
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
                folium.PolyLine(
                    locations=[list(reversed(coord)) for coord in route['geometry']['coordinates']], 
                    color="red",
                    weight=2,
                    opacity=0.8
                ).add_to(m)
                
                coordinates.extend([start_coords, end_coords])
            except Exception as e:
                st.error(f"Error plotting route for {ports[i]} to {ports[i+1]}: {str(e)}")
        
        if coordinates:
            m.fit_bounds(coordinates)
    
    return m

def main():
    st.title('🚢 CII Calculator')

    # User inputs
    col1, col2, col3 = st.columns(3)
    with col1:
        vessel_name = st.text_input("Enter Vessel Name")
    with col2:
        year = st.number_input('Year for CII Calculation', 
                              min_value=2023, 
                              max_value=date.today().year, 
                              value=date.today().year)
    with col3:
        calculate_clicked = st.button('Calculate CII')

    # Load world ports data
    world_ports_data = load_world_ports()

    # Calculate CII
    if calculate_clicked and vessel_name:
        engine = get_db_engine()
        df = get_vessel_data(engine, vessel_name, year)
        
        if not df.empty:
            vessel_type = df['vessel_type'].iloc[0]
            imo_ship_type = VESSEL_TYPE_MAPPING.get(vessel_type)
            capacity = df['capacity'].iloc[0]
            attained_aer = df['Attained_AER'].iloc[0]

            if imo_ship_type and attained_aer is not None:
                reference_cii = calculate_reference_cii(capacity, imo_ship_type)
                required_cii = calculate_required_cii(reference_cii, year)
                cii_rating = calculate_cii_rating(attained_aer, required_cii)
                
                st.session_state.cii_data = {
                    'attained_aer': attained_aer,
                    'required_cii': required_cii,
                    'cii_rating': cii_rating,
                    'total_distance': df['total_distance'].iloc[0],
                    'co2_emission': df['CO2Emission'].iloc[0],
                    'capacity': capacity
                }
            else:
                if imo_ship_type is None:
                    st.error(f"The vessel type '{vessel_type}' is not supported for CII calculations.")
                if attained_aer is None:
                    st.error("Unable to calculate Attained AER. Please check the vessel's data.")
        else:
            st.error(f"No data found for vessel {vessel_name} in year {year}")

    # Display CII results
    if st.session_state.cii_data:
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric('Attained AER', 
                     f'{st.session_state.cii_data.get("attained_aer", "N/A"):.4f}')
        with col2:
            st.metric('Required CII', 
                     f'{st.session_state.cii_data.get("required_cii", "N/A"):.4f}')
        with col3:
            st.metric('CII Rating', 
                     st.session_state.cii_data.get("cii_rating", "N/A"))
        with col4:
            total_distance = st.session_state.cii_data.get("total_distance")
            st.metric('Total Distance (NM)', 
                     f'{total_distance:.2f}' if total_distance is not None else "N/A")
        with col5:
            co2_emission = st.session_state.cii_data.get("co2_emission")
            st.metric('CO2 Emission (Tonnes)', 
                     f'{co2_emission:.2f}' if co2_emission is not None else "N/A")

    # CII Projections section
    st.subheader('CII Projections based on Route')

    # Split layout for table and map
    left_col, right_col = st.columns([6, 4])

    # Route information table
    with left_col:
        st.write("### Route Information Table")
        
        # Create DataFrame for route information
        port_data_df = pd.DataFrame(
            st.session_state.port_table_data,
            columns=["From Port", "To Port", "Port Days", "Speed (knots)", 
                    "Fuel Used (mT)", "Consumption/day (mT)"]
        )
        
        # Use data_editor instead of experimental_data_editor
        edited_df = st.data_editor(
            port_data_df,
            num_rows="dynamic",
            key="port_table_editor",
            column_config={
                "From Port": st.column_config.TextColumn(
                    "From Port",
                    help="Enter departure port name"
                ),
                "To Port": st.column_config.TextColumn(
                    "To Port",
                    help="Enter arrival port name"
                ),
                "Port Days": st.column_config.NumberColumn(
                    "Port Days",
                    help="Enter number of days in port",
                    min_value=0,
                    max_value=100,
                    step=0.5
                ),
                "Speed (knots)": st.column_config.NumberColumn(
                    "Speed (knots)",
                    help="Enter vessel speed in knots",
                    min_value=0,
                    max_value=30,
                    step=0.1
                ),
                "Fuel Used (mT)": st.column_config.NumberColumn(
                    "Fuel Used (mT)",
                    help="Enter total fuel consumption",
                    min_value=0,
                    step=0.1
                ),
                "Consumption/day (mT)": st.column_config.NumberColumn(
                    "Consumption/day (mT)",
                    help="Enter daily fuel consumption",
                    min_value=0,
                    step=0.1
                )
            }
        )
        
        # Update session state with edited data
        st.session_state.port_table_data = edited_df.values.tolist()

    # Map display
    with right_col:
        if len(st.session_state.port_table_data) >= 2:
            ports = [row[0] for row in st.session_state.port_table_data if row[0]] + \
                   [st.session_state.port_table_data[-1][1]] if st.session_state.port_table_data[-1][1] else []
            
            if all(ports):
                m = plot_route(ports)
            else:
                m = folium.Map(location=[0, 0], zoom_start=2)
        else:
            m = folium.Map(location=[0, 0], zoom_start=2)
        
        st_folium(m, width=None, height=400)

    # Project CII calculations
    if st.button('Project CII'):
        if len(st.session_state.port_table_data) >= 2:
            # Calculate total new distance
            total_new_distance = sum(
                route_distance(row[0], row[1]) 
                for row in st.session_state.port_table_data 
                if row[0] and row[1]
            )
            st.write(f"Total new distance: {total_new_distance:,.2f} nautical miles")

            # Get existing values from session state
            total_existing_distance = st.session_state.cii_data.get('total_distance', 0)
            co2_emission = st.session_state.cii_data.get('co2_emission', 0)
            capacity = st.session_state.cii_data.get('capacity', 0)

            if total_existing_distance is not None and capacity > 0:
                # Calculate projected AER
                try:
                    total_speed_hours = sum(row[3] for row in st.session_state.port_table_data if row[3] is not None) * 24
                    total_fuel = sum(row[4] for row in st.session_state.port_table_data if row[4] is not None)
                    
                    if total_speed_hours > 0:
                        projected_aer = (
                            co2_emission + 
                            (total_new_distance / total_speed_hours) * 
                            total_fuel * 3.114
                        ) * 1000000 / ((total_existing_distance + total_new_distance) * capacity)

                        required_cii = st.session_state.cii_data.get('required_cii', 0)
                        projected_cii_rating = calculate_cii_rating(projected_aer, required_cii)

                        st.session_state.projected_aer = projected_aer
                        st.session_state.projected_cii_rating = projected_cii_rating

                        # Display projected results
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric('Projected AER', f'{projected_aer:.4f}')
                        with col2:
                            st.metric('Projected CII Rating', projected_cii_rating)
                    else:
                        st.error("Please enter valid speed values for all routes.")
                except Exception as e:
                    st.error(f"Error calculating projections: {str(e)}")
            else:
                st.error("Missing vessel data for CII projections. Please calculate current CII first.")
        else:
            st.warning("Please add at least one complete route to calculate projections.")

if __name__ == '__main__':
    main()
