import sys
from dotenv import load_dotenv
from pathlib import Path
import streamlit as st
import pandas as pd
sys.path.append(str(Path(__file__).resolve().parent.parent))
from scripts.utils import get_engine

engine = get_engine()


query = """
    SELECT latitude, longitude
    FROM Business
"""

locations = pd.read_sql(query, engine, columns=['latitude', 'longitude'])


st.map(locations)