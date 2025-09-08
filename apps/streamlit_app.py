import streamlit as st
from apps.climate_map import render_map
from apps.climate_map_timeslider import render_timeslider

PAGES = {
    "Map": render_map,
    "Time Slider": render_timeslider,
}

choice = st.sidebar.radio("Page", list(PAGES.keys()))
PAGES[choice]()
