from datetime import timedelta

import streamlit as st
from streamlit_folium import st_folium

from apps.utils.maps import load_data, make_timeslider_map


def render_timeslider():
    st.title("Climate Map - Time Slider")
    days = st.slider("Days", 1, 30, 7)
    end = st.date_input("End Date")
    start = end - timedelta(days=days - 1)
    metric = st.selectbox("Metric", ["tavg_c", "prcp_mm", "hdd18", "cdd18", "gdd10_30"])
    df = load_data(start, end)
    m = make_timeslider_map(df, metric)
    st_folium(m, width=700, height=500)
    st.dataframe(df)
