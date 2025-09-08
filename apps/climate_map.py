import streamlit as st
from streamlit_folium import st_folium

from apps.utils.maps import load_data, make_map


def render_map():
    st.title("Climate Map")
    date = st.date_input("Date")
    metric = st.selectbox("Metric", ["tavg_c", "prcp_mm", "hdd18", "cdd18", "gdd10_30"])
    df = load_data(date, date)
    m = make_map(df, metric)
    st_folium(m, width=700, height=500)
    st.dataframe(df)
