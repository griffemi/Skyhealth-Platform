import pandas as pd
import duckdb
import folium
from folium.plugins import TimestampedGeoJson
from skyhealth.config import settings, export_path

DB_PATH = settings.dbt_duckdb
LOCATIONS_CSV = 'data/locations.csv'
EXPORT = export_path('climate_silver_daily_features')


def load_data(start, end):
    con = duckdb.connect(DB_PATH)
    df = con.execute(
        f"SELECT * FROM read_parquet('{EXPORT}/*') WHERE d BETWEEN '{start}' AND '{end}'"
    ).df()
    locations = pd.read_csv(LOCATIONS_CSV)
    return df.merge(locations, on='location_id')


def make_map(df, metric):
    m = folium.Map()
    for _, r in df.iterrows():
        folium.Marker([r['latitude'], r['longitude']], popup=f"{metric}: {r[metric]}").add_to(m)
    return m


def make_timeslider_map(df, metric):
    feats = []
    for _, r in df.iterrows():
        feats.append(
            {
                'type': 'Feature',
                'geometry': {'type': 'Point', 'coordinates': [r['longitude'], r['latitude']]},
                'properties': {'time': r['d'].isoformat(), 'popup': f"{metric}: {r[metric]}"},
            }
        )
    m = folium.Map()
    TimestampedGeoJson({'type': 'FeatureCollection', 'features': feats}).add_to(m)
    return m
