import os
import json
import pandas as pd
from collections import deque
import streamlit as st
import numpy as np
import pydeck as pdk
import altair as alt
from kafka import KafkaConsumer
from collections import deque
from streamlit_autorefresh import st_autorefresh

# Global variable
TOPIC = "final-data"
BOOTSTRAP_SERVER = "kafka:9092"
MAX_DATA = 20 

#Streamlit Setup
st.set_page_config("Global Radiation Monitor", layout="wide")
st.title("Global Radiation Monitor Dashboard")

# SIDEBAR
st.sidebar.header("Refresh Settings")
refresh_secs = st.sidebar.slider("Auto-refresh every (seconds)", 1, 30, 5)

# For Auto-refresh the dashboard
st_autorefresh(interval=refresh_secs * 1000, key="auto")

# Kafka Setup
if "consumer" not in st.session_state:
   st.session_state.consumer = KafkaConsumer(
   TOPIC,
   bootstrap_servers=[BOOTSTRAP_SERVER],
   auto_offset_reset="earliest",
   enable_auto_commit=True,
   value_deserializer=lambda x: json.loads(x.decode("utf-8")),)



if "records" not in st.session_state:
   st.session_state.records = deque(maxlen=MAX_DATA)

consumer = st.session_state.consumer
record_buffer = st.session_state.records

# Poll Kafka and only keep messages that have all required fields
new_batches = consumer.poll(timeout_ms=100)
for tp, msgs in new_batches.items():
   for msg in msgs:
       record = msg.value
       # Only include well-formed records with essential keys
       if all(key in record and record[key] not in [None, ""] for key in ["Latitude", "Longitude", "Value"]):
           record_buffer.appendleft(record)


if not record_buffer:
   st.warning("Waiting for data from Kafka topic.")
   st.stop()

# DataFrame Construction
df = pd.DataFrame(list(record_buffer))


# Normalize column names for latitude, longitude, time
colmap = {col.lower(): col for col in df.columns}
lat_key = colmap.get("latitude") or colmap.get("lat")
lon_key = colmap.get("longitude") or colmap.get("lon")
time_key = colmap.get("captured time") or colmap.get("timestamp")
df["latitude"] = pd.to_numeric(df.get(lat_key), errors="coerce")
df["longitude"] = pd.to_numeric(df.get(lon_key), errors="coerce")
df["captured_time"] = pd.to_datetime(df.get(time_key), errors="coerce")


# Handling radiation value
if "Value" in df.columns:
   df["average"] = pd.to_numeric(df["Value"], errors="coerce")
elif "value" in df.columns:
   df["average"] = pd.to_numeric(df["value"], errors="coerce")
elif "average" in df.columns:
   df["average"] = pd.to_numeric(df["average"], errors="coerce")
else:
   df["average"] = None


# Drop records missing key fields
df = df.dropna(subset=["latitude", "longitude", "average"])
if df.empty:
   st.warning("Data received but missing required fields (latitude, longitude, value).")
   st.stop()


# Visualization Setup

# Assign color based on alert
def alert_colour(alert):
   alert = str(alert).strip().lower()
   if "low" in alert:
       return [0, 200, 0] # Green
   elif "moderate" in alert or "modrate" in alert:
       return [255, 140, 0] # Orange
   elif "high" in alert:
       return [220, 20, 60] # Red
   return [150, 150, 150] # Gray

df["alert"] = df.get("alert", pd.Series([""] * len(df))).fillna("")
df["color"] = df["alert"].apply(alert_colour)

# Grouping by rough region
df["latitude"] = df["latitude"].round(1)
df["longitude"] = df["longitude"].round(1)

# Add light jitter for spreading points inside a country
jitter_strength = 1  # 100 kms 
df["latitude"] += np.random.uniform(-jitter_strength, jitter_strength, size=len(df))
df["longitude"] += np.random.uniform(-jitter_strength, jitter_strength, size=len(df))

# Fix radius of circle 
df["fixed_radius"] = 70000  # ~70km in pixels at global view

# Map Rendering
view_state = pdk.ViewState(latitude=20, longitude=0, zoom=1.3, pitch=0)
scatter_layer = pdk.Layer(
   "ScatterplotLayer",
   data=df,
   get_position=["longitude", "latitude"],
   get_fill_color="color",
   get_radius="fixed_radius",
   pickable=True,
   stroked=True,
   get_line_color=[0, 0, 0],
   line_width_min_pixels=1,)


text_layer = pdk.Layer(
   "TextLayer",
   data=df,
   get_position=["longitude", "latitude"],
   get_text="Value" if "Value" in df.columns else "average",
   get_color=[0, 0, 0],
   get_size=14,
   get_alignment_baseline='"middle"',
   get_text_anchor='"middle"',)


deck = pdk.Deck(
   layers=[scatter_layer, text_layer],
   initial_view_state=view_state,
   map_style="mapbox://styles/mapbox/light-v9",
   tooltip={"text": (
           "Country: {country}\n"
           "Unit: {Unit}\n"
           "Value: {Value}\n"
           "Alert: {alert}")},)
st.pydeck_chart(deck)

from collections import deque
# Preset of coountries to show in the dropdown
preferred_countries = ["Germany", "Japan", "United States", "Netherlands", "France", "United Kingdom", "India", "Canada", "Australia","Italy", "Spain", "China", "Russia", "Brazil", "South Korea", "Sweden", "Switzerland", "Mexico", "Norway","Turkey", "Poland", "South Africa", "Indonesia", "New Zealand"]

# Initialize selection only once
if "selected_country" not in st.session_state:
   st.session_state.selected_country = preferred_countries[0]  # default to first


# Always show full preferred list
selected_country = st.sidebar.selectbox("Select Country for Rolling Chart",options=preferred_countries,index=preferred_countries.index(st.session_state.selected_country)
                                        if st.session_state.selected_country in preferred_countries else 0)


# If selection changed
if selected_country != st.session_state.selected_country:
   st.session_state.selected_country = selected_country
   st.session_state.country_points = deque(maxlen=30)
   st.session_state.global_points = deque(maxlen=30)
   st.session_state.seen_event_ids = set()


# Live Rolling Country vs Global Line Chart
st.subheader("Live Rolling Country Average vs Global Average (Last 30 Points)")
country_name = st.session_state.selected_country
filtered_df = df[df["country"] == country_name].copy()


# Always, even if empty now
if "country_points" not in st.session_state:
   st.session_state.country_points = deque(maxlen=30)
if "global_points" not in st.session_state:
   st.session_state.global_points = deque(maxlen=30)
if "seen_event_ids" not in st.session_state:
   st.session_state.seen_event_ids = set()


# If new data exists.
if not filtered_df.empty:
   if "Captured Time" in filtered_df.columns:
       filtered_df["captured_time"] = pd.to_datetime(filtered_df["Captured Time"], errors="coerce")
   elif "captured_time" in filtered_df.columns:
       filtered_df["captured_time"] = pd.to_datetime(filtered_df["captured_time"], errors="coerce")
   else:
       filtered_df["captured_time"] = pd.NaT
   required_columns = [col for col in ["captured_time", "average", "global_average"] if col in filtered_df.columns]
   filtered_df = filtered_df.dropna(subset=required_columns)

   for _, row in filtered_df.iterrows():
       event_id = row.get("event_id")
       if event_id and event_id not in st.session_state.seen_event_ids:
           st.session_state.country_points.append((row.get("captured_time"), row.get("average")))
           st.session_state.global_points.append((row.get("captured_time"), row.get("global_average")))
           st.session_state.seen_event_ids.add(event_id)
#else:
 #  st.info(f"No new data for **{country_name}**. Showing last 30 points...")


# Always render the chart using current state
if st.session_state.country_points and st.session_state.global_points:
   rolling_df = pd.DataFrame({
       "captured_time": [t[0] for t in st.session_state.country_points],
       "country_average": [t[1] for t in st.session_state.country_points],
       "global_average": [t[1] for t in st.session_state.global_points]
   })


   melted = rolling_df.melt(
       id_vars="captured_time",
       value_vars=["country_average", "global_average"],
       var_name="Metric",
       value_name="Value"
   )


   line_chart = alt.Chart(melted).mark_line(point=True).encode(
       x=alt.X("captured_time:T", title="Captured Time"),
       y=alt.Y("Value:Q", title="Radiation Level (CPM)"),
       color=alt.Color("Metric:N", title="Metric"),
       tooltip=["captured_time:T", "Metric:N", "Value:Q"]
   ).properties(width=800, height=400).interactive()


   st.altair_chart(line_chart, use_container_width=True)
else:
   st.warning(f"No data yet to show chart for **{country_name}**.")

   # Trend Highlight Chart
st.subheader("Trend Highlight Overlay (Global & Country Trend)")

# Build trend_df from the last 30 rolling data points
trend_df = pd.DataFrame({
    "captured_time": [t[0] for t in st.session_state.country_points],
    "country_average": [t[1] for t in st.session_state.country_points],
    "global_average": [t[1] for t in st.session_state.global_points]
})

# Fill country/global trend from the main df
trend_df["country_trend"] = None
trend_df["global_trend"] = None

for i, t in enumerate(trend_df["captured_time"]):
    match = df[df["captured_time"] == t]
    if not match.empty:
        trend_df.at[i, "country_trend"] = match.iloc[0].get("country_trend", "no trend")
        trend_df.at[i, "global_trend"] = match.iloc[0].get("global_trend", "no trend")

# Global points
global_points = trend_df[["captured_time", "global_average", "global_trend"]].copy()
global_points["Metric"] = "Global"
global_points["Value"] = global_points["global_average"]
global_points["Trend"] = global_points["global_trend"]
global_points["Shape"] = "circle"

# Country points
country_points = trend_df[["captured_time", "country_average", "country_trend"]].copy()
country_points["Metric"] = "Country"
country_points["Value"] = country_points["country_average"]
country_points["Trend"] = country_points["country_trend"]
country_points["Shape"] = "square"

# Combine both
combined = pd.concat([global_points, country_points], ignore_index=True)

# Assign color by trend direction
def trend_color(t):
    t = str(t).lower()
    if "upward" in t:
        return "red"
    elif "downward" in t:
        return "green"
    return "gray"

combined["Color"] = combined["Trend"].apply(trend_color)

# Create Altair chart
highlight_chart = alt.Chart(combined).mark_point(filled=True, size=100).encode(
    x=alt.X("captured_time:T", title="Captured Time"),
    y=alt.Y("Value:Q", title="Radiation Level (CPM)"),
    color=alt.Color("Color:N", scale=None, legend=alt.Legend(title="Trend Direction")),
    shape=alt.Shape("Metric:N", legend=alt.Legend(title="Metric Source")),
    tooltip=["captured_time:T", "Metric:N", "Value:Q", "Trend:N"]
).properties(
    width=800,
    height=300
).interactive()

st.altair_chart(highlight_chart, use_container_width=True)

# Data Table
order = [ "country", "Unit", "Value", "alert", "average", "country_trend",
   "global_average", "global_trend", "captured_time", "Latitude", "Longitude",
   "Device ID", "event_id"]
df = df.reindex(columns=[col for col in order if col in df.columns])
df = df.dropna(axis=1, how="all")
df = df.sort_values("captured_time", ascending=False)
df = df.reset_index(drop=True)

st.subheader(f"Latest Radiation Records (showing {len(df)} records)")
st.dataframe(df)
