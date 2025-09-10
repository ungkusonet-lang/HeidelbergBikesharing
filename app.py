import os, json, time, uuid, io, math, datetime, textwrap, requests, pathlib
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium
import folium
from folium.plugins import Draw, HeatMap
from shapely.geometry import LineString
from geopy.distance import geodesic

# =========================
# App Configuration
# =========================
APP_TITLE = "Sharing-Bike Routes Survey"
APP_DESC  = "Draw your ride with a few clicks—routes will snap to the street network (cycling)."
DEFAULT_CENTER = (53.3498, -6.2603)  # Dublin
DEFAULT_ZOOM = 13
OSRM_BASE = "https://router.project-osrm.org"  # demo server; OK for MVP
SHEET_NAME = "Bike Routes Survey"
SERVICE_ACCOUNT_JSON = "/content/service_account.json"  # optional
LOCAL_DB = "/content/routes_db.csv"  # fallback store

# Minimal sample Nextbike (Heidelberg) stations as fallback
SAMPLE_STATIONS = [
    {"name": "Hbf Süd", "lat": 49.4029, "lon": 8.6726},
    {"name": "Bismarckplatz", "lat": 49.4108, "lon": 8.6900},
    {"name": "Universitätsplatz", "lat": 49.4148, "lon": 8.7078},
]

# =========================
# Storage (Google Sheets OR CSV)
# =========================
def _gspread_client():
    if not os.path.exists(SERVICE_ACCOUNT_JSON):
        return None, None
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        scopes = ["https://www.googleapis.com/auth/spreadsheets",
                  "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=scopes)
        gc = gspread.authorize(creds)
        try:
            sh = gc.open(SHEET_NAME)
        except gspread.SpreadsheetNotFound:
            sh = gc.create(SHEET_NAME)
        ws = sh.sheet1
        headers = ["timestamp_utc","respondent_id","age_group","role",
                   "commute_freq","route_index","route_distance_m",
                   "start_lat","start_lon","end_lat","end_lon",
                   "route_geojson","issues","suggestions","gbfs_url"]
        existing = ws.get_all_values()
        if not existing:
            ws.append_row(headers)
        return gc, ws
    except Exception as e:
        st.sidebar.warning(f"Sheets disabled (auth error): {e}")
        return None, None

def append_row(row_dict):
    """Write one route to Sheets or CSV."""
    gc, ws = st.session_state.get("gspread", (None, None))
    if ws is None:
        # CSV fallback
        df = pd.DataFrame([row_dict])
        if os.path.exists(LOCAL_DB):
            df0 = pd.read_csv(LOCAL_DB)
            df = pd.concat([df0, df], ignore_index=True)
        df.to_csv(LOCAL_DB, index=False)
    else:
        ordered = [row_dict.get(k,"") for k in
                   ["timestamp_utc","respondent_id","age_group","role","commute_freq",
                    "route_index","route_distance_m","start_lat","start_lon","end_lat",
                    "end_lon","route_geojson","issues","suggestions","gbfs_url"]]
        ws.append_row(ordered)

def load_all_routes():
    """Return DataFrame of all saved routes."""
    _, ws = st.session_state.get("gspread", (None, None))
    if ws is None:
        if os.path.exists(LOCAL_DB):
            return pd.read_csv(LOCAL_DB)
        else:
            return pd.DataFrame()
    else:
        vals = ws.get_all_records()
        return pd.DataFrame(vals)

# =========================
# Nextbike stations (GBFS or CSV)
# =========================
def fetch_gbfs_stations(gbfs_station_info_url: str):
    try:
        r = requests.get(gbfs_station_info_url, timeout=15)
        r.raise_for_status()
        data = r.json()
        stations = data.get("data", {}).get("stations", [])
        out = []
        for s in stations:
            name = s.get("name") or s.get("station_name") or "Station"
            lat = s.get("lat") or s.get("latitude")
            lon = s.get("lon") or s.get("longitude")
            if lat is not None and lon is not None:
                out.append({"name": name, "lat": float(lat), "lon": float(lon)})
        return out
    except Exception as e:
        st.warning(f"Could not load GBFS stations: {e}")
        return []

def parse_stations_csv(file):
    try:
        df = pd.read_csv(file)
        assert {"name","lat","lon"}.issubset(df.columns)
        return df[["name","lat","lon"]].to_dict(orient="records")
    except Exception as e:
        st.warning(f"Station CSV must have columns name,lat,lon. Error: {e}")
        return []

# =========================
# OSRM route snapping
# =========================
def osrm_snap_route(latlon_points):
    """
    latlon_points: list[(lat,lon)] as rough polyline from user.
    Returns (geojson_line, distance_m) snapped to roads using cycling profile.
    """
    if len(latlon_points) < 2:
        return None, 0.0, "Need at least start & end."
    # Build coordinates string as lon,lat;lon,lat...
    coords = ";".join([f"{lon:.6f},{lat:.6f}" for lat,lat in latlon_points])
    url = f"{OSRM_BASE}/route/v1/cycling/{coords}"
    params = {"overview":"full","geometries":"geojson","steps":"false"}
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        routes = data.get("routes", [])
        if not routes:
            return None, 0.0, "No route found."
        geom = routes[0]["geometry"]  # GeoJSON LineString
        # distance from OSRM if available
        dist_m = routes[0].get("distance")
        if not dist_m:
            # fallback compute geodesic length
            coords = geom["coordinates"]  # [lon,lat]
            d = 0.0
            for i in range(1, len(coords)):
                a = (coords[i-1][1], coords[i-1][0])
                b = (coords[i][1], coords[i][0])
                d += geodesic(a, b).meters
            dist_m = d
        return geom, float(dist_m), None
    except Exception as e:
        return None, 0.0, f"OSRM error: {e}"

# =========================
# Mapping helpers
# =========================
def make_map(center, zoom, stations, drawn_routes=None, snapped_preview=None, all_routes=None):
    m = folium.Map(location=center, zoom_start=zoom, tiles="cartodbpositron")
    # stations
    for s in stations:
        folium.CircleMarker(location=(s["lat"], s["lon"]), radius=5, color="#1f77b4",
                            fill=True, fill_opacity=0.9, tooltip=s["name"]).add_to(m)
    # draw control for polylines (user freehand)
    Draw(
        export=False,
        draw_options={
            "polyline": True, "polygon": False, "circle": False, "rectangle": False, "marker": False,
            "circlemarker": False
        },
        edit_options={"edit": True, "remove": True}
    ).add_to(m)
    # user's session routes
    if drawn_routes:
        for i, gj in enumerate(drawn_routes, 1):
            folium.GeoJson(gj, name=f"My Route {i}",
                           tooltip=f"My Route {i}").add_to(m)
    # preview snapped route in a different style
    if snapped_preview:
        folium.GeoJson(snapped_preview, name="Snapped Preview",
            style_function=lambda x: {"color":"#e6550d","weight":5,"opacity":0.9}
        ).add_to(m)
    # overview routes (all respondents)
    if all_routes is not None and not all_routes.empty:
        for _, row in all_routes.iterrows():
            try:
                gj = json.loads(row["route_geojson"])
                folium.GeoJson(gj,
                    style_function=lambda x: {"color":"#31a354","weight":3,"opacity":0.4}
                ).add_to(m)
            except Exception:
                pass
    folium.LayerControl().add_to(m)
    return m

def densify_for_heatmap(geojson_line, step=30):
    """
    Return list of lat/lon points sampled along the line every ~step meters.
    """
    coords = geojson_line["coordinates"]  # [lon,lat]
    pts = []
    for i in range(1, len(coords)):
        a = (coords[i-1][1], coords[i-1][0])
        b = (coords[i][1], coords[i][0])
        seg_m = geodesic(a, b).meters
        n = max(1, int(seg_m // step))
        for k in range(n):
            t = k / n
            lat = a[0] + (b[0]-a[0]) * t
            lon = a[1] + (b[1]-a[1]) * t
            pts.append([lat, lon])
    return pts

def make_overview_heatmap(center, zoom, df):
    m = folium.Map(location=center, zoom_start=zoom, tiles="cartodbpositron")
    heat_points = []
    for _, row in df.iterrows():
        try:
            gj = json.loads(row["route_geojson"])
            heat_points += densify_for_heatmap(gj, step=40)
        except Exception:
            pass
    if heat_points:
        HeatMap(heat_points, radius=12, blur=18, min_opacity=0.3).add_to(m)
    return m

# =========================
# UI
# =========================
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)
st.write(APP_DESC)

st.markdown("""
## Sharing-Bike Routes Survey Platform

Welcome to the Sharing-Bike Routes Survey platform! This tool allows you to share your experiences and contribute valuable data about your sharing-bike routes, helping to improve cycling infrastructure and services.

**Purpose:**

The goal of this survey is to collect information about the routes people take when using sharing bikes. By drawing your routes and providing feedback on your experiences, you help urban planners, bike operators, and researchers understand usage patterns, identify popular routes, pinpoint problem areas (like safety concerns or lack of parking), and gather suggestions for improvements.

**How to Use the Platform:**

1.  **Access the App:** Open the public URL provided (e.g., the ngrok link you generated).
2.  **Provide Participant Information (Optional but Recommended):** In the sidebar on the left, you can provide some basic information about yourself (age group, role, how often you ride sharing-bikes). This information helps us understand the different needs and experiences of various user groups. You can also share any general issues you encounter or suggestions you have.
3.  **Draw Your Route:**
    *   On the map, use the drawing tools (the pencil icon) to draw the route you took or would take.
    *   Simply click on the map to add points along your route. You don't need to be perfectly precise; a rough outline is sufficient.
4.  **Snap to Streets:** After drawing your route, click the "**Snap last drawing to streets**" button in the control panel on the right. The platform will use routing data to snap your rough drawing to the actual street network (optimized for cycling) and show you a preview. It will also estimate the distance of the snapped route.
5.  **Add to My Routes:** If the snapped route looks correct, click "**Add snapped to my routes**". You can draw and add multiple routes if you wish.
6.  **Review and Clear:**
    *   "**Clear my routes**" removes all routes you have added in your current session.
    *   "**Clear preview**" removes the currently displayed snapped route preview.
7.  **Show Overview (Optional):** You can toggle "**Show all respondents' routes overlay**" in the sidebar to see a heatmap or individual lines representing routes submitted by other participants (depending on the view implemented).
8.  **Consent and Submit:** Before submitting, please check the box stating "**I consent to storage and processing of my submission.**". This is required to share your data. Once you consent and have added at least one route, click "**Submit all my routes**".

**Your data is valuable!** By sharing your routes and feedback, you contribute directly to making sharing-bike systems better for everyone. Thank you for participating!

*(Note: The map may also show locations of sharing-bike stations if the data is available.)*
""")


# Initialize Sheets client (if available)
if "gspread" not in st.session_state:
    st.session_state["gspread"] = _gspread_client()

with st.sidebar:
    st.subheader("Survey — Participant")
    if "respondent_id" not in st.session_state:
        st.session_state["respondent_id"] = str(uuid.uuid4())
    st.text_input("Respondent ID", value=st.session_state["respondent_id"], disabled=True)

    age = st.selectbox("Age group*", ["<18","18–24","25–34","35–44","45–54","55–64","65+"])
    role = st.selectbox("Role*", ["Resident","Commuter","Business Owner","Student","Visitor","Other"])
    commute_freq = st.selectbox("How often do you ride sharing-bikes?",
                                ["Daily","Weekly","Monthly","Rarely"])
    issues = st.multiselect("Issues you experience (multi-select)",
                            ["Safety","Parking","Connectivity","Crowding","Maintenance","Wayfinding","Accessibility","Other"])
    suggestions = st.text_area("Any suggestions?", height=80)
    consent = st.checkbox("I consent to storage and processing of my submission.*", value=False)

    st.divider()
    st.subheader("Map Sources")
    gbfs_url = st.text_input("GBFS station_information URL (optional)",
                             help="Paste a Nextbike GBFS station_information.json URL. If empty, use CSV upload or fallback sample.")
    stations_csv = st.file_uploader("…or upload stations CSV (name,lat,lon)", type=["csv"])
    center_lat = st.number_input("Map center lat", value=float(DEFAULT_CENTER[0]), format="%.6f")
    center_lon = st.number_input("Map center lon", value=float(DEFAULT_CENTER[1]), format="%.6f")
    zoom = st.slider("Zoom", 5, 18, value=DEFAULT_ZOOM)

    st.divider()
    use_overview = st.toggle("Show all respondents' routes overlay", value=False)

# load stations
stations = []
if gbfs_url:
    stations = fetch_gbfs_stations(gbfs_url)
elif stations_csv is not None:
    stations = parse_stations_csv(stations_csv)
if not stations:
    stations = SAMPLE_STATIONS  # small fallback so the map is not empty

# session routes
if "my_routes" not in st.session_state:
    st.session_state["my_routes"] = []   # list of GeoJSON LineStrings
if "preview_route" not in st.session_state:
    st.session_state["preview_route"] = None
if "preview_distance" not in st.session_state:
    st.session_state["preview_distance"] = 0.0

col_map, col_controls = st.columns([7,5], gap="large")

with col_map:
    # overview DF
    all_df = load_all_routes() if use_overview else pd.DataFrame()
    m = make_map(center=(center_lat, center_lon), zoom=zoom, stations=stations,
                 drawn_routes=st.session_state["my_routes"],
                 snapped_preview=st.session_state["preview_route"],
                 all_routes=all_df)
    map_state = st_folium(m, height=620, width=None, key="map")

with col_controls:
    st.subheader("Draw & Snap")
    st.caption("Use the map’s draw tool (polyline). A few vertices are enough. Then click **Snap to streets**.")
    # Try to capture the last drawn geometry from streamlit-folium's return dict
    last_drawn = None
    if map_state: # Check if map_state is not None
        for k in ["last_active_drawing", "last_drawing", "last_drawn"]:
            if k in map_state and map_state[k]:
                last_drawn = map_state[k]
                break
        if not last_drawn and "all_drawings" in map_state and map_state["all_drawings"]:
            last_drawn = map_state["all_drawings"][-1]

    if st.button("Snap last drawing to streets"):
        if not last_drawn:
            st.warning("Draw a polyline first (use the pencil icon on the map).")
        else:
            try:
                # Expect a GeoJSON-like Feature or Geometry
                if "geometry" in last_drawn:
                    geom = last_drawn["geometry"]
                else:
                    geom = last_drawn
                if geom.get("type") != "LineString":
                    st.warning("Please draw a LineString (polyline).")
                else:
                    # Convert coordinates [lat,lon] or [lon,lat] robustly to [(lat,lon)]
                    coords = geom.get("coordinates", [])
                    # Heuristic: folium Draw returns [lat, lon]? Usually it's [lat, lng] in map events,
                    # but draw plugin serializes as [lng, lat] GeoJSON. We try both.
                    latlon_pts = []
                    for c in coords:
                        if abs(c[0]) > 90:  # then [lon, lat]
                            latlon_pts.append((float(c[1]), float(c[0])))
                        else:               # assume [lat, lon]
                            latlon_pts.append((float(c[0]), float(c[1])))
                    snapped_gj, dist_m, err = osrm_snap_route(latlon_pts)
                    if err:
                        st.error(err)
                    else:
                        st.session_state["preview_route"] = {"type":"Feature","geometry":snapped_gj,"properties":{}}
                        st.session_state["preview_distance"] = dist_m
                        st.success(f"Snapped distance ≈ {dist_m/1000:.2f} km")
            except Exception as e:
                st.error(f"Could not parse drawing: {e}")

    c1, c2, c3 = st.columns([1,1,1])
    with c1:
        if st.button("Add snapped to my routes"):
            if st.session_state["preview_route"] is None:
                st.warning("Nothing to add—snap a route first.")
            else:
                st.session_state["my_routes"].append(st.session_state["preview_route"])
                st.session_state["preview_route"] = None
                st.session_state["preview_distance"] = 0.0
                st.toast("Added.")
    with c2:
        if st.button("Clear my routes"):
            st.session_state["my_routes"] = []
            st.session_state["preview_route"] = None
            st.session_state["preview_distance"] = 0.0
    with c3:
        if st.button("Clear preview"):
            st.session_state["preview_route"] = None
            st.session_state["preview_distance"] = 0.0

    st.markdown("—")
    st.write(f"**My routes:** {len(st.session_state['my_routes'])}")

    st.subheader("Submit")
    if st.button("Submit all my routes"):
        if not consent:
            st.error("You must consent before submitting.")
        elif len(st.session_state["my_routes"]) == 0:
            st.warning("Add at least one route.")
        else:
            rid = st.session_state["respondent_id"]
            timestamp = datetime.datetime.utcnow().isoformat()
            for i, feat in enumerate(st.session_state["my_routes"]):
                gj = feat["geometry"] if "geometry" in feat else feat
                coords = gj.get("coordinates", [])
                if not coords:
                    continue
                start_lat, start_lon = coords[0][1], coords[0][0]
                end_lat, end_lon = coords[-1][1], coords[-1][0]
                # recompute distance for safety
                d = 0.0
                for k in range(1, len(coords)):
                    a = (coords[k-1][1], coords[k-1][0])
                    b = (coords[k][1], coords[k][0])
                    d += geodesic(a, b).meters
                row = {
                    "timestamp_utc": timestamp,
                    "respondent_id": rid,
                    "age_group": age,
                    "role": role,
                    "commute_freq": commute_freq,
                    "route_index": i+1,
                    "route_distance_m": round(d, 1),
                    "start_lat": round(start_lat, 6),
                    "start_lon": round(start_lon, 6),
                    "end_lat": round(end_lat, 6),
                    "end_lon": round(end_lon, 6),
                    "route_geojson": json.dumps(gj),
                    "issues": ";".join(issues),
                    "suggestions": suggestions.strip(),
                    "gbfs_url": gbfs_url or ("csv" if stations_csv else "sample"),
                }
                append_row(row)
            st.success("Thanks! Your routes have been sub")
