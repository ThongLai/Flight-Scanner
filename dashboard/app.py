import streamlit as st
import pandas as pd
from datetime import datetime
import json
import sys
from pathlib import Path
import re
import plotly.graph_objects as go
import plotly.express as px
import pydeck as pdk





# Make src/ importable
SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))

from ml.data_loader import load_all_calendars, load_all_itineraries
from ml.predict import predict_flight
from ml.optimise import find_pareto_flights

ROOT = Path(__file__).resolve().parents[1]
MODELS_DIR = ROOT / "models"

st.set_page_config(page_title="Flight Price Prediction", layout="wide", page_icon="✈")

# ---- Shared plot styling ----
ACCENT = "#2E86AB"
TEMPLATE = "plotly_white"

def styled(fig, height=380):
    fig.update_layout(template=TEMPLATE, height=height,
                      margin=dict(l=40, r=20, t=50, b=40),
                      title_font_size=15, dragmode='pan')
    return fig
PLOT_CONFIG = {"scrollZoom": True}

DATE_FMT = "%d/%m/%Y"
DATETIME_FMT = "%d/%m/%Y %H:%M:%S"


def fmt_dt(value, with_time=True):
    """Format any date/datetime/ISO-string as dd/mm/yyyy [HH:MM:SS]."""
    if value is None or value == "" or value == "unknown":
        return "unknown"
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return str(value)
    return ts.strftime(DATETIME_FMT if with_time else DATE_FMT)



# ---- Data loaders (cached) ----
@st.cache_data(ttl=3600, show_spinner="Loading calendar data...", show_time=True)
def load_calendar():
    cols = ["collected_at", "departure_date", "origin",
            "destination", "price"]
    return load_all_calendars(columns=cols)


@st.cache_data(ttl=3600, show_spinner="Loading itineraries...")
def load_itineraries():
    cols = ["origin_code", "dest_code", "price_raw", "duration_minutes",
            "stop_count", "carrier_names", "is_direct", "search_date",
            "segment_route", "departure"]
    return load_all_itineraries(columns=cols)


@st.cache_data(ttl=3600, show_spinner="Loading metrics...", show_time=True)
def load_metrics():
    
    path = MODELS_DIR / "metrics.json"
    return json.loads(path.read_text()) if path.exists() else {}


@st.cache_data(ttl=3600, show_spinner="Loading feature importance...", show_time=True)
def load_feature_importance():
    path = MODELS_DIR / "feature_importance.csv"
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner="Finding Pareto flights...", show_time=True)
def find_pareto_flights_and_cache(origin, dest, search_date):
    pareto = find_pareto_flights(origin, dest, search_date)
    return pareto


@st.cache_resource(ttl=3600, show_spinner="Predicting flight...", show_time=True)
def predict_flight_and_cache(route: str, departure_date: str) -> dict:
    return predict_flight(route, departure_date)


@st.cache_data(ttl=3600, show_spinner=False)
def _predict_cached(route: str, date: str) -> dict:
    """Cache predictions so repeated journeys don't recompute."""
    try:
        return predict_flight(route, date)
    except Exception:
        return {}


AIRPORTS = {
    # Origins
    "LHR": (51.4700, -0.4543), "MAN": (53.3537, -2.2750),
    # Destinations
    "SGN": (10.8188, 106.6520), "HAN": (21.2212, 105.8072),
    "BKK": (13.6900, 100.7501),
    # Transit hubs (major)
    "ZRH": (47.4647, 8.5492), "SIN": (1.3644, 103.9915),
    "AUH": (24.4330, 54.6511), "VIE": (48.1103, 16.5697),
    "HKG": (22.3080, 113.9185), "DXB": (25.2532, 55.3657),
    "DOH": (25.2731, 51.6080), "IST": (41.2753, 28.7519),
    "FRA": (50.0379, 8.5622), "CDG": (49.0097, 2.5479),
    "AMS": (52.3105, 4.7683), "HEL": (60.3172, 24.9633),
    "KUL": (2.7456, 101.7099), "CGK": (-6.1256, 106.6558),
    "DEL": (28.5562, 77.1000), "BOM": (19.0896, 72.8656),
    "MUC": (48.3538, 11.7861),
    # Budget / secondary hubs (from your DEBUG data)
    "BGY": (45.6739, 9.7042),    # Milan Bergamo
    "MXP": (45.6306, 8.7281),    # Milan Malpensa
    "BVA": (49.4544, 2.1128),    # Paris Beauvais
    "CRL": (50.4592, 4.4538),    # Brussels Charleroi
    "DUB": (53.4213, -6.2701),   # Dublin
    "ASB": (37.9687, 58.3610),   # Ashgabat
    "STN": (51.8850, 0.2350),    # London Stansted
    "LGW": (51.1537, -0.1821),   # London Gatwick
    "BCN": (41.2971, 2.0785),    # Barcelona
    "MAD": (40.4719, -3.5626),   # Madrid
    "WAW": (52.1657, 20.9671),   # Warsaw
    "SVO": (55.9726, 37.4146),   # Moscow
    "TAS": (41.2579, 69.2812),   # Tashkent
    "ALA": (43.3521, 77.0405),   # Almaty
    "CAN": (23.3924, 113.2988),  # Guangzhou
    "PVG": (31.1443, 121.8083),  # Shanghai
    "ICN": (37.4602, 126.4407),  # Seoul
    "TPE": (25.0777, 121.2328),  # Taipei
    "KMG": (25.1019, 102.9292),  # Kunming
    "REP": (13.4107, 103.8130),  # Siem Reap
    "PNH": (11.5466, 104.8441),  # Phnom Penh
}






def build_flight_arcs(route_itin: pd.DataFrame, sel_route: str, max_journeys: int = 8):
    """Multi-stop segment_route -> per-hop arc rows, enriched with
    price-drop behaviour, confidence, and stop count."""
    arcs = []
    airports_used = {}

    subset = (route_itin.sort_values("price_raw")
              .drop_duplicates("segment_route")
              .head(max_journeys))

    for row in subset.itertuples():
        stops = re.findall(r"\b[A-Z]{3}\b",
                           str(row.segment_route).upper())

        # Derive departure date for this journey and predict
        dep_date = pd.to_datetime(getattr(row, "departure", None),
                                  errors="coerce")
        pred = {}
        if pd.notna(dep_date):
            pred = _predict_cached(sel_route,
                                   dep_date.strftime("%Y-%m-%d"))

        drop_prob = pred.get("wait_probability")
        drop_txt = (f"{drop_prob * 100:.0f}%"
                    if drop_prob is not None else "n/a")
        outlook = pred.get("action", "n/a")
        confidence = pred.get("confidence", "n/a")
        stop_count = int(getattr(row, "stop_count", 0))

        for a, b in zip(stops, stops[1:]):
            if a not in AIRPORTS or b not in AIRPORTS:
                continue
            s_lat, s_lon = AIRPORTS[a]
            d_lat, d_lon = AIRPORTS[b]
            arcs.append({
                "from": a, "to": b,
                "source_lat": s_lat, "source_lon": s_lon,
                "dest_lat": d_lat, "dest_lon": d_lon,
                "price": f"{row.price_raw:.0f}",
                "carrier": row.carrier_names,
                "journey": " → ".join(stops),
                "stops": stop_count,
                "drop_prob": drop_txt,
                "outlook": outlook,
                "confidence": confidence,
            })
            airports_used[a] = (s_lat, s_lon)
            airports_used[b] = (d_lat, d_lon)

    return pd.DataFrame(arcs), airports_used


def build_flight_map(route_itin, sel_route, selected_layers, max_journeys=8):
    arcs, airports_used = build_flight_arcs(route_itin, sel_route, max_journeys)

    if arcs.empty:
        return None

    # Airport points for scatter + labels
    pts = pd.DataFrame([
        {"code": c, "lat": lat, "lon": lon}
        for c, (lat, lon) in airports_used.items()
    ])

    # Colour arcs: origin blue -> destination pink (your scheme)
    arc_layer = pdk.Layer(
        "ArcLayer", data=arcs,
        get_source_position=["source_lon", "source_lat"],
        get_target_position=["dest_lon", "dest_lat"],
        get_width=3,
        get_source_color=[0, 128, 200, 180],
        get_target_color=[200, 0, 80, 180],
        pickable=True, auto_highlight=True,
    )
    scatter_layer = pdk.Layer(
        "ScatterplotLayer", data=pts,
        get_position=["lon", "lat"],
        get_radius=40000,
        get_fill_color=[46, 134, 171, 200],
        pickable=True,
    )
    text_layer = pdk.Layer(
        "TextLayer", data=pts,
        get_position=["lon", "lat"], get_text="code",
        get_size=14, get_color=[255, 255, 255, 230],
        get_pixel_offset=[0, -12], get_text_anchor="'middle'",
    )

    # Auto centre + zoom (your logic)
    all_lats = pd.concat([arcs["source_lat"], arcs["dest_lat"]])
    all_lons = pd.concat([arcs["source_lon"], arcs["dest_lon"]])
    centre_lat, centre_lon = all_lats.mean(), all_lons.mean()
    max_range = max(all_lats.max() - all_lats.min(),
                    all_lons.max() - all_lons.min())
    zoom = (6 if max_range < 5 else 4 if max_range < 20
            else 3 if max_range < 60 else 2)
    view = pdk.ViewState(latitude=centre_lat, longitude=centre_lon,
                         zoom=zoom, pitch=30)

    layer_map = {
        "Flow lines": arc_layer,
        "Airports": scatter_layer,
        "Labels": text_layer,
    }
    layers = [layer_map[n] for n in selected_layers if n in layer_map]

    return pdk.Deck(
        layers=layers, initial_view_state=view,
        tooltip={
            "html": (
                "<b>{from} → {to}</b><br>"
                "Journey: {journey}<br>"
                "Stops: {stops}<br>"
                "Price: £{price} · {carrier}<br>"
                "<hr style='margin:3px 0'>"
                "Outlook: <b>{outlook}</b><br>"
                "Drop probability: <b>{drop_prob}</b><br>"
                "Confidence: <b>{confidence}</b>"
            ),
            "style": {"backgroundColor": "rgba(0,0,0,0.85)",
                      "color": "white", "font-size": "12px"},
        },
    )







st.title("Flight Price Prediction - Model Performance Dashboard")
st.caption(
    "This dashboard display the performance and analysis of the Multi-Objective Model (MOM) for flight price prediction and optimisation  "
    "\nThe bot chat are available to use on **Telegram**: ***[Flight Scanner Chatbot](https://t.me/tom_flight_scanner_bot)*** by ***[Tom Lai](https://github.com/ThongLai)***   "
    
    "\n**Models:** CNN (price behaviour)  "
    "\n**Optimiser:** NSGA-II (`pymoo`)  "
    "\n**Current Available Routes:** `LHR`, `MAN` <-> `SGN`, `HAN`, `BKK`  "
    "\n**Technical Details:** [Flight_Price_Prediction_and_Optimisation.pdf](https://mozilla.github.io/pdf.js/web/viewer.html?file=https://raw.githubusercontent.com/ThongLai/Flight-Scanner/main/docs/technical_document/Flight_Price_Prediction_and_Optimisation.pdf)"
)

if st.button("Refetch", type='primary', icon=":material/cloud_sync:"):
    st.cache_data.clear()
    st.rerun()

tab1, tab2, tab3 = st.tabs(
    ["Model Performance", "Flight Data Explorer", "Live Predictions"]
)




# ============================================================
# TAB 1 -- MODEL PERFORMANCE
# ============================================================
with tab1:
    st.header("Model Performance", divider="rainbow")
    metrics = load_metrics()

    if not metrics:
        st.warning("No metrics.json found. Run `python -m ml.evaluate`.")
    else:
        st.caption(
            f"Trained: `{fmt_dt(metrics.get('trained_at'), with_time=False)}`  ·  "
            f"Evaluated: `{fmt_dt(metrics.get('evaluated_at'))}`  ·  "
            f"Test rows: `{metrics.get('n_test', 0):,}`"
        )

        st.subheader("Price Regressor")
        reg = metrics.get("regressor", {})
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("MAE (£)", f"{reg.get('mae', 0):.2f}", border=True)
        c2.metric("RMSE (£)", f"{reg.get('rmse', 0):.2f}", border=True)
        c3.metric("R²", f"{reg.get('r2', 0):.3f}", border=True)
        c4.metric("MAPE (%)", f"{reg.get('mape', 0):.2f}", border=True)

        st.subheader("Price behaviour Classifier")
        clf = metrics.get("classifier", {})
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("ROC-AUC", f"{clf.get('auc', 0):.3f}", border=True)
        d2.metric("Accuracy", f"{clf.get('accuracy', 0):.3f}", border=True)
        d3.metric("Precision", f"{clf.get('precision', 0):.3f}",
                  border=True)
        d4.metric("Recall", f"{clf.get('recall', 0):.3f}", border=True)
    

    st.divider()
    st.subheader("Feature Importance")
    fi = load_feature_importance()
    if fi.empty:
        st.info("No feature_importance.csv found.")
    else:
        top_n = st.slider("Top features to show", 3,
                          min(50, len(fi)), min(10, len(fi)))
        fi_top = fi.nlargest(top_n, "importance").sort_values("importance")
        fig = px.bar(fi_top, x="importance", y="feature",
                     orientation="h", title="Top Feature Importances",
                     color="importance", color_continuous_scale="Blues")
        fig.update_layout(coloraxis_showscale=False, dragmode='pan')
        st.plotly_chart(styled(fig, height=max(350, top_n * 32)), config=PLOT_CONFIG)






# ============================================================
# TAB 2 -- FLIGHT DATA EXPLORER
# ============================================================
with tab2:
    st.header("Flight Data Explorer", divider="rainbow")
    cal = load_calendar()
    itin = load_itineraries()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Calendar records", f"{len(cal):,}", border=True)
    k2.metric("Itinerary records", f"{len(itin):,}", border=True)
    k3.metric("Routes tracked", cal["origin"].nunique() * 2, border=True)
    k4.metric("Collection days",
              cal["collected_at"].dt.date.nunique(), border=True)


    span = (f"{fmt_dt(cal['collected_at'].min(), with_time=False)} – "
    f"{fmt_dt(cal['collected_at'].max(), with_time=False)}")
    st.caption(f"Collection period: `{span}`")
    

    cal["route"] = cal["origin"] + "-" + cal["destination"]
    routes = sorted(cal["route"].unique())

    with st.container(horizontal_alignment='center'):
        sel_route = st.container(width='content').selectbox(
            "Select route to explore", 
            routes, 
            index=routes.index("MAN-SGN") if "MAN-SGN" in routes else 0,
        )
    
    route_df = cal[cal["route"] == sel_route]

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Price Distribution")
        fig = px.histogram(route_df, x="price", nbins=40,
                           title=f"{sel_route} price spread",
                           color_discrete_sequence=[ACCENT])
        st.plotly_chart(styled(fig), config=PLOT_CONFIG)

    with col_b:
        st.subheader("Booking Curve (Price vs Days Ahead)")
        curve = (route_df.groupby("days_ahead")["price"]
                 .median().reset_index())
        fig = px.line(curve, x="days_ahead", y="price",
                      title=f"{sel_route} median price by lead time",
                      color_discrete_sequence=[ACCENT])
        fig.update_xaxes(autorange="reversed")
        st.plotly_chart(styled(fig), config=PLOT_CONFIG)

    st.divider()
    
    st.subheader("Lowest Observed Price Over Time")
    ts = (route_df.groupby(route_df["collected_at"].dt.date)
          ["price"].min().reset_index())
    ts.columns = ["date", "min_price"]
    ts["date"] = pd.to_datetime(ts["date"])
    fig = px.line(ts, x="date", y="min_price",
                  title=f"{sel_route} lowest price by collection date",
                  color_discrete_sequence=[ACCENT])
    fig.update_xaxes(tickformat="%d/%m/%Y")
    st.plotly_chart(styled(fig), config=PLOT_CONFIG)






    st.divider()
    st.header("Flight Journeys Map", divider="rainbow")

    origin, dest = sel_route.split("-")
    route_itin = itin[(itin["origin_code"] == origin)
                      & (itin["dest_code"] == dest)].copy()

    if route_itin.empty:
        st.info("No itinerary paths for this route.")
    else:
        max_stops_avail = int(route_itin["stop_count"].max())

        with st.container(horizontal=True, vertical_alignment="center"):
            # max_journeys = st.container(width="content").selectbox(
            #     "Journeys to show", [4, 8, 12, 20], index=1, accept_new_options=True,
            #     key="map_top_n")
            max_journeys = st.container(width="content").selectbox(
                "Journeys to show", [4, 8, 12, 20, "All"], index=1,
                accept_new_options=True, key="map_top_n")
            max_journeys = int(max_journeys) if max_journeys != "All" else len(route_itin)
            max_stops = st.container(width="content").slider(
                "Max transits (stops)", 0, max_stops_avail,
                max_stops_avail, key="map_max_stops")
            selected_layers = st.container(width="content").multiselect(
                "Display layers",
                options=["Flow lines", "Airports", "Labels"],
                default=["Flow lines", "Airports", "Labels"])


        # Apply the transit filter
        filtered = route_itin[route_itin["stop_count"] <= max_stops]
        
        
        st.caption(f"Filtered to journeys with `≤ {max_stops}` "
                   f"transit(s): `{len(filtered):,}` itineraries")

        if filtered.empty:
            st.warning("No journeys match the transit filter.")
        else:
            deck = build_flight_map(filtered, sel_route,
                                    selected_layers, max_journeys)
            if deck is None:
                st.warning("No mappable coordinates "
                           "(missing airports in AIRPORTS dict).")
            else:
                st.pydeck_chart(deck, height=600)







# ============================================================
# TAB 3 -- LIVE PREDICTIONS & OPTIMISATION
# ============================================================
with tab3:
    st.header("Live Predictions & Optimisation", divider="rainbow")
    col_l, col_r = st.columns(2)

    with col_l:
        with st.container(border=True):
            st.subheader("Predict Price")
            p_route = st.text_input("Route", "LHR-SGN", key="pred_route")
            p_date = st.date_input("Departure date", key="pred_date", format="DD/MM/YYYY")
            if st.button("Predict", use_container_width=True):
                try:
                    r = predict_flight_and_cache(p_route, str(p_date))
                    m1, m2 = st.columns(2)
                    m1.metric("Estimated lowest price",
                              f"£{r['estimated_price']:.0f}")
                    m2.metric("Price-drop probability",
                              f"{r['wait_probability'] * 100:.1f}%")
                    st.info(f"**Outlook:** {r['action']}\n\n"
                            f"**Confidence:** {r['confidence']}")
                except Exception as e:
                    st.error(str(e))


    
    
    with col_r:
        with st.container(border=True):
            st.subheader("Optimise Flights")
            o_route = st.text_input("Route", "LHR-SGN", key="opt_route")
            o_date = st.date_input("Departure date", key="opt_date", format="DD/MM/YYYY")
            if st.button("Optimise (live)", use_container_width=True):
                try:
                    origin, dest = o_route.upper().split("-")
                    pareto = find_pareto_flights_and_cache(origin, dest, str(o_date))
                        
                    if pareto.empty:
                        st.warning("No flights found.")
                    else:
                        fig = px.scatter(
                            pareto, x="duration_hrs", y="price_raw",
                            size="stop_count",
                            color="stop_count",
                            hover_data=["carrier_names"],
                            title="Pareto front: price vs duration",
                            labels={"duration_hrs": "Duration (h)",
                                    "price_raw": "Price (£)",
                                    "stop_count": "Stops"},
                            color_continuous_scale="Blues")
                        st.plotly_chart(styled(fig), config=PLOT_CONFIG)
                        st.dataframe(pareto, hide_index=True)
                except Exception as e:
                    st.error(str(e))
                    
st.divider()
st.caption(f"[Tom Lai](https://tom-site.vercel.app) · 2026 · Contact: laiminhthong1@gmail.com")
