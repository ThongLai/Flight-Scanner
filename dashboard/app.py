import json
import sys
from pathlib import Path

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Make src/ importable
SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))

from ml.data_loader import load_all_calendars, load_all_itineraries
from ml.predict import predict_flight
from ml.optimise import find_pareto_flights

ROOT = Path(__file__).resolve().parents[1]
MODELS_DIR = ROOT / "models"

st.set_page_config(page_title="Flight Scanner Dashboard",
                   layout="wide", page_icon=None)


@st.cache_data(ttl=3600, show_spinner="Loading collected calendar data...")
def load_calendar():
    cols = ["collected_at", "departure_date", "origin",
            "destination", "price"]
    return load_all_calendars(columns=cols)



@st.cache_data(ttl=3600, show_spinner="Loading itineraries...")
def load_itineraries():
    cols = ["origin_code", "dest_code", "price_raw", "duration_minutes",
            "stop_count", "carrier_names", "is_direct", "search_date"]
    return load_all_itineraries(columns=cols)


@st.cache_data(ttl=3600, show_spinner="Loading model metrics...")
def load_metrics():
    """Load saved model metrics JSON if present."""
    path = MODELS_DIR / "metrics.json"
    if path.exists():
        return json.loads(path.read_text())
    return {}


@st.cache_data(ttl=3600, show_spinner="Loading feature importance...")
def load_feature_importance():
    """Load feature importance CSV if present."""
    path = MODELS_DIR / "feature_importance.csv"
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


st.title("Flight Scanner Model Dashboard")
st.caption("Multi-objective flight price prediction and optimisation")

tab1, tab2, tab3 = st.tabs(
    ["Model Performance", "Flight Data Explorer", "Predictions"]
)





with tab1:
    st.header("Model Performance")
    metrics = load_metrics()

    if not metrics:
        st.warning(
            "No metrics.json found in models/. "
            "Save metrics during training to populate this tab."
        )
    else:
        st.subheader("Price Regressor (LightGBM)")
        c1, c2, c3, c4 = st.columns(4)
        reg = metrics.get("regressor", {})
        c1.metric("MAE (£)", f"{reg.get('mae', 0):.2f}")
        c2.metric("RMSE (£)", f"{reg.get('rmse', 0):.2f}")
        c3.metric("R²", f"{reg.get('r2', 0):.3f}")
        c4.metric("MAPE (%)", f"{reg.get('mape', 0):.1f}")

        st.subheader("Wait Classifier (LightGBM)")
        d1, d2, d3, d4 = st.columns(4)
        clf = metrics.get("classifier", {})
        d1.metric("ROC-AUC", f"{clf.get('auc', 0):.3f}")
        d2.metric("Accuracy", f"{clf.get('accuracy', 0):.3f}")
        d3.metric("Precision", f"{clf.get('precision', 0):.3f}")
        d4.metric("Recall", f"{clf.get('recall', 0):.3f}")

    st.divider()

    st.subheader("Feature Importance")
    fi = load_feature_importance()
    if fi.empty:
        st.info("No feature_importance.csv found in models/.")
    else:
        top_n = st.slider("Top features to show", 5, 50, 20)
        fi_top = fi.nlargest(top_n, "importance")
        fig = px.bar(
            fi_top.sort_values("importance"),
            x="importance", y="feature", orientation="h",
            title="Top Feature Importances",
        )
        fig.update_layout(height=max(400, top_n * 22))
        st.plotly_chart(fig, use_container_width=True)







with tab2:
    st.header("Flight Data Explorer")
    cal = load_calendar()
    itin = load_itineraries()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Calendar records", f"{len(cal):,}")
    k2.metric("Itinerary records", f"{len(itin):,}")
    k3.metric("Routes tracked", cal["origin"].nunique() * 2)
    k4.metric("Collection days",
              cal["collected_at"].dt.date.nunique())

    st.divider()

    cal["route"] = cal["origin"] + "-" + cal["destination"]
    routes = sorted(cal["route"].unique())
    sel_route = st.selectbox("Select route", routes)
    route_df = cal[cal["route"] == sel_route]

    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Price Distribution")
        fig = px.histogram(route_df, x="price", nbins=40,
                           title=f"{sel_route} price spread")
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader("Price vs Days Ahead (Booking Curve)")
        curve = (route_df.groupby("days_ahead")["price"]
                 .median().reset_index())
        fig = px.line(curve, x="days_ahead", y="price",
                      title=f"{sel_route} median price by lead time")
        fig.update_xaxes(autorange="reversed")
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    st.subheader("Price Over Collection Time")
    ts = (route_df.groupby(route_df["collected_at"].dt.date)
          ["price"].min().reset_index())
    ts.columns = ["date", "min_price"]
    fig = px.line(ts, x="date", y="min_price",
                  title=f"{sel_route} lowest observed price over time")
    st.plotly_chart(fig, use_container_width=True)






with tab3:
    st.header("Live Predictions & Optimisation")

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Price Prediction")
        p_route = st.text_input("Route", "LHR-SGN", key="pred_route")
        p_date = st.date_input("Departure date", key="pred_date")
        if st.button("Predict"):
            try:
                r = predict_flight(p_route, str(p_date))
                st.metric("Estimated lowest price",
                          f"£{r['estimated_price']:.0f}")
                st.metric("Price-drop probability",
                          f"{r['wait_probability'] * 100:.1f}%")
                st.write(f"**Outlook:** {r['action']}")
                st.write(f"**Confidence:** {r['confidence']}")
            except Exception as e:
                st.error(str(e))

    with col_r:
        st.subheader("Pareto-Optimal Flights")
        o_route = st.text_input("Route", "LHR-SGN", key="opt_route")
        o_date = st.date_input("Departure date", key="opt_date")
        if st.button("Optimise (live)"):
            try:
                origin, dest = o_route.upper().split("-")
                pareto = find_pareto_flights(origin, dest, str(o_date))
                if pareto.empty:
                    st.warning("No flights found.")
                else:
                    fig = px.scatter(
                        pareto, x="duration_hrs", y="price_raw",
                        size="stop_count", hover_data=["carrier_names"],
                        title="Pareto front: price vs duration",
                        labels={"duration_hrs": "Duration (h)",
                                "price_raw": "Price (£)"},
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(pareto, use_container_width=True)
            except Exception as e:
                st.error(str(e))
