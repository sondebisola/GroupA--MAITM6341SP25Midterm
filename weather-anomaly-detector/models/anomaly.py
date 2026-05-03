# ── anomaly.py ───────────────────────────────────────────────────────
# ML module — Person 2
# Feature engineering and anomaly detection using Isolation Forest

import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler


def engineer_features(df):
    """
    Adds engineered features to the weather DataFrame.

    Args:
        df (pd.DataFrame): Weather data with columns: temp_max, temp_min,
                           precipitation, windspeed, temp_range, city

    Returns:
        pd.DataFrame: Enriched dataframe with rolling averages, deviations, and z-scores
    """
    # Create a copy to avoid modifying the original
    df_enriched = df.copy()

    # ── Rolling averages (7-day and 30-day) ──────────────────────────
    df_enriched['temp_max_30d'] = df_enriched['temp_max'].rolling(window=30, center=True).mean()
    df_enriched['precip_30d'] = df_enriched['precipitation'].rolling(window=30, center=True).mean()
    df_enriched['wind_30d'] = df_enriched['windspeed'].rolling(window=30, center=True).mean()

    # ── Deviation from rolling average ───────────────────────────────
    df_enriched['temp_max_dev'] = df_enriched['temp_max'] - df_enriched['temp_max_30d']
    df_enriched['precip_dev'] = df_enriched['precipitation'] - df_enriched['precip_30d']
    df_enriched['wind_dev'] = df_enriched['windspeed'] - df_enriched['wind_30d']

    # ── Z-scores (standardized values) ────────────────────────────────
    df_enriched['temp_max_z'] = (df_enriched['temp_max'] - df_enriched['temp_max'].mean()) / df_enriched['temp_max'].std()
    df_enriched['temp_min_z'] = (df_enriched['temp_min'] - df_enriched['temp_min'].mean()) / df_enriched['temp_min'].std()
    df_enriched['precip_z'] = (df_enriched['precipitation'] - df_enriched['precipitation'].mean()) / df_enriched['precipitation'].std()
    df_enriched['wind_z'] = (df_enriched['windspeed'] - df_enriched['windspeed'].mean()) / df_enriched['windspeed'].std()
    df_enriched['temp_range_z'] = (df_enriched['temp_range'] - df_enriched['temp_range'].mean()) / df_enriched['temp_range'].std()

    # ── Fill NaN values created by rolling windows ───────────────────
    df_enriched = df_enriched.fillna(df_enriched.median(numeric_only=True))

    return df_enriched


def detect_anomalies(df, contamination=0.05):
    """
    Detects weather anomalies using Isolation Forest algorithm.

    Args:
        df (pd.DataFrame): Weather DataFrame (preferably enriched with engineer_features)
        contamination (float): Expected proportion of anomalies (default: 0.05 = 5%)

    Returns:
        pd.DataFrame: DataFrame with added columns:
                      - is_anomaly (bool): True if day is flagged as anomalous
                      - anomaly_score (float): Anomaly score (more negative = more anomalous)
                      - anomaly_label (int): -1 for anomalies, +1 for normal
    """
    # Create a copy to avoid modifying the original
    df_result = df.copy()

    # If not already enriched, add features
    if 'temp_max_z' not in df_result.columns:
        df_result = engineer_features(df_result)

    # ── Define features for model ─────────────────────────────────────
    MODEL_FEATURES = [
        'temp_max', 'temp_min', 'precipitation', 'windspeed', 'temp_range',
        'temp_max_dev', 'precip_dev', 'wind_dev',
        'temp_max_z', 'temp_min_z', 'precip_z', 'wind_z', 'temp_range_z'
    ]

    # ── Prepare feature matrix ────────────────────────────────────────
    X = df_result[MODEL_FEATURES].values

    # ── Normalize features ────────────────────────────────────────────
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # ── Train Isolation Forest ────────────────────────────────────────
    model = IsolationForest(
        contamination=contamination,  # Expected proportion of anomalies
        n_estimators=200,              # Number of trees
        random_state=42,               # For reproducibility
        n_jobs=-1                      # Use all CPU cores
    )
    model.fit(X_scaled)

    # ── Predict anomalies ─────────────────────────────────────────────
    anomaly_labels = model.predict(X_scaled)
    anomaly_scores = model.decision_function(X_scaled)

    # ── Add results to DataFrame ──────────────────────────────────────
    df_result['is_anomaly'] = anomaly_labels == -1
    df_result['anomaly_score'] = anomaly_scores
    df_result['anomaly_label'] = anomaly_labels

    return df_result
