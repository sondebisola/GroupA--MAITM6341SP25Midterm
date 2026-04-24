# ── weather_api.py ────────────────────────────────────────────────────
# Data pipeline module — Person 1
# Fetches and parses weather data from Open-Meteo API

import requests
import pandas as pd


def get_city_coordinates(city_name):
    """City name → (lat, lon, display_name) via Open-Meteo geocoding."""
    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {"name": city_name, "count": 1, "language": "en", "format": "json"}
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    if "results" not in data or len(data["results"]) == 0:
        raise ValueError(f"City '{city_name}' not found.")
    result = data["results"][0]
    return result["latitude"], result["longitude"], f"{result['name']}, {result.get('country','')}"


def fetch_weather_data(lat, lon, start_year=2020, end_year=2024):
    """Fetches daily historical weather, returns raw JSON."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": f"{start_year}-01-01",
        "end_date":   f"{end_year}-12-31",
        "daily": ["temperature_2m_max", "temperature_2m_min",
                  "precipitation_sum", "windspeed_10m_max"],
        "timezone": "auto"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    raw = response.json()
    if "daily" not in raw:
        raise ValueError("Unexpected API response.")
    return raw


def parse_weather_dataframe(raw, city_name):
    """Converts raw JSON into a clean Pandas DataFrame."""
    daily = raw["daily"]
    df = pd.DataFrame({
        "date":         pd.to_datetime(daily["time"]),
        "temp_max":      daily["temperature_2m_max"],
        "temp_min":      daily["temperature_2m_min"],
        "precipitation": daily["precipitation_sum"],
        "windspeed":     daily["windspeed_10m_max"],
    })
    df = df.set_index("date")
    df["temp_range"] = df["temp_max"] - df["temp_min"]
    df = df.fillna(df.median(numeric_only=True))
    df["city"] = city_name
    return df


def get_weather_df(city_name, start_year=2020, end_year=2024):
    """
    Master function — takes city name, returns clean DataFrame.
    This is the only function Person 3 (Flask) needs to call.
    """
    lat, lon, display_name = get_city_coordinates(city_name)
    raw = fetch_weather_data(lat, lon, start_year, end_year)
    df = parse_weather_dataframe(raw, display_name)
    return df, display_name


# ── Verify file was written ───────────────────────────────────────────
print("✓ weather_api.py saved to weather-anomaly-detector/data/")%%writefile weather-anomaly-detector/data/weather_api.py
# ── weather_api.py ────────────────────────────────────────────────────
# Data pipeline module — Person 1
# Fetches and parses weather data from Open-Meteo API

import requests
import pandas as pd


def get_city_coordinates(city_name):
    """City name → (lat, lon, display_name) via Open-Meteo geocoding."""
    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {"name": city_name, "count": 1, "language": "en", "format": "json"}
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    if "results" not in data or len(data["results"]) == 0:
        raise ValueError(f"City '{city_name}' not found.")
    result = data["results"][0]
    return result["latitude"], result["longitude"], f"{result['name']}, {result.get('country','')}"


def fetch_weather_data(lat, lon, start_year=2020, end_year=2024):
    """Fetches daily historical weather, returns raw JSON."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": f"{start_year}-01-01",
        "end_date":   f"{end_year}-12-31",
        "daily": ["temperature_2m_max", "temperature_2m_min",
                  "precipitation_sum", "windspeed_10m_max"],
        "timezone": "auto"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    raw = response.json()
    if "daily" not in raw:
        raise ValueError("Unexpected API response.")
    return raw


def parse_weather_dataframe(raw, city_name):
    """Converts raw JSON into a clean Pandas DataFrame."""
    daily = raw["daily"]
    df = pd.DataFrame({
        "date":         pd.to_datetime(daily["time"]),
        "temp_max":      daily["temperature_2m_max"],
        "temp_min":      daily["temperature_2m_min"],
        "precipitation": daily["precipitation_sum"],
        "windspeed":     daily["windspeed_10m_max"],
    })
    df = df.set_index("date")
    df["temp_range"] = df["temp_max"] - df["temp_min"]
    df = df.fillna(df.median(numeric_only=True))
    df["city"] = city_name
    return df


def get_weather_df(city_name, start_year=2020, end_year=2024):
    """
    Master function — takes city name, returns clean DataFrame.
    This is the only function Person 3 (Flask) needs to call.
    """
    lat, lon, display_name = get_city_coordinates(city_name)
    raw = fetch_weather_data(lat, lon, start_year, end_year)
    df = parse_weather_dataframe(raw, display_name)
    return df, display_name


# ── Verify file was written ───────────────────────────────────────────
print("✓ weather_api.py saved to weather-anomaly-detector/data/")
