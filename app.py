from flask import Flask, request, jsonify
import openmeteo_requests
import requests_cache
import pandas as pd
import datetime
from retry_requests import retry
from google.cloud import storage
import os
import json
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Setup Open-Meteo API client
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# API URL
url = "https://archive-api.open-meteo.com/v1/archive"

# Configure GCS
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Ensure credentials are set
if not GOOGLE_APPLICATION_CREDENTIALS:
    raise EnvironmentError("Google Application Credentials not set in the environment.")

# GCS Client
storage_client = storage.Client()

@app.route('/store-weather-data', methods=['POST'])
def store_weather_data():
    try:
        # Parse the JSON request body
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid JSON payload"}), 400
        
        latitude = float(data.get("latitude"))
        longitude = float(data.get("longitude"))
        start_date_string = data.get("start_date")
        end_date_string = data.get("end_date")

        if not all([latitude, longitude, start_date_string, end_date_string]):
            return jsonify({"error": "Missing required parameters"}), 400

        # Validate dates
        start_date_object = datetime.datetime.strptime(start_date_string, "%Y-%m-%d").date()
        end_date_object = datetime.datetime.strptime(end_date_string, "%Y-%m-%d").date()

        if start_date_object > end_date_object:
            return jsonify({"error": "Start date must be before end date"}), 400

        # Define API parameters
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date_object.strftime("%Y-%m-%d"),
            "end_date": end_date_object.strftime("%Y-%m-%d"),
            "daily": [
                "temperature_2m_max",
                "temperature_2m_min",
                "temperature_2m_mean",
                "apparent_temperature_max",
                "apparent_temperature_min",
                "apparent_temperature_mean"
            ],
            "timezone": "auto"
        }

        # Fetch data from Open-Meteo API
        response = openmeteo.weather_api(url, params=params)
        if not response:
            return jsonify({"error": "Failed to fetch data from Open-Meteo API"}), 502

        # Process the daily data
        response_data = response[0] 
        daily = response_data.Daily()

        daily_data = {
            "date": pd.date_range(
                start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="left"
            ).strftime('%Y-%m-%d').tolist(),
            "temperature_2m_max": daily.Variables(0).ValuesAsNumpy().tolist(),
            "temperature_2m_min": daily.Variables(1).ValuesAsNumpy().tolist(),
            "temperature_2m_mean": daily.Variables(2).ValuesAsNumpy().tolist(),
            "apparent_temperature_max": daily.Variables(3).ValuesAsNumpy().tolist(),
            "apparent_temperature_min": daily.Variables(4).ValuesAsNumpy().tolist(),
            "apparent_temperature_mean": daily.Variables(5).ValuesAsNumpy().tolist(),
        }

        # Prepare data for storage
        file_data = {
            "latitude": response_data.Latitude(),
            "longitude": response_data.Longitude(),
            "start_date": start_date_string,
            "end_date": end_date_string,
            "daily_data": daily_data
        }
        file_name = f"weather_data_{latitude}_{longitude}_{start_date_string}_to_{end_date_string}.json"

        # Upload to GCS
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(file_name)
        blob.upload_from_string(json.dumps(file_data), content_type='application/json')

        return jsonify({
            "message": "Data stored successfully in GCS",
            "file_name": file_name
        }), 200

    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500
    
@app.route('/list-weather-files', methods=['GET'])
def list_weather_files():
    try:
        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(GCS_BUCKET_NAME)

        # List all files in the bucket
        files = [blob.name for blob in bucket.list_blobs()]

        return jsonify(files), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/weather-file-content/<file_name>', methods=['GET'])
def get_weather_file_content(file_name):
    try:
        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(GCS_BUCKET_NAME)

        # Get the blob (file) from the bucket
        blob = bucket.blob(file_name)

        if not blob.exists():
            return jsonify({"error": "File not found."}), 404
        
         # Download file content as string
        content = blob.download_as_text()

        # Return content as JSON
        return jsonify(json.loads(content)), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
if __name__ == '__main__':
    app.run(debug=True)
