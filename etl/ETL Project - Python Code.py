#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  3 12:34:35 2025

@author: nageshpentakota
"""

import pandas as pd
import mysql.connector
from datetime import datetime

# ---------------- DB CONFIG ----------------
DB_CONFIG = {
    "host": "your_host",
    "user": "your_username",
    "password": "your_pwd",
    "database": "your_dbname"
}

def db_connection():
    return mysql.connector.connect(**DB_CONFIG)

# ---------------- LOAD WEATHER ----------------
def load_weather():
    weather_file = "Washington, DC, Sept 2024 to Nov 2025.xlsx"
    print("Reading Weather Data:", weather_file)

    weather_df = pd.read_excel(weather_file)
    weather_df["datetime"] = pd.to_datetime(weather_df["datetime"]).dt.date
    weather_df = weather_df.where(pd.notnull(weather_df), None)

    weather_sql = """
    INSERT INTO weather_daily (
        name, datetime, tempmax, tempmin, temp, feelslikemax, feelslikemin,
        feelslike, dew, humidity, precip, precipprob, precipcover, preciptype,
        snow, snowdepth, windgust, windspeed, winddir, sealevelpressure,
        cloudcover, visibility, solarradiation, solarenergy, uvindex,
        severerisk, sunrise, sunset, moonphase, conditions,
        description, icon, stations
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s)
    """

    weather_data = weather_df[[
        "name","datetime","tempmax","tempmin","temp","feelslikemax","feelslikemin",
        "feelslike","dew","humidity","precip","precipprob","precipcover","preciptype",
        "snow","snowdepth","windgust","windspeed","winddir","sealevelpressure",
        "cloudcover","visibility","solarradiation","solarenergy","uvindex",
        "severerisk","sunrise","sunset","moonphase","conditions",
        "description","icon","stations"
    ]].values.tolist()

    db_conn = db_connection()
    db_cursor = db_conn.cursor()

    db_cursor.executemany(weather_sql, weather_data)
    db_conn.commit()

    db_cursor.close()
    db_conn.close()
    print("Weather Data Imported & Loaded successfully")
# ---------------- LOAD VIOLATIONS ----------------
def load_violations(file_name):
    print("Loading violations:", file_name)

    chunk_size = 5000

    db_conn = db_connection()
    db_cursor = db_conn.cursor()

    violations_sql = """
    INSERT INTO moving_violations (
        OBJECTID, LOCATION, XCOORD, YCOORD, ISSUE_DATE, ISSUE_TIME,
        ISSUING_AGENCY_CODE, ISSUING_AGENCY_NAME, ISSUING_AGENCY_SHORT,
        VIOLATION_CODE, VIOLATION_PROCESS_DESC, PLATE_STATE,
        ACCIDENT_INDICATOR, DISPOSITION_CODE, DISPOSITION_TYPE,
        DISPOSITION_DATE, FINE_AMOUNT, TOTAL_PAID, PENALTY_1, PENALTY_2,
        PENALTY_3, PENALTY_4, PENALTY_5, RP_MULT_OWNER_NO,
        BODY_STYLE, LATITUDE, LONGITUDE, MAR_ID, GIS_LAST_MOD_DTTM
    ) VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s
    )
    """

    for chunk_df in pd.read_csv(file_name, chunksize=chunk_size):

        chunk_df["ISSUE_DATE"] = pd.to_datetime(chunk_df["ISSUE_DATE"], errors="coerce")
        chunk_df["DISPOSITION_DATE"] = pd.to_datetime(chunk_df["DISPOSITION_DATE"], errors="coerce")
        chunk_df["GIS_LAST_MOD_DTTM"] = pd.to_datetime(chunk_df["GIS_LAST_MOD_DTTM"], errors="coerce")

        #  CLEANING — removes BOTH NaN and "nan" strings
        chunk_df = chunk_df.replace({pd.NA: None, "nan": None, "NaN": None, "": None})
        chunk_df = chunk_df.astype(object).where(pd.notnull(chunk_df), None)

        violations_data = chunk_df[
            ["OBJECTID","LOCATION","XCOORD","YCOORD","ISSUE_DATE","ISSUE_TIME",
             "ISSUING_AGENCY_CODE","ISSUING_AGENCY_NAME","ISSUING_AGENCY_SHORT",
             "VIOLATION_CODE","VIOLATION_PROCESS_DESC","PLATE_STATE",
             "ACCIDENT_INDICATOR","DISPOSITION_CODE","DISPOSITION_TYPE",
             "DISPOSITION_DATE","FINE_AMOUNT","TOTAL_PAID","PENALTY_1","PENALTY_2",
             "PENALTY_3","PENALTY_4","PENALTY_5","RP_MULT_OWNER_NO",
             "BODY_STYLE","LATITUDE","LONGITUDE","MAR_ID","GIS_LAST_MOD_DTTM"]
        ].values.tolist()

        db_cursor.executemany(violations_sql, violations_data)
        db_conn.commit()

        print(f"Inserted {len(violations_data)} rows from {file_name}")

    db_cursor.close()
    db_conn.close()
    print("Finished:", file_name)

# ---------------- MAIN ----------------
if __name__ == "__main__":

    load_weather()

    violation_files = [
        "Moving_Violations_Issued_in_September_2024.csv",
        "Moving_Violations_Issued_in_October_2024.csv",
        "Moving_Violations_Issued_in_November_2024.csv",
        "Moving_Violations_Issued_in_December_2024.csv",
        "Moving_Violations_Issued_in_January_2025.csv",
        "Moving_Violations_Issued_in_February_2025.csv",
        "Moving_Violations_Issued_in_March_2025.csv"
    ]

    for f in violation_files:
        load_violations(f)

    print("ETL PROCESS COMPLETED SUCCESSFULLY")