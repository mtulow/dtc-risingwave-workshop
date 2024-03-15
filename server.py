#!/usr/bin/env python3

from flask import Flask, jsonify, render_template
import os
import psycopg2
from flask_cors import CORS
from dotenv import load_dotenv

app = Flask(__name__)
CORS(app)

# Read the .env file
load_dotenv()
# Replace the following with your PostgreSQL database connection details
db_connection_params = {
    'host': os.environ.get('PGHOST','localhost'),
    'database': os.environ.get('PGDATABASE','dev'),
    'user': os.environ.get('PGUSER','root'),
    'port': os.environ.get('PGPORT',4566),
}

def run_query(query):
    connection = psycopg2.connect(**db_connection_params)
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    connection.close()
    return result

@app.route('/get_busiest_zones', methods=['GET'])
def get_busiest_zones():
    # Replace the following with your SQL query for busiest zones
    query = 'SELECT * FROM busiest_zones_1_min'
    result = run_query(query)
    return jsonify(result)

@app.route('/get_longest_trips', methods=['GET'])
def get_longest_trips():
    # Replace the following with your SQL query for longest trips
    query = 'SELECT * FROM longest_trip_1_min'
    result = run_query(query)
    return jsonify(result)

@app.route('/get_trip_minutes', methods=['GET'])
def get_trip_minutes():
    # Replace the following with your SQL query for trip times
    query = 'SELECT * FROM trip_minutes'
    result = run_query(query)
    return jsonify(result)

@app.route('/get_trip_seconds', methods=['GET'])
def get_trip_seconds():
    # Replace the following with your SQL query for trip times
    query = 'SELECT * FROM trip_seconds'
    result = run_query(query)
    return jsonify(result)

@app.route('/')
def index():
    return render_template('index.html')


if __name__ == '__main__':
    app.run(debug=True)
