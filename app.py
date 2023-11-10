from flask import Flask, request, jsonify
import requests
import psycopg2
import random
import circuitbreaker
from circuitbreaker import circuit
from requests.exceptions import HTTPError
import json
import logging

app = Flask(__name__)

# Configura la conexión a la base de datos
conn = psycopg2.connect(
    host="localhost",
    database="Soft_12",
    user="postgres",
    password="161049"
)

def personal_function():
    selected_data = {
            "anime_id": "error"
        }
    print("\nselected_data\n")
    return jsonify(selected_data.json())

def is_rate_limited(thrown_type, thrown_value):
    return issubclass(thrown_type, HTTPError) and thrown_value.response.status_code == 429


class MyCircuitBreaker(circuitbreaker.CircuitBreaker):
    FAILURE_THRESHOLD = 1
    RECOVERY_TIMEOUT = 30
    EXPECTED_EXCEPTION = is_rate_limited


@circuit(cls=MyCircuitBreaker)
def get_anime_details(anime_id):
    """
    cached_data = get_cached_anime_details(anime_id)
    if cached_data:
        print('in cache', anime_id)
        return cached_data
    """
    url = f'https://api.jikan.moe/v4/anime/{anime_id}/full'

    
    response = requests.get(url)
    print("status code")
    print(response.status_code)
    response.raise_for_status()

    anime_data = []
    if response.status_code == 200:
        anime_data = response.json()["data"]
        selected_data = {
            "anime_id": anime_id,
            "title": anime_data["title"],
            "title_english": anime_data["title_english"],
            "title_japanese": anime_data["title_japanese"]
        }

        # Insert the data into the database
        with conn.cursor() as cur:
            insert_query = """
            INSERT INTO anime (id, title, title_english, title_japanese)
            VALUES (%(anime_id)s, %(title)s, %(title_english)s, %(title_japanese)s);
            """
            cur.execute(insert_query, selected_data)
            conn.commit()
    return anime_data

@app.route('/get_anime/<int:anime_id>', methods=['GET'])
def get_anime(anime_id):
    anime_id = random.randint(1, 1000)
    try:
        data = get_anime_details(anime_id)
        return {
        "status_code": 200,
        "success": True,
        "message": "Success get starwars data", 
        "data": data
        }
    except circuitbreaker.CircuitBreakerError as e:
        print("\Circuit breaker active\n")
        return {
        "status_code": 200,
        "success": False,
        "message": f"Circuit breaker active: {e}"
        }, 200
    except requests.exceptions.RequestException as e:
        print("\nError\n")
        return {
        "status_code": 500,
        "success": False,
        "message": f"Failed get anime data: {e}"
        }, 500
        



# Function to get anime details from the database
def get_cached_anime_details(anime_id):
    with conn.cursor() as cur:
        select_query = "SELECT * FROM anime WHERE id = %s;"
        cur.execute(select_query, (anime_id,))
        anime_data = cur.fetchone()
        if anime_data:
            selected_data = {
                "anime_id": anime_data[0],
                "title": anime_data[1],
                "title_english": anime_data[2],
                "title_japanese": anime_data[3]
            }
            return selected_data
    return None




if __name__ == '__main__':
    app.run(debug=True)
