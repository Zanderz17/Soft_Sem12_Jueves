from flask import Flask, request, jsonify
import requests
import psycopg2
import random
import json
import logging
import time



app = Flask(__name__)

# Configura la conexión a la base de datos
conn = psycopg2.connect(
    host="localhost",
    database="Soft_12",
    user="postgres",
    password="161049"
)

class CircuitBreaker:
    def __init__(self, failure_threshold, recovery_timeout):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.circuit_opened_callback = None

    def on_circuit_opened(self):
        if self.circuit_opened_callback:
            self.circuit_opened_callback()

    def set_circuit_opened_callback(self, callback):
        self.circuit_opened_callback = callback

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            if self.failure_count >= self.failure_threshold:
                current_time = time.time()
                if self.last_failure_time is not None and current_time - self.last_failure_time < self.recovery_timeout:
                    raise CircuitBreakerError("Circuit breaker is open")
                else:
                    # Reset the failure count when recovery timeout is reached
                    self.failure_count = 0
                    self.on_circuit_opened()

            try:
                result = func(*args, **kwargs)
                # Reset failure count on successful request
                self.failure_count = 0
                return result
            except requests.exceptions.RequestException:
                self.failure_count += 1
                self.last_failure_time = time.time()
                raise CircuitBreakerError("Request failed, circuit breaker failure count increased")

        return wrapper

class CircuitBreakerError(Exception):
    pass

# Uso del decorador personalizado
circuit_breaker_instance = CircuitBreaker(failure_threshold=1, recovery_timeout=30)

# Función que se llamará cuando el circuit breaker esté abierto
def on_circuit_opened_callback():
    print("Handling circuit breaker opened event!")

# Configurar el callback en el circuit breaker
circuit_breaker_instance.set_circuit_opened_callback(on_circuit_opened_callback)

@circuit_breaker_instance
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
    try:
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
    except CircuitBreakerError as e:
        # Manejar la excepción del circuit breaker cuando está abierto
        print(f"gaaa: {e}")
        return None
    except requests.exceptions.RequestException as e:
        # Manejar otras excepciones de solicitud aquí
        print(f"Request failed: {e}")
        raise



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
    except requests.exceptions.RequestException as e:
        return {
        "status_code": 500,
        "success": False,
        "message": f"Failed get starwars data: {e}"
        }
        



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
