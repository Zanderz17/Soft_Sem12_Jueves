from flask import Flask, request, jsonify
import requests
import psycopg2
import time
import random
import os

app = Flask(__name__)

# Configura la conexión a la base de datos
conn = psycopg2.connect(
    host="localhost",
    database="Soft_12",
    user="postgres",
    password="161049"
)

# Ruta para obtener detalles de un anime por ID
@app.route('/get_anime/<int:anime_id>', methods=['GET'])
def get_anime(anime_id):
    anime_id = random.randint(1, 1000)
    anime_data = get_anime_details(anime_id)
    if anime_data:
        return jsonify(anime_data)
    else:
        return jsonify({"error": "Anime no encontrado"}), 404

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

# Function to get anime details from the Jikan API and store in the database
def get_anime_details(anime_id):
    cached_data = get_cached_anime_details(anime_id)
    if cached_data:
        print('in cache', anime_id)
        return cached_data
    
    url = f'https://api.jikan.moe/v4/anime/{anime_id}/full'
        
    response = requests.get(url)

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

        return selected_data
    
    else:
        return response, 500

if __name__ == '__main__':
    app.run(debug=True)