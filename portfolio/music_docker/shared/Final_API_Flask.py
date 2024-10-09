from flask import Flask, request, jsonify
import pandas as pd
import requests
import json
import psycopg2


app = Flask(__name__)


@app.route('/api/get_pg_data', methods=['GET'])
def get_pg_data():
    
    ## Connect to Database
    conn_ps = psycopg2.connect(
        host="postgres", # "localhost"
        port=5432,
        database="lexi",
        user="lexi",
        password="lexi123")
    
    ## Create a cursor object to interface with psql
    cur_ps = conn_ps.cursor()
    
    cur_ps.execute(
        """
        SELECT * FROM music.genre_results
        """)

    rows = cur_ps.fetchall()
    
    data = [{'id':row[0],'fav_genre':row[1],'age_mean':row[2],'hours_mean':row[3],'effects_mean':row[4],'anxiety_mean':row[5],'depression_mean':row[6],'insomnia_mean':row[7],'count':row[8]} \
            for row in rows]
    
    cur_ps.close()
    conn_ps.close()
    
    return jsonify(data)



@app.route('/api/get_alltablesjoined', methods=['GET'])
def get_alltablesjoined():
    
    ## Connect to Database
    conn_ps = psycopg2.connect(
        host="postgres", # "localhost"
        port=5432,
        database="lexi",
        user="lexi",
        password="lexi123")
    
    ## Create a cursor object to interface with psql
    cur_ps = conn_ps.cursor()
    
    cur_ps.execute(
        """
        select album_name, track_name, artist_name, fav_genre, country
        from 
        music.spotify_songs a
        left join 
        music.artists b
        on a.artist_id = b.artist_id
        left join 
        music.artist_genres_mapping c 
        on a.artist_id = c.artist_id
        left join 
        music.genre_results d
        on c.genre_id = d.genre_id
        left join 
        music.artist_country_mapping e
        on a.artist_id = e.artist_id
        left join
        music.country f
        on 
        e.country_id = f.country_id limit 10
        """)

    rows = cur_ps.fetchall()
    
    data = [{'album_name':row[0],'track_name':row[1],'artist_name':row[2],'fav_genre':row[3],'country':row[4]} \
            for row in rows]
    
    cur_ps.close()
    conn_ps.close()
    
    return jsonify(data)

if __name__ == '__main__':
    port = 8001
    app.run(debug=True, port=port)
