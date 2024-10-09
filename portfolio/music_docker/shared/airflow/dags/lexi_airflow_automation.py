from airflow import DAG
import psycopg2
from psycopg2 import sql
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy_operator import DummyOperator
#this needs to be installed for the spotify api to work
#!pip install spotipy
import spotipy
from spotipy import SpotifyClientCredentials


from random import randint
from datetime import datetime, timedelta
import pandas as pd
import json 
import os
import numpy as np

def create_schema_and_tables():
    conn_ps = psycopg2.connect(
    host="postgres", # "localhost"
    port=5432,
    database="lexi",
    user="lexi",
    password="lexi123")

    ## Create a cursor object to interface with psql
    cur_ps = conn_ps.cursor()

    #creates the music schema 
    cur_ps.execute(
        """
        CREATE SCHEMA IF NOT EXISTS music
        """)

    #creates the artists table
    cur_ps.execute(
        """
         CREATE TABLE IF NOT EXISTS music.artists (
                  artist_id bigint NOT NULL PRIMARY KEY,
                    artist_name VARCHAR(255) NOT NULL,
                    listeners bigint NOT NULL,
                    plays bigint NOT NULL
                 )
        """)

    #creates the genre_results table
    cur_ps.execute(
        """
         CREATE TABLE IF NOT EXISTS music.genre_results (
                  genre_id bigint NOT NULL PRIMARY KEY,
                  fav_genre varchar(50) NOT NULL,
                  age_mean float NOT NULL,	
                  hours_mean float NOT NULL	,
                  effects_mean float NOT NULL	,
                  anxiety_mean	float NOT NULL,
                  depression_mean float NOT NULL,
                  insomnia_mean	float NOT NULL,
                  count int NOT NULL
                 )
        """)

    #creates the country table
    cur_ps.execute(
        """
         CREATE TABLE IF NOT EXISTS music.country (
                  country_id bigint NOT NULL PRIMARY KEY,
                    country VARCHAR(255) NOT NULL
                 )
        """)  
    
    #creates the artist_country_mapping table
    cur_ps.execute(
        """
         CREATE TABLE IF NOT EXISTS music.artist_country_mapping (
                  artist_id bigint NOT NULL,
                  country_id bigint NOT NULL,
                  CONSTRAINT artist_fk_name FOREIGN KEY (artist_id)
                  REFERENCES music.artists (artist_id),
                  CONSTRAINT country_fk_name FOREIGN KEY (country_id)
                  REFERENCES music.country (country_id),
                  CONSTRAINT artist_country_unique UNIQUE (artist_id, country_id)
                 )
        """)


    #creates the spotify_songs table
    cur_ps.execute(
        """
         CREATE TABLE IF NOT EXISTS music.spotify_songs (
                 track_id VARCHAR(256) PRIMARY KEY,
                 track_name	VARCHAR(256) NOT NULL,
                 artist_id bigint NOT NULL,
                 popularity	INT NOT NULL,
                 album_name	VARCHAR(256) NOT NULL,
                 album_or_single VARCHAR(256) NOT NULL,
                 release_date VARCHAR(256) NOT NULL,
                 year int NOT NULL,
                  CONSTRAINT artists_fk_name FOREIGN KEY (artist_id)
                  REFERENCES music.artists (artist_id))
        """)


    #creates the artist_genres_mapping table
    cur_ps.execute(
        """
         CREATE TABLE IF NOT EXISTS music.artist_genres_mapping (
                  artist_id bigint NOT NULL,
                  genre_id bigint NOT NULL,
                  CONSTRAINT artist_fk_name FOREIGN KEY (artist_id)
                      REFERENCES music.artists (artist_id),
                  CONSTRAINT genre_fk_name FOREIGN KEY (genre_id)
                      REFERENCES music.genre_results (genre_id),
                  CONSTRAINT artist_genre_unique UNIQUE (artist_id, genre_id)
                 )
        """)

    #creates the individual_survey table
    cur_ps.execute(
        """
         CREATE TABLE IF NOT EXISTS music.individual_survey (
                  individual_id bigint NOT NULL PRIMARY KEY,
                  genre_id bigint NOT NULL,
                  age int NOT NULL,
                  music_effects varchar(50) NOT NULL,
                  listening_hours int NOT NULL,
                  anxiety int NOT NULL,
                  depression int NOT NULL,
                  insomnia int NOT NULL,
                  music_effects_num int NOT NULL,
                  CONSTRAINT genre_id_fk_name FOREIGN KEY (genre_id)
                  REFERENCES music.genre_results (genre_id)
                 )
        """)
    
    conn_ps.commit()
    cur_ps.close()
    conn_ps.close()




def generate_hash_id(value):
    # Initialize an accumulator for the hash
    hash_value = 0
    
    if  (type(value) == float) | (value is None):
        value = "0"
    
    value = value.lower()
    
    # Iterate over each character in the value
    for char in str(value):
        # Update the hash_value using a simple hash function
        hash_value = (hash_value * 31 + ord(char)) % (2**32)

    return hash_value
    


def spotify_pull():
    secret = "1c2bc31409b642fbb9516547175ae62d"
    ID = "e5b9203d4ddb40598614382faf363094"
    
    
    client_credentials_manager = SpotifyClientCredentials(client_id=ID, client_secret=secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    artist_name = []
    track_name = []
    popularity = []
    track_id = []
    album_name = []
    album_or_single = []
    release_date = []
    
    playlists = ["5XALIurWS8TuF6kk8bj438"]
    
    for playlist_id in playlists:
        for i in range(0,1000,50):
            track_results = sp.playlist_tracks(playlist_id, limit=100, offset=i)
            for t in track_results['items']:
                track = t['track']
                artist_name.append(track['artists'][0]['name'])
                track_name.append(track['name'])
                track_id.append(track['id'])
                popularity.append(track['popularity'])
                album_name.append(track['album']["name"])
                album_or_single.append(track["album"]["album_type"])
                release_date.append(track['album']['release_date'])

    #saving data pulled as df
    track_dataframe = pd.DataFrame({
        'artist_name' : artist_name, 
        'track_name' : track_name, 
        'track_id' : track_id, 
        'popularity' : popularity, 
        'album_name': album_name, 
        'album_or_single': album_or_single,
        'release_date': release_date 
    })

    #adding a couple columns to the df and 
    track_dataframe['year'] =  track_dataframe['release_date'].str[0:4].astype(int)
    track_dataframe['artist_id'] = track_dataframe['artist_name'].apply(generate_hash_id)
    track_dataframe = track_dataframe.drop_duplicates()

    #save df as csv
    track_dataframe.to_csv("/home/lexi/airflow/dags/data/spotify_pull.csv", index = False)
    

# Load the new individual survey data
def _load_data(ti):
    
    print(os.getcwd())
    files_and_dirs = os.listdir(os.getcwd())
    mxmh_df= pd.read_csv("airflow/dags/data/mxmh_survey_results.csv")
    mxmh_df = mxmh_df.dropna(subset=['Age', 'Music effects'])
    mxmh_df['Age'] = mxmh_df['Age'].astype(int)
    mxmh_df["individual_id"] = mxmh_df.index + 1
    individual_survey = mxmh_df[['individual_id','Age', 'Fav genre', 'Music effects', 'Hours per day', 'Anxiety','Depression','Insomnia' ]]
    individual_survey = individual_survey.rename(columns={'Age': 'age',
                                      'Fav genre': 'fav_genre',
                                      'Music effects': 'music_effects',
                                      'Hours per day': 'listening_hours',
                                      'Anxiety': 'anxiety',
                                      'Depression': 'depression',
                                       'Insomnia': 'insomnia'
                                             }) 

    # Create numeric values for effects
    effects_dict = {'Improve': 1,'No effect': 0,'Worsen': -1}
    individual_survey['music_effects_num'] = individual_survey['music_effects'].map(effects_dict)
    individual_survey['genre_id']=None
    individual_survey['genre_id']=individual_survey['fav_genre'].apply(generate_hash_id)
    data=individual_survey[['individual_id', 'fav_genre', 'genre_id', 'age', 'music_effects', 'listening_hours', 'anxiety', 'depression', 'insomnia', 'music_effects_num']]

    # Convert dataframe to list of dictionaries before pushing to xcom
    data_dict = data.to_dict(orient='records')

    # Used to push to another task
    ti.xcom_push(key="data", value=data_dict)

    

    #load in and manipulate the artists.csv along with the spotify csv
    files_and_dirs = os.listdir(os.getcwd())
    artist_df = pd.read_csv("/home/lexi/airflow/dags/data/artists.csv",low_memory=False)
    spotify_df = pd.read_csv("/home/lexi/airflow/dags/data/spotify_pull.csv")


    
    #prepping tha artists table
    artist_df = artist_df[artist_df['scrobbles_lastfm'] > 10000 ][['artist_lastfm','country_lastfm','tags_lastfm','listeners_lastfm','scrobbles_lastfm']]
    
    spotify_df['artist_name_lower'] = spotify_df['artist_name'].str.lower()
    
    artist_df['artist_lastfm_lower'] = artist_df['artist_lastfm'].str.lower()
    artist_df_merge = pd.merge(spotify_df['artist_name_lower'], artist_df, left_on='artist_name_lower', right_on='artist_lastfm_lower', how='inner')
    
    spotify_df = spotify_df.drop('artist_name_lower', axis=1)
    
    artist_df_merge = artist_df_merge.drop('artist_lastfm_lower', axis=1)
    
    artist_df_merge['artist_id'] = artist_df_merge['artist_lastfm'].apply(generate_hash_id)
    artist_df_merge = artist_df_merge[['artist_id','artist_lastfm', 'country_lastfm', 'tags_lastfm', 'listeners_lastfm', 'scrobbles_lastfm']]
    artist_df_merge.columns = ['artist_id','artist_name', 'country', 'tags', 'listeners', 'plays']
    artist_df_merge = artist_df_merge.drop_duplicates()
    artist_df_merge['country_id']= artist_df_merge['country'].apply(generate_hash_id)
    artist_final = artist_df_merge[['artist_id', 'artist_name', 'country_id','listeners', 'plays']]
    
    # Convert dataframe to list of dictionaries before pushing to xcom
    data_dict_artist = artist_final.to_dict(orient='records')
    # Used to push to another task
    ti.xcom_push(key="artists", value=data_dict_artist)


    #prepping spotify filtered
    tst_artist_join = pd.merge(spotify_df, artist_final, left_on='artist_id', right_on='artist_id', how='left')
    orphan2 = tst_artist_join[tst_artist_join['artist_name_y'].isna()].drop_duplicates()
    spotify_df_filtered = spotify_df[~spotify_df['artist_name'].isin(orphan2['artist_name_x'])]
    data_dict_spotify = spotify_df_filtered.to_dict(orient='records')
    ti.xcom_push(key="spotify", value=data_dict_spotify)
    
    #prepping artist_genres mapping
    artist_df_merge['tags'] = artist_df_merge['tags'].str.split('; ')
    artist_genres_mapping = artist_df_merge[['artist_id','tags']].explode('tags')
    data_dict_artist_genre = artist_genres_mapping.to_dict(orient='records')
    ti.xcom_push(key="artists_genre", value=data_dict_artist_genre)


    
    #prepping artist_country_mapping
    artist_df_merge['country'] = artist_df_merge['country'].str.split('; ')
    artist_country_mapping = artist_df_merge[['artist_id','country']].explode('country')
    artist_country_mapping['country_id'] = artist_country_mapping['country'].apply(generate_hash_id)
    artist_country_mapping = artist_country_mapping[['artist_id','country_id','country']]
    data_dict_artist_country = artist_country_mapping.to_dict(orient='records')
    ti.xcom_push(key="artists_country", value=data_dict_artist_country)

    #prepping country
    country = artist_country_mapping.groupby(['country_id','country'], dropna = False).count().reset_index()[['country_id','country']]
    data_dict_country = country.to_dict(orient='records')
    ti.xcom_push(key="country", value=data_dict_country)

    
    
    

# Summary of the survey
def _process_data(ti):
    # Convert list of dictionaries back to a dataframe for processing
    data_dict = ti.xcom_pull(key="data")
    data = pd.DataFrame(data_dict)
    # print(data)
    print("_process_data:")
    
    genre_results = data.groupby('fav_genre').agg(
                   age_mean =pd.NamedAgg(column='age', aggfunc='mean'),
                   hours_mean =pd.NamedAgg(column='listening_hours', aggfunc='mean'),
                   effects_mean=pd.NamedAgg(column='music_effects_num', aggfunc='mean'),
                   anxiety_mean=pd.NamedAgg(column='anxiety', aggfunc='mean'),
                   depression_mean=pd.NamedAgg(column='depression', aggfunc='mean'),
                   insomnia_mean=pd.NamedAgg(column='insomnia', aggfunc='mean'),
                   count=pd.NamedAgg(column='age', aggfunc='count')
                ).reset_index()

    
    genre_results['genre_id']=genre_results['fav_genre'].apply(generate_hash_id)

    genre_results = genre_results[['genre_id','fav_genre', 'age_mean', 'hours_mean', 'effects_mean', 'anxiety_mean',
       'depression_mean', 'insomnia_mean', 'count']]
    
    # Convert resulting dataframe back to a list of dictionaries before pushing to xcom
    genre_results_dict = genre_results.to_dict(orient='records')

    print(genre_results_dict)

    ti.xcom_push(key="genre_results", value=genre_results_dict)


# Load 'individual_survey' table data to PostgreSQL database
def _load_sql_individual(ti):
    # Pull data from xcom and convert the list of dictionaries to dataframe before loading data
    data_dict = ti.xcom_pull(key="data")
    data = pd.DataFrame(data_dict)
    print("_load_sql_individual")
    
    # Create a connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname='lexi',
        user='lexi',
        password='lexi123',
        host='postgres',
        port='5432'
    )
    
    # Create a cursor object
    cur = conn.cursor()
    
    # Insert data query for 'individual_survey' table
    insert_query = """
        INSERT INTO music.individual_survey (
            individual_id, genre_id, age, music_effects, 
            listening_hours, anxiety, depression, insomnia, music_effects_num
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (individual_id) DO UPDATE
        SET 
            genre_id = EXCLUDED.genre_id,
            age = EXCLUDED.age,
            music_effects = EXCLUDED.music_effects,
            listening_hours = EXCLUDED.listening_hours,
            anxiety = EXCLUDED.anxiety,
            depression = EXCLUDED.depression,
            insomnia = EXCLUDED.insomnia,
            music_effects_num = EXCLUDED.music_effects_num;
    """
    
    # Iterate through the DataFrame and execute the insert query
    for index, row in data.iterrows():
        cur.execute(
            sql.SQL(insert_query),
            (row['individual_id'], row['genre_id'], row['age'], row['music_effects'], 
             row['listening_hours'], row['anxiety'], row['depression'], 
             row['insomnia'], row['music_effects_num'])
        )
    
    # Commit and close connection
    conn.commit()
    cur.close()
    conn.close()
    
    return "end"


# Load 'genre_results' table data to PostgreSQL database
def _load_sql_genre_results(ti):
    # Convert list of dictionaries to dataframe before loading data
    genre_results_dict = ti.xcom_pull(key="genre_results")
    genre_results = pd.DataFrame(genre_results_dict)
    print("_load_sql_genre_results")
    
    # Create a connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname='lexi',
        user='lexi',
        password='lexi123',
        host='postgres',
        port='5432'
    )
    
    # Create a cursor object
    cur = conn.cursor()
    
    # Insert data query for 'genre_results' table
    insert_query = """
        INSERT INTO music.genre_results (
            genre_id, fav_genre, age_mean, hours_mean, effects_mean, 
            anxiety_mean, depression_mean, insomnia_mean, count
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (genre_id) DO UPDATE
        SET 
            fav_genre = EXCLUDED.fav_genre,
            age_mean = EXCLUDED.age_mean,
            hours_mean = EXCLUDED.hours_mean,
            effects_mean = EXCLUDED.effects_mean,
            anxiety_mean = EXCLUDED.anxiety_mean,
            depression_mean = EXCLUDED.depression_mean,
            insomnia_mean = EXCLUDED.insomnia_mean,
            count = EXCLUDED.count;
    """
    
    # Iterate through the DataFrame and execute the insert query
    for index, row in genre_results.iterrows():
        cur.execute(
            sql.SQL(insert_query),
            (row['genre_id'], row['fav_genre'], row['age_mean'], row['hours_mean'], 
                row['effects_mean'], row['anxiety_mean'], row['depression_mean'], 
                row['insomnia_mean'], row['count'])
        )
    
    # Commit and close connection
    conn.commit()
    cur.close()
    conn.close()
    
    return "end"

# Load 'artists' table data to PostgreSQL database
def _load_sql_artists(ti):
    # Convert list of dictionaries to dataframe before loading data
    artists_dict = ti.xcom_pull(key="artists")
    artists_df = pd.DataFrame(artists_dict)
    print("_load_sql_artists")
    
    # Create a connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname='lexi',
        user='lexi',
        password='lexi123',
        host='postgres',
        port='5432'
    )
    
    # Create a cursor object
    cur = conn.cursor()
    
    # Insert data query for 'spotify_songs' table
    insert_query = """
        INSERT INTO music.artists (
            artist_id, artist_name, listeners, plays
        ) VALUES (%s, %s, %s, %s)
        ON CONFLICT (artist_id) DO UPDATE
        SET 
            artist_name = EXCLUDED.artist_name,
            listeners = EXCLUDED.listeners,
            plays = EXCLUDED.plays
    """
    
    # Iterate through the DataFrame and execute the insert query
    for index, row in artists_df.iterrows():
        cur.execute(
            sql.SQL(insert_query),
            (row['artist_id'], row['artist_name'], row['listeners'], row['plays'])
        )
    
    # Commit and close connection
    conn.commit()
    cur.close()
    conn.close()
    
    return "end"





def _load_sql_spotify(ti):
    # Convert list of dictionaries to dataframe before loading data
    spotify_dict = ti.xcom_pull(key="spotify")
    spotify_df = pd.DataFrame(spotify_dict)
    
    print("_load_sql_spotify")
    
    # Create a connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname='lexi',
        user='lexi',
        password='lexi123',
        host='postgres',
        port='5432'
    )
    
    # Create a cursor object
    cur = conn.cursor()
    
    # Insert data query for 'spotify_songs' table
    insert_query = """
        INSERT INTO music.spotify_songs (
            track_id, track_name, artist_id, popularity, album_name, 
            album_or_single, release_date, year
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (track_id) DO UPDATE
        SET 
            track_name = EXCLUDED.track_name,
            artist_id = EXCLUDED.artist_id,
            popularity = EXCLUDED.popularity,
            album_name = EXCLUDED.album_name,
            album_or_single = EXCLUDED.album_or_single,
            release_date = EXCLUDED.release_date,
            year = EXCLUDED.year;
    """
    
    # Iterate through the DataFrame and execute the insert query
    for index, row in spotify_df.iterrows():
        cur.execute(
            sql.SQL(insert_query),
            (row['track_id'], row['track_name'], row['artist_id'], row['popularity'], 
                row['album_name'], row['album_or_single'], row['release_date'], 
                row['year'])
        )
    
    # Commit and close connection
    conn.commit()
    cur.close()
    conn.close()
    
    return "end"




# Load 'artists_genres_mapping' table data to PostgreSQL database
def _load_sql_artist_genres(ti):
    # Convert list of dictionaries to dataframe before loading data
    artists_genres_dict = ti.xcom_pull(key="artists_genres")
    artists_genres_mapping = pd.DataFrame(artists_genres_dict)
    print("_load_sql_artist_genres")
    
    # Create a connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname='lexi',
        user='lexi',
        password='lexi123',
        host='postgres',
        port='5432'
    )
    
    # Create a cursor object
    cur = conn.cursor()

    # Insert data query for 'spotify_songs' table
    insert_query = """
        INSERT INTO music.artist_genres_mapping (artist_id, genre_id) VALUES (%s,%s)
        ON CONFLICT (artist_id, genre_id) DO NOTHING
    """
    
    # Iterate through the DataFrame and execute the insert query
    for i, row in artists_genres_mapping.iterrows(): 
        cur.execute(sql.SQL(insert_query), (row['artist_id'],row['genre_id']))
        
    # Commit and close connection
    conn.commit()
    cur.close()
    conn.close()
    
    return "end"


# Load 'artists_genres_mapping' table data to PostgreSQL database
def _load_sql_artist_country(ti):
    # Convert list of dictionaries to dataframe before loading data
    artists_country_dict = ti.xcom_pull(key="artists_country")
    artists_country_mapping = pd.DataFrame(artists_country_dict)
    print("_load_sql_artist_country")
    
    # Create a connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname='lexi',
        user='lexi',
        password='lexi123',
        host='postgres',
        port='5432'
    )
    
    # Create a cursor object
    cur = conn.cursor()

    # Insert data query for 'spotify_songs' table
    insert_query = """
        INSERT INTO music.artist_country_mapping (artist_id, country_id) VALUES (%s,%s)
        ON CONFLICT (artist_id, country_id) DO NOTHING
    """
    
    # Iterate through the DataFrame and execute the insert query
    for i, row in artists_country_mapping.iterrows(): 
        cur.execute(sql.SQL(insert_query),  (row['artist_id'],row['country_id']))
        
    # Commit and close connection
    conn.commit()
    cur.close()
    conn.close()
    
    return "end"


# Load 'artists_genres_mapping' table data to PostgreSQL database
def _load_sql_country(ti):
    # Convert list of dictionaries to dataframe before loading data
    country_dict = ti.xcom_pull(key="country")
    country = pd.DataFrame(country_dict)
    print("_load_sql_artist_country")
    
    # Create a connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname='lexi',
        user='lexi',
        password='lexi123',
        host='postgres',
        port='5432'
    )
    
    # Create a cursor object
    cur = conn.cursor()

    # Insert data query for 'spotify_songs' table
    insert_query = """
        INSERT INTO music.country (country_id, country) VALUES (%s,%s)
        ON CONFLICT (country_id) DO UPDATE
        SET 
            country = EXCLUDED.country
    """
    
    # Iterate through the DataFrame and execute the insert query
    for i, row in country.iterrows(): 
        cur.execute(sql.SQL(insert_query),  (row['country_id'],row['country']))  
    
    # Commit and close connection
    conn.commit()
    cur.close()
    conn.close()
    
    return "end"












# Define the default arguments
default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2024, 7, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG("final_music_dag_222", default_args=default_args,
    schedule_interval="@daily", catchup=False) as dag:


        create_tables = PythonOperator(
            task_id="create_schema_and_tables",
            python_callable = create_schema_and_tables
        )
    
        spotify_api = PythonOperator(
            task_id="spotify_api",
            python_callable = spotify_pull
        )
    
        load_data = PythonOperator(
            task_id="load_data",
            python_callable = _load_data
        )

        process_data = PythonOperator(
            task_id="process_data",
            python_callable = _process_data
        )

        artists = PythonOperator(
            task_id="artists",
            python_callable = _load_sql_artists
        )
    
        genre_results = PythonOperator(
            task_id="genre_results",
            python_callable = _load_sql_genre_results
        )

        country = PythonOperator(
            task_id="country",
            python_callable = _load_sql_country
        )

        artist_country = PythonOperator(
            task_id="artist_country",
            python_callable = _load_sql_artist_country
        )
    
        spotify = PythonOperator(
            task_id="spotify",
            python_callable = _load_sql_spotify
        )

        artist_genre = PythonOperator(
            task_id="artist_genre",
            python_callable = _load_sql_artist_genres
        )    

        individual = PythonOperator(
            task_id="individual",
            python_callable = _load_sql_individual
        )
    

    
        create_tables >> spotify_api >> load_data >> process_data >> artists >> genre_results >> country >> artist_country >> spotify >> artist_genre >> individual

