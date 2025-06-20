from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import pendulum
import requests
import json


#Latitude and Longitude of Atlanta, GA
LATITUDE = '33.753746'
LONGITUDE = '-84.386330'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args ={
    'start_date': pendulum.datetime(2025, 6, 15, 0, 0, tz="EST"),
    
}

#DAG (Directed Acyclic Graph)
with DAG(dag_id='weather_etl_pipeline',
         default_args = default_args,
         schedule = '@daily',
         catchup = False) as dags:
    
    @task()
    def extract_weather_data():
        #Exract API Weather Data Using Airflow, Start With HTTP HOOKS



        http_hook = HttpHook(http_conn_id=API_CONN_ID,method='GET')

        #Build API endpoint
        endpoint =f'data/3.0/onecall?lat={LATITUDE}&lon={LONGITUDE}&exclude=minutely,current,alerts,daily&units=imperial&appid=mypersonalapikey'

        #Make Request via Http hook
        response=http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.staus_code}")
        
    @task()
    def transform_weather_data(weather_data:dict)-> list[dict]:
        rows = []
        for hour in weather_data.get('hourly', []):
            rows.append({
                'lat': LATITUDE,
                'lon': LONGITUDE,
                'dt': hour['dt'],
                'temp': hour['temp'],
                'pressure': hour['pressure'],
                'humidity': hour['humidity'],
                'dew_point': hour['dew_point'],
                'uvi': hour['uvi'],
                'clouds': hour['clouds'],
                'wind_speed': hour['wind_speed'],
                'wind_deg': hour['wind_deg'],
                'wind_gust': hour.get('wind_gust'),
                'description': hour["weather"][0]['description']
                
                
                
                
                })
            
        return rows
    
    @task()
    def load_weather_data(rows: list[dict])-> None:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        #Create Table in postgres
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (           
            lat FLOAT,
            lon FLOAT,
            dt BIGINT,
            temp FLOAT,
            pressure FLOAT,
            humidity INT,
            dew_point FLOAT,
            uvi FLOAT,
            clouds INT,
            wind_speed FLOAT,
            wind_deg INT,
            wind_gust FLOAT,
            description VARCHAR(50)
        );
        """)

        #Insert data into table
        insert_sql = """
            INSERT INTO weather_data 
            (lat, lon, dt, temp, pressure, humidity, dew_point, uvi, clouds, wind_speed, wind_deg, wind_gust, description)
           VALUES (%(lat)s, %(lon)s, %(dt)s, %(temp)s, %(pressure)s, %(humidity)s, %(dew_point)s, %(uvi)s, %(clouds)s, %(wind_speed)s, %(wind_deg)s, %(wind_gust)s, %(description)s)
       
            
            
        
    """
        
        cursor.executemany(insert_sql, rows)
        conn.commit()

    #DAG Workflow ETL Pipe line
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)





        

    
         
