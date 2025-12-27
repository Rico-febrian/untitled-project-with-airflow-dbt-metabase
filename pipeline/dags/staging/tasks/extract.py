import json
import pendulum
from datetime import datetime
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

def extract_to_minio(**context):
    
    # Set up variables
    cities = Variable.get("WEATHER_TARGET_CITIES", deserialize_json=True)
    bucket = Variable.get("WEATHER_S3_BUCKET")
    prefix = Variable.get("WEATHER_S3_PATH_PREFIX")
    api_units = Variable.get("WEATHER_API_UNITS")
    api_lang = Variable.get("WEATHER_API_LANG")
    
    # Set up hooks
    http_hook = HttpHook(http_conn_id='open-weather-api', method='GET')
    s3_hook = S3Hook(aws_conn_id='s3-conn')
    
    # Get API Key from connection
    api_key = http_hook.get_connection('open-weather-api').extra_dejson.get('api_key')
    
    # Set incremental logic
    execution_date = context['logical_date'].in_timezone('Asia/Jakarta')
    partition = execution_date.strftime('year=%Y/month=%m/day=%d/hour=%H')
    
    for city in cities:
        
        city_name = city['name']
        country_code = city['country']        
        
        try:
            
            # Finding City Coordinates
            print(f'\nLooking for the coordinates of the city of {city_name}...')
    
            geo_endpoint = f'/geo/1.0/direct?q={city_name},{country_code}&limit=1&appid={api_key}'
            geo_response = http_hook.run(endpoint=geo_endpoint, extra_options={'check_response': False})
            geo_data = geo_response.json()
            
            if geo_response.status_code == 200 and len(geo_data) > 0:
                lat = geo_data[0]['lat']
                lon = geo_data[0]['lon']
                
                print(f'Find coordinates for {city_name}: {lat}, {lon}')
            
                # Finding Current Weather
                print(f'\nCurrent Weather Result')      
                                     
                weather_endpoint = f"/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units={api_units}&lang={api_lang}"
                weather_response = http_hook.run(endpoint=weather_endpoint)
                            
                if weather_response.status_code == 200:
                    weather_data = weather_response.json()
                    
                    weather_data['city_name'] = city_name
                    
                    # Set up file name in MinIO
                    filename = f'{city_name.replace(' ', '_')}.json'
                    s3_key = f'{prefix}/{partition}/{filename}'
                    
                    s3_hook.load_string(
                        string_data=json.dumps(weather_data),
                        key=s3_key,
                        bucket_name=bucket,
                        replace=True
                    )
                    
                    print(f'Sucessfully saved {city_name} to {s3_key}')
                 
                else:
                    print(f'Failed while procesing current weather data: {weather_response.staus_code}')
                
            else:
                print(f'Failed while processing geo location data: {geo_response.status_code}')           
                
        except Exception as e:
            print(f'Error while extracting data: {str(e)}')
