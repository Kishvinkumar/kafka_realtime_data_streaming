import uuid    # to generate unique id 
from datetime import datetime
from airflow import DAG  # for scheduling 
from airflow.operators.python import PythonOperator


#assigning owner and start date of the event 

default_args = {
    'owner': 'airscholar',                  
    'start_date': datetime(2023, 9, 3, 10, 00)
}

# getting the data from the API("https://randomuser.me/api/") in the json format
def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]

    return res


#formatiing the data by using the column names and keep it in data{} function
def format_data(res):
    data = {}
    location = res['location']
    data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


#streaming the data 
def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

#creating a producer and send the data to the broker with port 29092
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

#take the messages for every 60sec
    while True:
        if time.time() > curr_time + 60: #1 minute
            break
            
  #creating a topic to get the data from the producer in the json format.          
        try:
            res = get_data()
            res = format_data(res)

            producer.send('users_created', json.dumps(res).encode('utf-8'))
  # if any errors are there just skip and load 
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue


#doing scheduling for getting the data from airflow . its a daily load.
with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:


#create tasks and make it run in the apache airflow
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
    