from time import sleep
from json import dumps
from kafka import KafkaProducer
import configparser
import requests
import sys
 
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                     	value_serializer=lambda x:
                     	dumps(x).encode('utf-8'))


	 
def get_api_key():
	config = configparser.ConfigParser()
	config.read('config.ini')
	return config['openweathermap']['api']

def get_weather(api_key, location):
	url = "https://samples.openweathermap.org/data/2.5/weather?q=London,uk&appid=b6907d289e10d714a6e88b30761fae22".format(location, api_key)
	r = requests.get(url)
	return r.json()

def main():
	    
	while(True):
    	api_key = get_api_key()
    	data = get_weather(api_key, "London")
    	producer.send('numtest', value=data)
    	sleep(5)
   	 
	print(weather['main']['temp'])
	#print(weather)
    


if __name__ == '__main__':
	main()
