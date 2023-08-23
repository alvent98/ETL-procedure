from time import sleep
from kafka import KafkaProducer
import mysql.connector

database = mysql.connector.connect(
  host="localhost",
  user="root",
  password="root",
  database="my_schema"
)

#run the query
cursor = database.cursor()
JSONFriendlyQuery = 'select JSON_OBJECT(\'id\',id,\'circulationYear\',circulationYear,\'country\',country,\'used\',used,\'sampleCondition\',sampleCondition) from stamps'
cursor.execute(JSONFriendlyQuery)
fetched = cursor.fetchall()

stamps = []
for fethedStamp in fetched: stamps.append(fethedStamp)

#initialize the producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic = "products-topic"

currentStamp = 1
#Sending of producer's messages
for stamp in stamps:
    stamp = str(stamp)[2:-3]
    try:
        producer.send(topic, bytes(stamp, "UTF-8"))
    except IndexError as error:
        print(error)
    if(currentStamp % 10 ==0): 
        sleep(20)
    currentStamp += 1