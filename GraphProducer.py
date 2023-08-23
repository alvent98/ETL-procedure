from time import sleep
from kafka import KafkaProducer
from neo4j import GraphDatabase

uri = "neo4j://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "root"))

#the function that runs the query
def get_population(tx): 
    result = tx.run("MATCH (a:person) RETURN a as person")
    users = []
    for population in result:
        for user in population:
            users.append(user)
    return users

usersToBeSent = []
#get and parse the query result
with driver.session() as session:
    users = session.read_transaction(get_population)
    for user in users:
        user = (str(user).partition('erties=')[2])
        user = user[:-1]
        usersToBeSent.append(user)
driver.close()

#initialize the producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic = "users-topic"

currentUser = 1
#Sending of producer's messages
for user in usersToBeSent:
    try:
        producer.send(topic, bytes(user, "UTF-8"))
    except IndexError as error:
        print(error)
    if(currentUser % 5 ==0): 
        sleep(20)
    currentUser += 1