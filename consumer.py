from kafka import KafkaConsumer

#initialize the consumer
consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],consumer_timeout_ms=60000) # 60 seconds
consumer.subscribe(["products-topic","users-topic"])

products = []
users = []

#Receive the messages and separate them by topic
for msg in consumer:
    if("products-topic" in str(msg)): products.append(msg)
    if("users-topic" in str(msg)): users.append(msg)

#Write the products to a file
f = open("products.txt", "w")
for product in products:
    product = str(product).partition('value=b\'')[2]
    product = product.partition(", headers=")[0][:-1]
    f.write(str(product)+'\n')

#Write the people to a file
f = open("people.txt", "w")
for user in users:
    user = (str(user).partition('value=b"')[2])
    user = user.partition(", headers=")[0][:-1]
    user = user.replace("\'","\"")
    f.write(str(user)+'\n')