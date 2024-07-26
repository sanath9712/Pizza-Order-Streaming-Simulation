from threading import Thread
from faker import Faker
import json
from kafka import KafkaProducer, KafkaConsumer
import random
import time

# Initialize Faker
fake = Faker()

def produce_orders(producer, topic):
    while True:
        order = {
            "customer_name": fake.name(),
            "customer_address": fake.address(),
            "pizza_size": random.choice(["Small", "Medium", "Large"]),
            "toppings": random.sample(["Pepperoni", "Mushrooms", "Onions", "Sausage", "Bacon", "Extra Cheese", "Black Olives", "Green Peppers"], k=random.randint(2, 5)),
            "quantity": random.randint(1, 5)
        }
        producer.send(topic, value=order)
        producer.flush()
        print(f"Produced order: {order}")
        with open('orders.log', 'a') as f:
            f.write(f"Produced order: {order}\n")
        time.sleep(1)  # Produce an order every 10 seconds

def consume_orders(consumer):
    for message in consumer:
        order = message.value
        chef_name = f"Chef {fake.first_name()}"
        print(f"{chef_name} is preparing order: {order}")
        with open('chef_allocations.log', 'a') as f:
            f.write(f"{chef_name} is preparing order: {order}\n")

if __name__ == "__main__":
    topic = 'pizza_orders'
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='kitchen-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    
    # Start producer thread
    producer_thread = Thread(target=produce_orders, args=(producer, topic))
    producer_thread.daemon = True
    producer_thread.start()
    
    # Start consumer in the main thread
    consume_orders(consumer)