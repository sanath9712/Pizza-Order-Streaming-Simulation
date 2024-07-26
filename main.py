from threading import Thread
from faker import Faker
import json
import random
import time
from kafka import KafkaProducer, KafkaConsumer
from collections import defaultdict

# Initialize Faker
fake = Faker()

# Define restaurants and their chefs
restaurants = {
    'Restaurant A': {'chefs': ['Alice', 'Aaron'], 'orders': defaultdict(int)},
    'Restaurant B': {'chefs': ['Bob', 'Bella'], 'orders': defaultdict(int)},
    'Restaurant C': {'chefs': ['Charlie', 'Chloe'], 'orders': defaultdict(int)},
    'Restaurant D': {'chefs': ['Dave', 'Diana'], 'orders': defaultdict(int)},
    'Restaurant E': {'chefs': ['Eve', 'Ethan'], 'orders': defaultdict(int)}
}

def choose_least_loaded_restaurant():
    """ Choose the restaurant with the least orders pending """
    return min(restaurants, key=lambda r: sum(restaurants[r]['orders'].values()))

def choose_least_loaded_chef(restaurant):
    """ Within the selected restaurant, choose the chef with the least orders """
    return min(restaurants[restaurant]['chefs'], key=lambda chef: restaurants[restaurant]['orders'][chef])

def produce_orders(producer, topic):
    """ Continuously generate and send pizza orders to a Kafka topic """
    while True:
        order = {
            "customer_name": fake.name(),
            "customer_address": fake.address(),
            "pizza_size": random.choice(["Small", "Medium", "Large"]),
            "toppings": random.sample(["Pepperoni", "Mushrooms", "Onions", "Sausage", "Bacon", "Extra Cheese", "Black Olives", "Green Peppers"], k=random.randint(2, 5)),
            "quantity": random.randint(1, 5),
            "order_id": fake.uuid4()
        }
        producer.send(topic, value=json.dumps(order))
        producer.flush()
        print(f"Produced order: {order}")
        time.sleep(random.randint(1, 5))  # Simulate varying order production times

def consume_orders(consumer, producer, prepared_topic):
    """ Process orders: assign to chefs and simulate preparation and completion """
    for message in consumer:
        order = json.loads(message.value)
        restaurant = choose_least_loaded_restaurant()
        chef = choose_least_loaded_chef(restaurant)
        restaurants[restaurant]['orders'][chef] += 1  # Increment chef's load

        # Simulate order preparation
        time.sleep(random.randint(2, 6))  # Time to prepare the order
        print(f"{chef} at {restaurant} is preparing order {order['order_id']}")

        # Mark the order as prepared and send to delivery topic
        order['chef'] = chef
        order['restaurant'] = restaurant
        producer.send(prepared_topic, value=json.dumps(order))
        producer.flush()

        # Simulate order completion and decrease chef's load
        restaurants[restaurant]['orders'][chef] -= 1

if __name__ == "__main__":
    orders_topic = 'pizza_orders'
    prepared_topic = 'prepared_orders'
    
    # Kafka producer and consumer setup
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: x.encode('utf-8'))
    consumer = KafkaConsumer(
        orders_topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='kitchen-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    # Start the producer thread
    producer_thread = Thread(target=produce_orders, args=(producer, orders_topic))
    producer_thread.daemon = True
    producer_thread.start()

    # Start the consumer thread
    consume_orders_thread = Thread(target=consume_orders, args=(consumer, producer, prepared_topic))
    consume_orders_thread.daemon = True
    consume_orders_thread.start()

    # Run the consumer in the main thread for interactive monitoring
    consume_orders(consumer, producer, prepared_topic)
