# producer.py
# Task 6: Producer Script
# Role: Only send seed URLs to RabbitMQ queue

import pika

# --- RabbitMQ Connection ---
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# Task 5: Create durable queue
channel.queue_declare(queue='url_queue', durable=True)
# --- Purge queue so old URLs are removed ---
channel.queue_purge(queue='url_queue')
print("[PRODUCER] Cleared old URLs from queue")


# Seed URLs (small site, ~10–15 pages)
seed_urls = [
    'https://www.khanacademy.org/',
    'https://www.stackoverflow.com/',
    'https://www.example.com/'
]

# Publish seed URLs to queue
for url in seed_urls:
    channel.basic_publish(
        exchange='',
        routing_key='url_queue',
        body=url,
        properties=pika.BasicProperties(
            delivery_mode=2  # persistent
        )
    )
    print(f"[PRODUCER] Sent URL: {url}")

connection.close()
print("[PRODUCER] All seed URLs sent")
