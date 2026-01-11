# worker.py
# Task 3-9: Worker Consumer Script
# Role: Consume URL, crawl, save HTML, extract links, send back to queue
# Supports multiple workers with shared visited set

import pika
import requests
import os
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from multiprocessing import Process, Manager, current_process, freeze_support

# --- Configuration ---
PAGES_DIR = "pages"
MAX_URLS = 5  # Each worker crawls at most 5 URLs
os.makedirs(PAGES_DIR, exist_ok=True)

# --- Utility ---
def is_valid_http(url):
    return url.startswith("http://") or url.startswith("https://")

# --- Worker Function ---
def run_worker(worker_id, visited_global):
    processed_count = 0  # Count per worker
    print(f"[Worker-{worker_id}] Starting...")

    # RabbitMQ Connection
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='url_queue', durable=True)
    channel.basic_qos(prefetch_count=1)  # Fair dispatch

    # --- Callback function ---
    def callback(ch, method, properties, body):
        nonlocal processed_count
        url = body.decode()

        if url in visited_global:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        print(f"[Worker-{worker_id}] Crawling: {url}")
        try:
            response = requests.get(url, timeout=5)
            html = response.text
        except Exception as e:
            print(f"[Worker-{worker_id}] Failed to fetch {url}: {e}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Save HTML
        filename = os.path.join(PAGES_DIR, f"{worker_id}_{len(visited_global)}.html")
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html)

        visited_global[url] = True
        processed_count += 1

        # Extract links
        soup = BeautifulSoup(html, "html.parser")
        base_domain = urlparse(url).netloc

        for tag in soup.find_all("a", href=True):
            link = urljoin(url, tag['href'])
            parsed = urlparse(link)
            if is_valid_http(link) and parsed.netloc == base_domain and link not in visited_global:
                channel.basic_publish(
                    exchange='',
                    routing_key='url_queue',
                    body=link,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                print(f"[Worker-{worker_id}] Queued link: {link}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

        # Stop if MAX_URLS reached
        if processed_count >= MAX_URLS:
            print(f"[Worker-{worker_id}] Reached {MAX_URLS} URLs. Exiting...")
            ch.stop_consuming()

    # Start consuming
    channel.basic_consume(queue='url_queue', on_message_callback=callback)
    channel.start_consuming()
    connection.close()

# --- Main: Launch Multiple Workers ---
if __name__ == "__main__":
    freeze_support()  # Required for Windows
    manager = Manager()
    visited_global = manager.dict()  # Shared visited URLs

    num_workers = 3  # Number of parallel workers
    workers = []

    for i in range(num_workers):
        p = Process(target=run_worker, args=(f"W{i+1}", visited_global))
        p.start()
        workers.append(p)

    for p in workers:
        p.join()

    print("[MASTER] All workers finished.")
