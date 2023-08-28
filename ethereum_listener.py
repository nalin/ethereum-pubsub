import os
import time
import json
from web3 import Web3
from web3.datastructures import AttributeDict
from google.cloud import pubsub_v1
from hexbytes import HexBytes
from io import StringIO

class HexJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, HexBytes):
            return obj.hex()
        if isinstance(obj, AttributeDict):
            return dict(obj)
        return super().default(obj)

def read_latest_block_number(file_name="latest_block.txt"):
    try:
        with open(file_name, "r") as f:
            return int(f.read())
    except FileNotFoundError:
        return None

def write_latest_block_number(block_number, file_name="latest_block.txt"):
    with open(file_name, "w") as f:
        f.write(str(block_number))

def create_web3_instance():
    provider_url = os.environ.get("ETHEREUM_PROVIDER_URL")
    if not provider_url:
        raise ValueError("ETHEREUM_PROVIDER_URL environment variable is required")
    return Web3(Web3.HTTPProvider(provider_url))

def init_pubsub_publisher():
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT_ID")
    topic_id = os.environ.get("PUBSUB_TOPIC_ID")
    if not project_id or not topic_id:
        raise ValueError("Both GOOGLE_CLOUD_PROJECT_ID and PUBSUB_TOPIC_ID environment variables are required")
    return pubsub_v1.PublisherClient(), f"projects/{project_id}/topics/{topic_id}"

def listen_and_publish_transactions(web3, publisher, topic_path):
    io = StringIO()
    stored_block = read_latest_block_number()
    current_block = stored_block if stored_block is not None else web3.eth.block_number
    while True:
        new_block = web3.eth.block_number
        if new_block > current_block:
            print(new_block)
            for tx_hash in web3.eth.get_block(new_block).transactions:
                tx = web3.eth.get_transaction(tx_hash)
                tx_dict = dict(tx)
                tx_json = json.dumps(tx_dict, cls=HexJsonEncoder)
                print(tx_json)
                publisher.publish(topic_path, tx_json.encode('utf-8'))
            current_block = new_block
            write_latest_block_number(current_block)
        time.sleep(1)

if __name__ == "__main__":
    print('creating web3 instance...')
    web3_instance = create_web3_instance()
    print('creating pubsub publisher...')
    pubsub_publisher, topic_path = init_pubsub_publisher()
    print('listening for txns...')
    listen_and_publish_transactions(web3_instance, pubsub_publisher, topic_path)