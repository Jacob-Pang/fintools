import json
import multiprocessing
import os

from fintools.datareader import get_database
from pyutils.task_scheduler import TaskScheduler

def main():
    auth_cred_file_path = os.path.join(os.path.dirname(__file__), "authentication_credentials.json")

    with open(auth_cred_file_path) as file_input:
        authentication_cred = json.load(file_input)
        access_token = authentication_cred.get("access_token")

    crypto_database = get_database("crypto_database")
    crypto_database.set_access_token(access_token)
    coin_reserves_id_list = ["usdt_reserves", "dai_reserves"]
    
    task_scheduler = TaskScheduler()

    for coin_reserves_id in coin_reserves_id_list:
        coin_reserves_node = crypto_database.get_child_node(coin_reserves_id)

        for task in coin_reserves_node.get_update_tasks(reschedule_on_done=False):
            task_scheduler.submit_task(task)

        task_scheduler.add_resources(*coin_reserves_node.get_update_resources())

    task_scheduler.start(description="Updating coin_reserves")
    task_scheduler.join()

if __name__ == "__main__":
    main()