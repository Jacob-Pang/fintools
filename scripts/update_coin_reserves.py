import json
import os

from fintools.datareader import get_database
from pyutils.pytask_scheduler import run_pytasks_scheduler

def main():
    auth_cred_file_path = os.path.join(os.path.dirname(__file__), "authentication_credentials.json")

    with open(auth_cred_file_path) as file_input:
        authentication_cred = json.load(file_input)
        access_token = authentication_cred.get("access_token")

    crypto_database = get_database("crypto_database")
    crypto_database.set_access_token(access_token)
    coin_reserves_id_list = ["usdt_reserves", "dai_reserves"]

    pytasks = []

    for coin_reserves_id in coin_reserves_id_list:
        coin_reserves_node = crypto_database.get_child_node(coin_reserves_id)
        pytasks.extend(coin_reserves_node.get_update_pytasks(repeat_tasks=False))

    run_pytasks_scheduler(pytasks)

if __name__ == "__main__":
    main()