import logging
from pprint import pformat


def print_user_details():
    details = {
        "name": "Shaik Malik Basha",
        "age": 26,
        "role": "Member Software Engineer",
    }
    logging.info(f"Details: {details}")
    print(pformat(details))
