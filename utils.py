"""File contains the utility functions used across modules."""
import json


def save_data_as_json(file_name, data):
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_json_data(file_name):
    with open(file_name, encoding='utf-8') as f:
        return json.load(f)
