import json
import os
import requests


HERE = os.path.dirname(os.path.abspath(__file__))
REMOTE_DATA_JSON = os.path.join(HERE, 'remote_data.json')


def main():
    with open(REMOTE_DATA_JSON, 'r') as f:
        data = json.load(f)
    for name, url in data.items():
        print(f'Downloading {name}...')
        response = requests.get(url)
        with open(os.path.join(HERE, name), 'wb') as f:
            f.write(response.content)


if __name__ == '__main__':
    main()
