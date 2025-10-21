import requests, json

response = requests.get("https://www.freetogame.com/api/games")
# print(response.json())


# create a formatted string of the Python JSON object
def jprint(obj):
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)

jprint(response.json())