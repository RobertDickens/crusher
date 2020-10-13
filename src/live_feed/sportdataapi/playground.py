import requests

headers = {
  "apikey": "apikeyhere"}

params = (
   ("type","prematch"),
)

response = requests.get('https://app.sportdataapi.com/api/v1/soccer/odds/120423', headers=headers, params=params)
print(response.text)
