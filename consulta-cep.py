import requests, json, csv


CSV = "ceps.csv"
OUTPUT_JSON = "enderecos.json"
OUTPUT_CSV = "endereços.csv"

API_URL = f"https://viacep.com.br/ws/{cep}/json/"
