import gzip
import io
import re
import csv
import requests

URL = "https://github.com/casadosdados/consulta-cep/releases/download/0.0.2/cep-20190602.csv.gz"
OUT = "ceps_10000_reais.csv"
LIMITE = 10000

r = requests.get(URL, timeout=120)
r.raise_for_status()

ceps = set()
cep_re = re.compile(r"\b(\d{5}-?\d{3})\b")  # pega 12345-678 ou 12345678

with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as gz:
    for raw in gz:
        line = raw.decode("utf-8", errors="ignore")
        m = cep_re.search(line)
        if not m:
            continue

        cep = m.group(1).replace("-", "")
        if len(cep) != 8:
            continue

        ceps.add(cep)
        if len(ceps) >= LIMITE:
            break

with open(OUT, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["cep"])
    for cep in sorted(ceps):
        w.writerow([cep])

print(f"Gerado: {OUT} com {len(ceps)} CEPs")
