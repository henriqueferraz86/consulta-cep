# Consulta CEP - Documentacao

Este projeto faz consulta de CEPs (ViaCEP), grava em arquivos e/ou em bancos
PostgreSQL e MongoDB, e oferece duas formas de consumo: uma API (FastAPI) e
uma UI (Streamlit).

## Visao geral das aplicacoes

- **consulta-cep copy.py**: coletor principal. Le um CSV de CEPs, consulta o
  ViaCEP com paralelismo, grava os dados no Postgres e no MongoDB e gera
  artefatos (enderecos.json/xml). Inclui fila para escrita no DB, rate limit
  e retries.
- **consulta-cep.py**: versao simples/legada do coletor (menos recursos).
- **main.py**: API FastAPI para consultar enderecos no banco.
- **app.py**: UI Streamlit para consultar dados via JSON, Postgres ou MongoDB.
- **download-ceps.py**: baixa um conjunto de CEPs reais e gera um CSV.

## Arquivos e o que fazem

- `.env`: configuracoes do Postgres e do MongoDB (veja abaixo).
- `requirements.txt`: dependencias Python.
- `docker-compose.yml`: sobe Postgres e MongoDB.
- `docker-compose copy.yml`: copia do compose (mesmos servicos).
- `init.sql`: cria a tabela `public.enderecos`.
- `limpar_tabelas.sql`: limpa a tabela `public.enderecos`.
- `ceps.csv` / `ceps copy.csv`: entrada com coluna `cep`.
- `enderecos.json`: saida com enderecos coletados (JSON).
- `enderecos.xml`: saida com enderecos coletados (XML).
- `erros.log`: log de erros (HTTP/DB/etc).
- `erros_consulta.csv`: CSV com CEPs que falharam na consulta (gerado em tempo real).
- `erros copy.log`: copia de log (historico).
- `bd.py`: funcao simples para inserir um endereco no Postgres.

## Fluxo de dados (coleta)

1) Ler `ceps.csv` (coluna `cep`) e normalizar para 8 digitos.  
2) Consultar o ViaCEP por HTTP.  
3) Salvar no Postgres (fila + worker) e no MongoDB e/ou em arquivos
   `enderecos.json/xml`.  
4) Registrar falhas em `erros.log` e `erros_consulta.csv`.

## Banco de dados

Tabela `public.enderecos` (ver `init.sql`):
- `cep` (unico), `logradouro`, `bairro`, `localidade`, `uf`, etc.

Colecao MongoDB:
- `enderecos` (documentos com campos iguais ao ViaCEP).

## .env (exemplo)

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ceps
DB_USER=cepuser
DB_PASSWORD=ceppass

MONGO_URI=mongodb://mongouser:mongopass@localhost:27017
MONGO_DB=ceps
MONGO_COLLECTION=enderecos
```

## Como executar (resumo)

### 1) Subir o Postgres e o MongoDB (opcional, se for usar DB)
```
docker compose up -d
```

### 2) Coletar CEPs (principal)
```
python "consulta-cep copy.py"
```

### 3) API FastAPI
```
uvicorn main:app --reload
```

### 4) UI Streamlit
```
streamlit run app.py
```

## Observacoes

- A API e a UI usam o banco (se a fonte for "Banco") e/ou o MongoDB.
- O coletor grava no banco em background; os inserts aparecem no fim do
  processamento (ou conforme o log de stats).
- Se precisar limpar o Postgres, use `limpar_tabelas.sql`.
