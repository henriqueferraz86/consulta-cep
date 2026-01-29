# Consulta CEP - Documentacao

Este projeto faz consulta de CEPs (ViaCEP), grava em arquivos e/ou no banco
PostgreSQL, e oferece duas formas de consumo: uma API (FastAPI) e uma UI
(Streamlit).

## Visao geral das aplicacoes

- **consulta-cep copy.py**: coletor principal. Le um CSV de CEPs, consulta o
  ViaCEP com paralelismo, grava os dados no banco e gera artefatos
  (enderecos.json/xml). Inclui fila para escrita no DB, rate limit e retries.
- **consulta-cep.py**: versao simples/legada do coletor (menos recursos).
- **main.py**: API FastAPI para consultar enderecos no banco.
- **app.py**: UI Streamlit para consultar dados via JSON ou banco.
- **download-ceps.py**: baixa um conjunto de CEPs reais e gera um CSV.

## Arquivos e o que fazem

- `.env`: configuracao do banco (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD).
- `requirements.txt`: dependencias Python.
- `docker-compose.yml`: sobe o Postgres (container `postgres_ceps`).
- `init.sql`: cria a tabela `public.enderecos`.
- `limpar_tabelas.sql`: limpa a tabela `public.enderecos`.
- `ceps.csv` / `ceps copy.csv`: entrada com coluna `cep`.
- `enderecos.json`: saida com enderecos coletados (JSON).
- `enderecos.xml`: saida com enderecos coletados (XML).
- `erros.log`: log de erros (HTTP/DB/etc).
- `erros_consulta.csv`: CSV com CEPs que falharam na consulta.
- `erros copy.log`: copia de log (historico).
- `bd.py`: funcao simples para inserir um endereco no Postgres.

## Fluxo de dados (coleta)

1) Ler `ceps.csv` (coluna `cep`) e normalizar para 8 digitos.  
2) Consultar o ViaCEP por HTTP.  
3) Salvar no banco (fila + worker) e/ou em arquivos `enderecos.json/xml`.  
4) Registrar falhas em `erros.log` e `erros_consulta.csv`.

## Banco de dados

Tabela `public.enderecos` (ver `init.sql`):
- `cep` (unico), `logradouro`, `bairro`, `localidade`, `uf`, etc.

## Como executar (resumo)

### 1) Subir o Postgres (opcional, se for usar DB)
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

- A API e a UI usam o banco (se a fonte for "Banco").
- O coletor grava no banco em background; os inserts aparecem no fim do
  processamento (ou conforme o log de stats).
- Se precisar limpar o banco, use `limpar_tabelas.sql`.
