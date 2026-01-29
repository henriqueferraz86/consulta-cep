import os
from typing import Optional, List

from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()

def get_engine() -> Engine:
    user = os.getenv("DB_USER")
    pwd = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")

    if not all([user, pwd, host, port, db]):
        raise RuntimeError("Faltando variáveis no .env (DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD)")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)

engine = get_engine()

app = FastAPI(title="Consulta CEP API", version="1.0.1")


class Endereco(BaseModel):
    cep: str
    logradouro: Optional[str] = None
    complemento: Optional[str] = None
    unidade: Optional[str] = None
    bairro: Optional[str] = None
    localidade: Optional[str] = None
    uf: Optional[str] = None
    estado: Optional[str] = None
    regiao: Optional[str] = None
    ibge: Optional[str] = None
    gia: Optional[str] = None
    ddd: Optional[str] = None
    siafi: Optional[str] = None


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/enderecos/{cep}", response_model=Endereco)
def get_endereco(cep: str):
    cep = "".join(c for c in cep if c.isdigit())
    if len(cep) != 8:
        raise HTTPException(status_code=400, detail="CEP inválido (precisa ter 8 dígitos).")

    sql = text(r"""
        SELECT
            cep, logradouro, complemento, unidade, bairro,
            localidade, uf, estado, regiao, ibge, gia, ddd, siafi
        FROM enderecos
        WHERE regexp_replace(cep, '\D', '', 'g') = :cep
        LIMIT 1
    """)

    with engine.connect() as conn:
        row = conn.execute(sql, {"cep": cep}).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="CEP não encontrado.")
    return row


@app.get("/enderecos", response_model=List[Endereco])
def list_enderecos(
    uf: Optional[str] = None,
    cidade: Optional[str] = Query(default=None, alias="localidade"),
    bairro: Optional[str] = None,
    logradouro: Optional[str] = None,
    q: Optional[str] = Query(default=None, description="Busca livre (cep/rua/bairro/cidade/uf)"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    where = []
    params = {"limit": limit, "offset": offset}

    if uf:
        where.append("upper(uf) = upper(:uf)")
        params["uf"] = uf.strip()

    if cidade:
        where.append("localidade ILIKE :cidade")
        params["cidade"] = f"%{cidade.strip()}%"

    if bairro:
        where.append("bairro ILIKE :bairro")
        params["bairro"] = f"%{bairro.strip()}%"

    if logradouro:
        where.append("logradouro ILIKE :logradouro")
        params["logradouro"] = f"%{logradouro.strip()}%"

    if q:
        qv = q.strip()
        q_digits = "".join(c for c in qv if c.isdigit())

        where.append(r"""
            (
                regexp_replace(cep, '\D', '', 'g') = :q_digits
                OR logradouro ILIKE :q_like
                OR bairro ILIKE :q_like
                OR localidade ILIKE :q_like
                OR uf ILIKE :q_like
            )
        """)

        params["q_digits"] = q_digits if len(q_digits) == 8 else "________"
        params["q_like"] = f"%{qv}%"

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = text(f"""
        SELECT
            cep, logradouro, complemento, unidade, bairro,
            localidade, uf, estado, regiao, ibge, gia, ddd, siafi
        FROM enderecos
        {where_sql}
        ORDER BY regexp_replace(cep, '\\D', '', 'g')
        LIMIT :limit OFFSET :offset
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql, params).mappings().all()

    return rows
