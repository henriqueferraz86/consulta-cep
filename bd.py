import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def salvar_endereco(endereco: dict):
    """
    Recebe um dicionário com os dados do endereço
    e salva no PostgreSQL.
    """

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

    sql = """
    INSERT INTO enderecos (
      cep, logradouro, complemento, unidade, bairro,
      localidade, uf, estado, regiao, ibge, gia, ddd, siafi
    ) VALUES (
      %(cep)s, %(logradouro)s, %(complemento)s, %(unidade)s, %(bairro)s,
      %(localidade)s, %(uf)s, %(estado)s, %(regiao)s, %(ibge)s,
      %(gia)s, %(ddd)s, %(siafi)s
    )
    ON CONFLICT (cep) DO NOTHING;
    """

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, endereco)
    finally:
        conn.close()
