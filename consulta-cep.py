import os
import random
import time
import sys
import json
import csv
import logging
import threading
import requests
from requests.adapters import HTTPAdapter
import psycopg2
import pandas as pd
import xml.etree.ElementTree as ET
from queue import Queue
from dotenv import load_dotenv
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient

# ================== SETUP ==================
sys.stdout.reconfigure(encoding="utf-8")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)

CSV_IN = r"C:\Users\henri\Documents\Projetos\consulta-cep\ceps.csv"
OUT_JSON = os.path.join(BASE_DIR, "enderecos.json")
OUT_XML = os.path.join(BASE_DIR, "enderecos.xml")
OUT_ERRORS_CSV = os.path.join(BASE_DIR, "erros_consulta.csv")
LOG_PATH = os.path.join(BASE_DIR, "erros.log")

MAX_WORKERS = 20        # threads de consulta HTTP
RATE_PER_SEC = 0.5       # limite global (ex: 0.5/s = 30/min)
MAX_RETRIES = 5
DB_CONNECT_TIMEOUT = 5
MONGO_URI = os.getenv("MONGO_URI", "").strip()
MONGO_DB = os.getenv("MONGO_DB", "ceps").strip()
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "enderecos").strip()

CONNECT_TIMEOUT = 120
READ_TIMEOUT = 120
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

BACKOFF_BASE = 0.8
BACKOFF_MAX = 10.0
RETRY_STATUS = {429, 500, 502, 503, 504}
SAVE_EVERY = 10

HTTP_FALLBACK_ON_SSL = True
SHOW_PER_CEP = False
DB_STATS_EVERY = 10
CHECK_CEP = ""
CHECK_CEP_EVERY = 0
MONGO_ENABLED = bool(MONGO_URI)

_thread_local = threading.local()

def _build_session() -> requests.Session:
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=2, pool_maxsize=2)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "consulta-cep/1.0", "Connection": "close"})
    return s

def _get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = _build_session()
    return _thread_local.session

enderecos_coletados = []
enderecos_lock = threading.Lock()

erros_coletados = []
erros_lock = threading.Lock()
erros_csv_lock = threading.Lock()

# fila para gravar no banco (1 thread dedicada)
db_queue = Queue(maxsize=2000)
DB_ENABLED = True
db_stats = {"inserted": 0, "ignored": 0, "errors": 0}
db_stats_lock = threading.Lock()

mongo_client = None
mongo_lock = threading.Lock()
mongo_stats = {"upserted": 0, "updated": 0, "errors": 0}
mongo_stats_lock = threading.Lock()

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.ERROR,
    format="%(asctime)s | %(message)s",
    encoding="utf-8"
)
root_logger = logging.getLogger()
if not any(isinstance(h, logging.FileHandler) and h.baseFilename == LOG_PATH for h in root_logger.handlers):
    file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
    root_logger.addHandler(file_handler)

def _flush_log_handlers():
    for h in logging.getLogger().handlers:
        try:
            h.flush()
        except Exception:
            pass

def _append_error_csv(cep: str, msg: str):
    with erros_csv_lock:
        need_header = not os.path.exists(OUT_ERRORS_CSV) or os.path.getsize(OUT_ERRORS_CSV) == 0
        with open(OUT_ERRORS_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["cep", "erro"])
            if need_header:
                writer.writeheader()
            writer.writerow({"cep": cep, "erro": msg})

def registrar_erro(cep: str, msg: str, log_msg: str = None):
    if log_msg is None:
        log_msg = f"{cep} | {msg}" if cep else msg
    logging.error(log_msg)
    _flush_log_handlers()
    with erros_lock:
        erros_coletados.append({"cep": cep, "erro": msg})
    _append_error_csv(cep, msg)

# ================== RATE LIMITER (GLOBAL) ==================
class RateLimiter:
    def __init__(self, rate_per_sec: float):
        if rate_per_sec <= 0:
            raise ValueError("rate_per_sec deve ser > 0")
        self.interval = 1.0 / rate_per_sec
        self.lock = threading.Lock()
        self.next_allowed = 0.0

    def wait(self):
        with self.lock:
            now = time.time()
            if now < self.next_allowed:
                time.sleep(self.next_allowed - now)
            self.next_allowed = time.time() + self.interval

limiter = RateLimiter(RATE_PER_SEC)

# ================== BACKOFF ==================
def calcular_backoff(tentativa: int) -> float:
    base = BACKOFF_BASE * (2 ** (tentativa - 1))
    base = min(base, BACKOFF_MAX)
    jitter = random.uniform(0, base * 0.2)
    return min(base + jitter, BACKOFF_MAX)

def aguardar_tentativa(tentativa: int, retry_after=None):
    if retry_after:
        try:
            espera = float(retry_after)
            if espera > 0:
                time.sleep(espera)
                return
        except ValueError:
            pass
    time.sleep(calcular_backoff(tentativa))

# ================== BANCO (1 WORKER) ==================
SQL_INSERT = """
INSERT INTO public.enderecos (
  cep, logradouro, complemento, unidade, bairro,
  localidade, uf, estado, regiao, ibge, gia, ddd, siafi
) VALUES (
  %(cep)s, %(logradouro)s, %(complemento)s, %(unidade)s, %(bairro)s,
  %(localidade)s, %(uf)s, %(estado)s, %(regiao)s, %(ibge)s,
  %(gia)s, %(ddd)s, %(siafi)s
)
ON CONFLICT (cep) DO NOTHING;
"""

def normalizar_payload_viacep(dados: dict) -> dict:
    """
    Garante que todas as chaves do INSERT existem (evita KeyError).
    ViaCEP pode não devolver 'estado'/'regiao' dependendo da versão.
    """
    campos = [
        "cep", "logradouro", "complemento", "unidade", "bairro",
        "localidade", "uf", "estado", "regiao", "ibge", "gia", "ddd", "siafi"
    ]
    out = {k: dados.get(k, "") for k in campos}
    return out

def _validar_env_db() -> bool:
    required = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        msg = f"DB indisponivel: variaveis ausentes: {', '.join(missing)}"
        print(msg)
        registrar_erro("", msg, f"DB | env | {msg}")
        return False
    return True

def _db_stats_snapshot():
    with db_stats_lock:
        return db_stats["inserted"], db_stats["ignored"], db_stats["errors"]

def _db_count_public_enderecos() -> int:
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            connect_timeout=DB_CONNECT_TIMEOUT,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM public.enderecos;")
                return int(cur.fetchone()[0])
        finally:
            conn.close()
    except Exception as e:
        print(f"DB count erro: {e}")
        return -1

def _get_mongo_collection():
    global mongo_client
    if not MONGO_ENABLED:
        return None
    with mongo_lock:
        if mongo_client is None:
            mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        return mongo_client[MONGO_DB][MONGO_COLLECTION]

def _testar_mongo():
    global MONGO_ENABLED
    if not MONGO_ENABLED:
        print("MongoDB desabilitado (MONGO_URI vazio).")
        return False
    try:
        col = _get_mongo_collection()
        if col is None:
            return False
        # pinga o servidor
        col.database.client.admin.command("ping")
        print(f"MongoDB conectado. db={MONGO_DB} col={MONGO_COLLECTION}")
        return True
    except Exception as e:
        print(f"MongoDB indisponivel: {e}")
        registrar_erro("", f"Mongo connect: {e}", f"MONGO | connect | {e}")
        MONGO_ENABLED = False
        return False

def _salvar_mongo(dados: dict):
    if not MONGO_ENABLED:
        return
    try:
        col = _get_mongo_collection()
        if col is None:
            return
        cep_val = dados.get("cep")
        res = col.update_one({"cep": cep_val}, {"$set": dados}, upsert=True)
        with mongo_stats_lock:
            if res.upserted_id is not None:
                mongo_stats["upserted"] += 1
            else:
                mongo_stats["updated"] += 1
    except Exception as e:
        with mongo_stats_lock:
            mongo_stats["errors"] += 1
        registrar_erro(dados.get("cep", ""), f"Mongo: {e}", f"{dados.get('cep','')} | MONGO | {e}")

def testar_conexao_db() -> bool:
    if not _validar_env_db():
        return False
    try:
        print(
            f"DB config: host={os.getenv('DB_HOST')} "
            f"port={os.getenv('DB_PORT')} db={os.getenv('DB_NAME')}"
        )
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            connect_timeout=DB_CONNECT_TIMEOUT,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.enderecos');")
                reg = cur.fetchone()[0]
                if reg is None:
                    raise RuntimeError("Tabela public.enderecos nao encontrada")
                cur.execute("SELECT COUNT(*) FROM public.enderecos;")
                total = cur.fetchone()[0]
                cur.execute(
                    "SELECT current_database(), current_schema(), "
                    "inet_server_addr(), inet_server_port();"
                )
                dbname, schema, server_addr, server_port = cur.fetchone()
                print(f"DB servidor: db={dbname} schema={schema} addr={server_addr} port={server_port}")
            print(f"DB conectado. public.enderecos={total}")
        finally:
            conn.close()
        return True
    except Exception as e:
        print(f"DB indisponivel: {e}")
        registrar_erro("", f"DB connect: {e}", f"DB | connect | {e}")
        return False

def db_worker():
    global DB_ENABLED
    if not _validar_env_db():
        DB_ENABLED = False
        return
    try:
        print("DB worker iniciando...")
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            connect_timeout=DB_CONNECT_TIMEOUT,
        )
        conn.autocommit = True
        print("DB worker conectado.")
    except Exception as e:
        print(f"DB indisponivel: {e}")
        registrar_erro("", f"DB connect: {e}", f"DB | connect | {e}")
        DB_ENABLED = False
        return
    try:
        with conn:
            with conn.cursor() as cur:
                while True:
                    item = db_queue.get()
                    if item is None:
                        db_queue.task_done()
                        break
                    try:
                        cur.execute(SQL_INSERT, item)
                        if cur.rowcount and cur.rowcount > 0:
                            with db_stats_lock:
                                db_stats["inserted"] += 1
                        else:
                            with db_stats_lock:
                                db_stats["ignored"] += 1
                    except Exception as e:
                        cep_item = item.get("cep", "")
                        registrar_erro(cep_item, f"DB: {e}", f"{cep_item} | DB | {e}")
                        with db_stats_lock:
                            db_stats["errors"] += 1
                    finally:
                        db_queue.task_done()
    finally:
        conn.close()


# ================== API ==================
def _request_cep_json(url: str) -> dict:
    session = _get_session()
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def _processar_dados_cep(cep: str, dados: dict):
    if dados.get("erro"):
        msg = "CEP inv?lido ou n?o encontrado"
        registrar_erro(cep, msg)
        return None

    # printa 1 linha por CEP (evita bagun?a no console)
    if SHOW_PER_CEP:
        try:
            tqdm.write(f"{cep} -> OK")
        except Exception:
            print(f"{cep} -> OK")

    # salva no banco via fila (sem abrir conex?o por CEP)
    payload_db = normalizar_payload_viacep(dados)
    if DB_ENABLED:
        try:
            db_queue.put(payload_db, timeout=5)
        except Exception as e:
            msg = f"DB queue cheia: {e}"
            registrar_erro(cep, msg, f"{cep} | DB | {msg}")

    if MONGO_ENABLED:
        _salvar_mongo(dados)

    with enderecos_lock:
        enderecos_coletados.append(dados)

    return dados

def consultar_cep(cep: str):
    url = f"https://viacep.com.br/ws/{cep}/json/"
    url_http = f"http://viacep.com.br/ws/{cep}/json/"

    for tentativa in range(1, MAX_RETRIES + 1):
        try:
            limiter.wait()
            dados = _request_cep_json(url)
            return _processar_dados_cep(cep, dados)

        except (requests.exceptions.ConnectTimeout,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError) as e:
            if tentativa < MAX_RETRIES:
                aguardar_tentativa(tentativa)
                continue

            msg = str(e)
            registrar_erro(cep, msg, f"{cep} | HTTP | {msg}")
            return None

        except requests.exceptions.SSLError as e:
            if tentativa < MAX_RETRIES:
                aguardar_tentativa(tentativa)
                continue

            if HTTP_FALLBACK_ON_SSL:
                try:
                    limiter.wait()
                    dados = _request_cep_json(url_http)
                    return _processar_dados_cep(cep, dados)
                except Exception as e_http:
                    msg = str(e_http)
                    registrar_erro(cep, f"HTTP-FALLBACK: {msg}", f"{cep} | HTTP-FALLBACK | {msg}")
                    return None

            msg = str(e)
            registrar_erro(cep, msg, f"{cep} | HTTP | {msg}")
            return None

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status in RETRY_STATUS and tentativa < MAX_RETRIES:
                retry_after = e.response.headers.get("Retry-After") if e.response else None
                aguardar_tentativa(tentativa, retry_after)
                continue

            msg = f"HTTP {status} {e}" if status else str(e)
            registrar_erro(cep, msg, f"{cep} | HTTP | {msg}")
            return None

        except ValueError as e:
            if tentativa < MAX_RETRIES:
                aguardar_tentativa(tentativa)
                continue

            msg = f"JSON inv?lido: {e}"
            registrar_erro(cep, msg, f"{cep} | HTTP | {msg}")
            return None

        except requests.exceptions.RequestException as e:
            msg = str(e)
            registrar_erro(cep, msg, f"{cep} | HTTP | {msg}")
            return None

        except Exception as e:
            # qualquer erro inesperado
            msg = str(e)
            registrar_erro(cep, msg, f"{cep} | UNK | {msg}")
            return None


def salvar_artefatos():
    with enderecos_lock:
        enderecos_snapshot = list(enderecos_coletados)
    with erros_lock:
        erros_snapshot = list(erros_coletados)

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(enderecos_snapshot, f, ensure_ascii=False, indent=2)

    root = ET.Element("enderecos")
    for end in enderecos_snapshot:
        e = ET.SubElement(root, "endereco")
        for k, v in end.items():
            campo = ET.SubElement(e, k)
            campo.text = "" if v is None else str(v)

    tree = ET.ElementTree(root)
    tree.write(OUT_XML, encoding="utf-8", xml_declaration=True)

    with open(OUT_ERRORS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["cep", "erro"])
        writer.writeheader()
        writer.writerows(erros_snapshot)


def main():
    global DB_ENABLED, MONGO_ENABLED
    DB_ENABLED = testar_conexao_db()
    MONGO_ENABLED = _testar_mongo()
    db_thread = None
    if DB_ENABLED:
        db_thread = threading.Thread(target=db_worker, daemon=True)
        db_thread.start()
    else:
        print("DB indisponivel: salvando apenas em arquivos.")

    # ================== LER CSV ==================
    df = pd.read_csv(CSV_IN, usecols=["cep"], dtype=str)
    df["cep"] = df["cep"].str.replace(r"\D", "", regex=True)
    df = df[df["cep"].str.len() == 8]

    ceps = df["cep"].drop_duplicates().tolist()
    print(f"CEPs válidos únicos: {len(ceps)}")
    print(f"Workers: {MAX_WORKERS} | Rate: {RATE_PER_SEC:.2f} req/s (~{RATE_PER_SEC*60:.0f}/min)")

    # ================== PARALELISMO ==================
    sucesso = 0
    falha = 0
    processados = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(consultar_cep, cep) for cep in ceps]

        with tqdm(total=len(futures), desc="Consultando CEPs", unit="cep") as pbar:
            for future in as_completed(futures):
                result = future.result()
                if result:
                    sucesso += 1
                else:
                    falha += 1
                processados += 1

                if DB_ENABLED:
                    dbq = db_queue.qsize()
                    ins, ign, dberr = _db_stats_snapshot()
                    pbar.set_postfix_str(f"ok={sucesso} falha={falha} dbq={dbq} ins={ins} ign={ign} dberr={dberr}")
                else:
                    pbar.set_postfix_str(f"ok={sucesso} falha={falha} db=off")

                if SAVE_EVERY and processados % SAVE_EVERY == 0:
                    salvar_artefatos()
                if DB_ENABLED and DB_STATS_EVERY and processados % DB_STATS_EVERY == 0:
                    ins, ign, dberr = _db_stats_snapshot()
                    try:
                        tqdm.write(f"DB stats: ins={ins} ign={ign} dberr={dberr} dbq={db_queue.qsize()}")
                    except Exception:
                        print(f"DB stats: ins={ins} ign={ign} dberr={dberr} dbq={db_queue.qsize()}")
                pbar.update(1)


    # ================== FINALIZAR BANCO ==================
    if DB_ENABLED:
        db_queue.join()
        db_queue.put(None)
        db_thread.join()
        ins, ign, dberr = _db_stats_snapshot()
        print(f"DB resumo: inseridos={ins} ignorados={ign} erros={dberr}")
        total_final = _db_count_public_enderecos()
        if total_final >= 0:
            print(f"DB total final public.enderecos={total_final}")
    if MONGO_ENABLED:
        with mongo_stats_lock:
            up = mongo_stats["upserted"]
            upd = mongo_stats["updated"]
            merr = mongo_stats["errors"]
        print(f"Mongo resumo: upserted={up} updated={upd} errors={merr}")
        try:
            if mongo_client is not None:
                mongo_client.close()
        except Exception:
            pass

    salvar_artefatos()

    print("Processo finalizado.")


if __name__ == "__main__":
    main()
