import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine
from pymongo import MongoClient

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)

# ===== CONFIG =====
st.set_page_config(page_title="Consulta CEP", layout="wide")

st.markdown(
    "<h1 style='text-align: center;'>Consulta CEP</h1>",
    unsafe_allow_html=True
)

# ===== FUNÇÕES =====
@st.cache_data(show_spinner=False)
def carregar_json(caminho="enderecos.json"):
    return pd.read_json(caminho)

@st.cache_data(show_spinner=False)
def carregar_db():
    required = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Variáveis ausentes no .env: {', '.join(missing)}")

    engine = create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
        f"{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:"
        f"{os.getenv('DB_PORT')}/"
        f"{os.getenv('DB_NAME')}"
    )

    try:
        with engine.connect():
            st.success("DB conectado.")
    except Exception as e:
        raise RuntimeError(f"Falha ao conectar no DB: {e}")

    df = pd.read_sql("""
        SELECT
            cep, logradouro, complemento, unidade, bairro,
            localidade, uf, estado, regiao, ibge, gia, ddd, siafi
        FROM enderecos
        ORDER BY cep
    """, engine)

    return df

@st.cache_data(show_spinner=False)
def carregar_mongo():
    uri = os.getenv("MONGO_URI")
    db = os.getenv("MONGO_DB", "ceps")
    col = os.getenv("MONGO_COLLECTION", "enderecos")
    if not uri:
        raise RuntimeError("Variavel ausente no .env: MONGO_URI")
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
        st.success("MongoDB conectado.")
        docs = list(client[db][col].find({}, {"_id": 0}))
        return pd.DataFrame(docs)
    except Exception as e:
        raise RuntimeError(f"Falha ao conectar no MongoDB: {e}")
    finally:
        client.close()

def limpar_cep(x):
    if pd.isna(x):
        return ""
    return "".join(c for c in str(x) if c.isdigit())

# ===== UI =====
fonte = st.radio(
    "Fonte de dados",
    ["JSON", "Banco", "MongoDB"],
    horizontal=True
)

ufs = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
    "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
    "RS","RO","RR","SC","SP","SE","TO"
]

with st.sidebar:
    st.header("Filtros")

    busca = st.text_input("Busca livre (CEP, rua, bairro, cidade)")
    
    ufs_selecionadas = st.multiselect(
        "UF",
        options=ufs
    )
# ===== CONTROLE DE CACHE =====
if st.button("🔄 Atualizar dados"):
    st.cache_data.clear()

# ===== CARREGAR DADOS =====
try:
    if fonte == "JSON":
        df = carregar_json()
    elif fonte == "Banco":
        df = carregar_db()
    else:
        df = carregar_mongo()
except Exception as e:
    st.error(f"Erro ao carregar dados: {e}")
    st.stop()

# ===== NORMALIZAÇÃO =====
if "cep" in df.columns:
    df["cep"] = df["cep"].apply(limpar_cep)

df_filtrado = df.copy()

# ===== FILTRO UF =====
if ufs_selecionadas and "uf" in df_filtrado.columns:
    df_filtrado = df_filtrado[df_filtrado["uf"].isin(ufs_selecionadas)]

# ===== BUSCA LIVRE =====
if busca.strip():
    b_raw = busca.strip().lower()
    b_num = "".join(ch for ch in b_raw if ch.isdigit())  # remove "-" e qualquer coisa

    colunas_busca = [
        c for c in ["cep", "logradouro", "bairro", "localidade", "uf"]
        if c in df_filtrado.columns
    ]

    mask = False

    for c in colunas_busca:
        s = df_filtrado[c].astype(str).str.lower()

        # se o usuário digitou números (ex: CEP com ou sem "-"), faz match numérico no campo cep
        if c == "cep" and b_num:
            s_num = s.str.replace(r"\D", "", regex=True)
            mask = mask | s_num.str.contains(b_num, na=False)
        else:
            mask = mask | s.str.contains(b_raw, na=False)

    df_filtrado = df_filtrado[mask]


# ===== RESULTADO =====
st.caption(f"Registros: {len(df_filtrado)} de {len(df)}")

st.dataframe(
    df_filtrado,
    width="stretch",
    hide_index=True
)

# ===== DOWNLOAD =====
csv_bytes = df_filtrado.to_csv(index=False).encode("utf-8")
st.download_button(
    "Baixar CSV",
    data=csv_bytes,
    file_name="enderecos_filtrados.csv",
    mime="text/csv"
)
