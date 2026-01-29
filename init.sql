CREATE TABLE IF NOT EXISTS enderecos (
    id SERIAL PRIMARY KEY,
    cep VARCHAR(9) UNIQUE NOT NULL,
    logradouro TEXT,
    complemento TEXT,
    unidade TEXT,
    bairro TEXT,
    localidade TEXT,
    uf CHAR(2),
    estado TEXT,
    regiao TEXT,
    ibge VARCHAR(10),
    gia VARCHAR(10),
    ddd VARCHAR(3),
    siafi VARCHAR(10),
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
