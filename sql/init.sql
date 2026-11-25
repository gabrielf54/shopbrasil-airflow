-- Tabelas da base analítica (roda só na primeira subida do postgres-analytics)

-- Snapshot do dia: uma linha por categoria por dia. A PK é o que impede
-- duplicar quando o pipeline roda de novo (a DAG faz upsert).
CREATE TABLE IF NOT EXISTS metricas_categoria (
    data_referencia DATE          NOT NULL,
    categoria       VARCHAR(100)  NOT NULL,
    qtd_produtos    INTEGER       NOT NULL,
    preco_medio     NUMERIC(12,2) NOT NULL,
    preco_min       NUMERIC(12,2) NOT NULL,
    preco_max       NUMERIC(12,2) NOT NULL,
    dag_run_id      VARCHAR(250)  NOT NULL,
    atualizado_em   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (data_referencia, categoria)
);

-- Histórico: append, cada execução acrescenta linhas (evolução dos preços)
CREATE TABLE IF NOT EXISTS historico_metricas_categoria (
    id              BIGSERIAL     PRIMARY KEY,
    data_referencia DATE          NOT NULL,
    categoria       VARCHAR(100)  NOT NULL,
    qtd_produtos    INTEGER       NOT NULL,
    preco_medio     NUMERIC(12,2) NOT NULL,
    preco_min       NUMERIC(12,2) NOT NULL,
    preco_max       NUMERIC(12,2) NOT NULL,
    dag_run_id      VARCHAR(250)  NOT NULL,
    coletado_em     TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_historico_categoria_coleta
    ON historico_metricas_categoria (categoria, coletado_em);
