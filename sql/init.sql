-- =============================================================================
-- Base analítica da ShopBrasil
-- Roda automaticamente na primeira vez que o container postgres-analytics sobe.
-- =============================================================================

-- Snapshot diário que alimenta o painel de pricing.
-- A chave primária (data_referencia, categoria) é o que garante a idempotência:
-- existe no máximo UMA linha por categoria por dia, por mais que o pipeline
-- rode de novo. A DAG grava com INSERT ... ON CONFLICT DO UPDATE (upsert).
CREATE TABLE IF NOT EXISTS metricas_categoria (
    data_referencia DATE          NOT NULL,
    categoria       VARCHAR(100)  NOT NULL,
    qtd_produtos    INTEGER       NOT NULL,
    preco_medio     NUMERIC(12,2) NOT NULL,
    preco_min       NUMERIC(12,2) NOT NULL,
    preco_max       NUMERIC(12,2) NOT NULL,
    dag_run_id      VARCHAR(250)  NOT NULL,   -- qual execução gravou a versão atual
    atualizado_em   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (data_referencia, categoria)
);

-- Histórico (requisito opcional): modo APPEND, uma linha por categoria a cada
-- coleta, para acompanhar a evolução dos preços ao longo do tempo.
-- Aqui repetir uma execução ACRESCENTA linhas de propósito: cada linha é uma
-- observação, marcada com o momento da coleta e a run que a gerou.
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
