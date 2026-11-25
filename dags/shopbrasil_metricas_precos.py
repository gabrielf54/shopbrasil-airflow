"""
### ShopBrasil - métricas de preço por categoria

Roda todo dia às 06:00 (Brasília): busca o catálogo na API, calcula preço
médio, mínimo, máximo e quantidade por categoria e grava no Postgres.

- ingestao: buscar_produtos -> validar_produtos -> agrupar_por_categoria
- analise: calcular_metricas (uma por categoria) -> consolidar
- persistencia: salvar_snapshot (upsert) e salvar_historico (append)
"""

import logging
import os
from datetime import timedelta

import pendulum
import requests
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from shopbrasil.callbacks import alertar_falha, avisar_retry, avisar_sucesso
from shopbrasil.operators import ValidarProdutosOperator

log = logging.getLogger(__name__)

TZ = "America/Sao_Paulo"
CONN_ID = "postgres_shopbrasil"
POOL = "ecommerce_pool"
API_PADRAO = os.getenv("FAKESTORE_API_URL", "https://fakestoreapi.com")


@dag(
    dag_id="shopbrasil_metricas_precos",
    # o cron usa o fuso do start_date; sem o tz rodaria 06:00 UTC (03:00 aqui)
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "time-dados",
        "on_failure_callback": alertar_falha,
    },
    tags=["shopbrasil", "pricing", "atividade-01"],
    doc_md=__doc__,
)
def shopbrasil_metricas_precos():

    # --- ingestão ---
    @task_group(group_id="ingestao")
    def ingestao():

        # 5 tentativas no total, espera começando em 30s e dobrando (teto de 10 min)
        @task(
            retries=4,
            retry_delay=timedelta(seconds=30),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=10),
            execution_timeout=timedelta(minutes=2),
            on_success_callback=avisar_sucesso,
            on_retry_callback=avisar_retry,
            on_failure_callback=alertar_falha,
        )
        def buscar_produtos() -> list[dict]:
            base = Variable.get("fakestore_api_url", default_var=API_PADRAO)
            url = f"{base.rstrip('/')}/products"
            try:
                resposta = requests.get(url, timeout=15)
                resposta.raise_for_status()
                produtos = resposta.json()
                if not isinstance(produtos, list) or not produtos:
                    raise ValueError("a API devolveu um catálogo vazio ou fora do formato")
            except (requests.RequestException, ValueError) as erro:
                log.error("Falha ao buscar o catálogo em %s: %s", url, erro)
                raise

            log.info("%d produtos recebidos da API", len(produtos))
            # só os campos usados, para o XCom ficar pequeno
            campos = ("id", "title", "category", "price")
            return [{c: p.get(c) for c in campos} for p in produtos]

        validar = ValidarProdutosOperator(
            task_id="validar_produtos",
            produtos=buscar_produtos(),
        )

        @task
        def agrupar_por_categoria(produtos: list[dict]) -> list[dict]:
            # cada item vira uma task no expand, então categoria nova = task nova
            precos_por_categoria: dict[str, list[float]] = {}
            for p in produtos:
                precos_por_categoria.setdefault(p["category"], []).append(p["price"])
            lotes = [
                {"categoria": categoria, "precos": precos}
                for categoria, precos in sorted(precos_por_categoria.items())
            ]
            log.info("%d categorias: %s", len(lotes), [l["categoria"] for l in lotes])
            return lotes

        return agrupar_por_categoria(validar.output)

    # --- análise ---
    @task_group(group_id="analise")
    def analise(lotes):

        # map_index_template: mostra o nome da categoria na UI em vez de 0, 1, 2...
        @task(pool=POOL, map_index_template="{{ categoria }}")
        def calcular_metricas(lote: dict) -> dict:
            get_current_context()["categoria"] = lote["categoria"]
            precos = lote["precos"]
            return {
                "categoria": lote["categoria"],
                "qtd_produtos": len(precos),
                "preco_medio": round(sum(precos) / len(precos), 2),
                "preco_min": min(precos),
                "preco_max": max(precos),
            }

        @task
        def consolidar(metricas) -> list[dict]:
            resultado = sorted(metricas, key=lambda m: m["categoria"])
            for m in resultado:
                log.info(
                    "%-18s %3d produtos | médio %8.2f | mín %8.2f | máx %8.2f",
                    m["categoria"], m["qtd_produtos"], m["preco_medio"], m["preco_min"], m["preco_max"],
                )
            return resultado

        return consolidar(calcular_metricas.expand(lote=lotes))

    # --- persistência ---
    @task_group(group_id="persistencia")
    def persistencia(metricas):

        @task
        def salvar_snapshot(metricas: list[dict], data_interval_end=None, run_id=None) -> int:
            # A run das 06:00 de hoje cobre [ontem 06:00, hoje 06:00], então a
            # data de referência é a de hoje. Rodar de novo cai na mesma data e
            # o upsert atualiza as linhas em vez de duplicar.
            data_ref = data_interval_end.in_timezone(TZ).date()
            linhas = [
                (data_ref, m["categoria"], m["qtd_produtos"], m["preco_medio"],
                 m["preco_min"], m["preco_max"], run_id)
                for m in metricas
            ]
            upsert = """
                INSERT INTO metricas_categoria
                    (data_referencia, categoria, qtd_produtos, preco_medio,
                     preco_min, preco_max, dag_run_id, atualizado_em)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (data_referencia, categoria) DO UPDATE SET
                    qtd_produtos  = EXCLUDED.qtd_produtos,
                    preco_medio   = EXCLUDED.preco_medio,
                    preco_min     = EXCLUDED.preco_min,
                    preco_max     = EXCLUDED.preco_max,
                    dag_run_id    = EXCLUDED.dag_run_id,
                    atualizado_em = NOW()
            """
            # se uma categoria sumiu da API, tira ela do snapshot do dia
            remover_ausentes = """
                DELETE FROM metricas_categoria
                WHERE data_referencia = %s AND NOT (categoria = ANY(%s))
            """
            conn = PostgresHook(postgres_conn_id=CONN_ID).get_conn()
            try:
                with conn, conn.cursor() as cur:  # uma transação só
                    cur.executemany(upsert, linhas)
                    cur.execute(remover_ausentes, (data_ref, [m["categoria"] for m in metricas]))
            finally:
                conn.close()

            log.info("Snapshot de %s gravado: %d categorias", data_ref, len(linhas))
            return len(linhas)

        @task
        def salvar_historico(metricas: list[dict], data_interval_end=None, run_id=None) -> int:
            data_ref = data_interval_end.in_timezone(TZ).date()
            hook = PostgresHook(postgres_conn_id=CONN_ID)
            hook.insert_rows(
                table="historico_metricas_categoria",
                rows=[
                    (data_ref, m["categoria"], m["qtd_produtos"], m["preco_medio"],
                     m["preco_min"], m["preco_max"], run_id)
                    for m in metricas
                ],
                target_fields=["data_referencia", "categoria", "qtd_produtos", "preco_medio",
                               "preco_min", "preco_max", "dag_run_id"],
            )
            log.info("Histórico: %d linhas acrescentadas", len(metricas))
            return len(metricas)

        salvar_snapshot(metricas)
        salvar_historico(metricas)

    persistencia(analise(ingestao()))


shopbrasil_metricas_precos()
