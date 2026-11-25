"""Callbacks de sucesso, retry e falha usados nas tasks da ShopBrasil."""

import json
import logging

log = logging.getLogger(__name__)


def _resumo(context) -> dict:
    ti = context["ti"]
    return {
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context["run_id"],
        # No 2.9, quando o callback roda o try_number já aponta para a próxima
        # tentativa. A que acabou de rodar é prev_attempted_tries.
        "tentativa": ti.prev_attempted_tries,
        "log_url": ti.log_url,
    }


def avisar_sucesso(context) -> None:
    r = _resumo(context)
    log.info("[OK] %s concluída na tentativa %s (run %s)", r["task_id"], r["tentativa"], r["run_id"])


def avisar_retry(context) -> None:
    r = _resumo(context)
    log.warning(
        "[RETRY] %s falhou na tentativa %s e vai tentar de novo. Motivo: %s",
        r["task_id"], r["tentativa"], context.get("exception"),
    )


def alertar_falha(context) -> None:
    # Alerta simulado: em produção seria um POST no webhook do Slack.
    r = _resumo(context)
    alerta = {
        "canal": "#dados-alertas",
        "titulo": f"Pipeline {r['dag_id']} falhou",
        "texto": (
            f"A task {r['task_id']} esgotou as tentativas (última: {r['tentativa']}). "
            "O painel de pricing pode estar sem os dados de hoje."
        ),
        "erro": str(context.get("exception")),
        "run_id": r["run_id"],
        "log": r["log_url"],
    }
    log.error("[ALERTA] enviado (simulado):\n%s", json.dumps(alerta, ensure_ascii=False, indent=2))
