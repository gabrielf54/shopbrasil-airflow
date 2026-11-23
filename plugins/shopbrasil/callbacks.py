"""Callbacks de ciclo de vida das tasks.

O Airflow chama essas funções sozinho, passando o `context` da execução
(task, run, exceção, número da tentativa...). Elas não mudam o resultado da
task: servem para avisar alguém do que aconteceu.
"""

from __future__ import annotations

import json
import logging

log = logging.getLogger(__name__)


def _resumo(context) -> dict:
    ti = context["ti"]
    return {
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context["run_id"],
        # No Airflow 2.9, quando o callback roda a task já mudou de estado e
        # `try_number` aponta para a PRÓXIMA tentativa. A que acabou de rodar
        # é `prev_attempted_tries`.
        "tentativa": ti.prev_attempted_tries,
        "log_url": ti.log_url,
    }


def avisar_sucesso(context) -> None:
    r = _resumo(context)
    log.info("✅ %s concluída na tentativa %s (run %s)", r["task_id"], r["tentativa"], r["run_id"])


def avisar_retry(context) -> None:
    r = _resumo(context)
    log.warning(
        "🔁 %s falhou na tentativa %s e vai tentar de novo. Motivo: %s",
        r["task_id"], r["tentativa"], context.get("exception"),
    )


def alertar_falha(context) -> None:
    """Simula o envio de um alerta (Slack, e-mail, PagerDuty...).

    Em produção, aqui entraria a chamada ao webhook do canal do time. Para o
    projeto, montamos a mensagem completa e a registramos no log da task,
    onde ela aparece na UI com destaque.
    """
    r = _resumo(context)
    alerta = {
        "canal": "#dados-alertas",
        "titulo": f"🚨 Pipeline {r['dag_id']} falhou",
        "texto": (
            f"A task {r['task_id']} esgotou as tentativas (última: {r['tentativa']}). "
            "O painel de pricing pode estar sem os dados de hoje."
        ),
        "erro": str(context.get("exception")),
        "run_id": r["run_id"],
        "log": r["log_url"],
    }
    log.error("ALERTA ENVIADO (simulado):\n%s", json.dumps(alerta, ensure_ascii=False, indent=2))
