"""Operador customizado da ShopBrasil."""

from __future__ import annotations

import logging
from collections.abc import Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

log = logging.getLogger(__name__)


class ValidarProdutosOperator(BaseOperator):
    """Valida o schema dos produtos vindos da API antes do processamento.

    Cada produto precisa ter `id` inteiro, `title` e `category` textos não
    vazios e `price` numérico e não negativo. Produtos inválidos são
    descartados com aviso no log, para que um item quebrado não derrube o
    painel inteiro. Se sobrarem menos que `minimo_validos`, a task falha:
    isso indica um problema na API, não num produto isolado.

    `produtos` está em `template_fields`, então pode receber a saída de outra
    task (um XComArg). O Airflow resolve o valor na hora de executar e cria a
    dependência entre as tasks sozinho.
    """

    template_fields: Sequence[str] = ("produtos",)
    ui_color = "#fde68a"

    def __init__(self, *, produtos, minimo_validos: int = 1, **kwargs) -> None:
        super().__init__(**kwargs)
        self.produtos = produtos
        self.minimo_validos = minimo_validos

    @staticmethod
    def problemas(produto) -> list[str]:
        if not isinstance(produto, dict):
            return ["não é um objeto JSON"]
        erros = []
        if not isinstance(produto.get("id"), int):
            erros.append("id ausente ou não inteiro")
        for campo in ("title", "category"):
            valor = produto.get(campo)
            if not isinstance(valor, str) or not valor.strip():
                erros.append(f"{campo} ausente ou vazio")
        preco = produto.get("price")
        # bool é subclasse de int em Python: True passaria como preço 1
        if isinstance(preco, bool) or not isinstance(preco, (int, float)):
            erros.append("price ausente ou não numérico")
        elif preco < 0:
            erros.append("price negativo")
        return erros

    def execute(self, context) -> list[dict]:
        validos, descartados = [], 0
        for produto in self.produtos:
            erros = self.problemas(produto)
            if erros:
                descartados += 1
                log.warning("Produto descartado %s: %s", produto, "; ".join(erros))
            else:
                validos.append(produto)

        log.info("%d produtos válidos, %d descartados", len(validos), descartados)
        if len(validos) < self.minimo_validos:
            raise AirflowException(
                f"Só {len(validos)} produtos válidos (mínimo {self.minimo_validos}). "
                "A API provavelmente mudou de formato."
            )
        return validos
