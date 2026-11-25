import logging
from collections.abc import Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

log = logging.getLogger(__name__)


class ValidarProdutosOperator(BaseOperator):
    """Valida o schema dos produtos antes do processamento.

    Produto inválido é descartado com aviso no log. Se sobrarem menos que
    `minimo_validos`, a task falha (aí o problema é a API, não um produto).
    """

    # permite passar a saída de outra task em `produtos`
    template_fields: Sequence[str] = ("produtos",)

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
