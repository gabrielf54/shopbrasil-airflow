"""Código compartilhado pelas DAGs da ShopBrasil.

O Airflow coloca a pasta plugins/ no sys.path, então as DAGs importam daqui com
`from shopbrasil.callbacks import ...` e `from shopbrasil.operators import ...`.
"""
