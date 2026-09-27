import abc


class BaseFlow(abc.ABC):
    """
    Interface base para todos os fluxos de orquestracao do Lakehouse.
    Garante que qualquer pipeline de dominio implemente o metodo run().
    """

    def __init__(self, flow_name: str):
        self.flow_name = flow_name

    @abc.abstractmethod
    def run(self) -> str:
        """Executa a logica de ponta a ponta do fluxo e retorna o status/URI."""
        pass