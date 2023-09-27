from abc import abstractmethod, ABC


class BaseOperatorState(ABC):

    def __init__(self, operator_names: set[str]):
        self.operator_names = operator_names

    @abstractmethod
    async def put(self, key, value, t_id: int, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    async def put_immediate(self, key, value, t_id: int, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    async def get_all(self, t_id: int, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    async def get(self, key, t_id: int, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    async def get_immediate(self, key, t_id: int, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    async def delete(self, key, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    async def exists(self, key, operator_name: str):
        raise NotImplementedError
