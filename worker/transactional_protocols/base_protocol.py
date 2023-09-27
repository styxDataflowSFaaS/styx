from abc import abstractmethod, ABC


PROTOCOL_PORT: int = 8889


class BaseTransactionalProtocol(ABC):

    @abstractmethod
    async def run_function(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    async def function_scheduler(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    async def communication_protocol(self):
        raise NotImplementedError

    @abstractmethod
    async def take_snapshot(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    async def stop(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    async def start(self, *args, **kwargs):
        raise NotImplementedError
