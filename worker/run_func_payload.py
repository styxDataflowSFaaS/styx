from dataclasses import dataclass


@dataclass
class RunFuncPayload:
    request_id: bytes
    key: object
    timestamp: int
    operator_name: str
    partition: int
    function_name: str
    params: tuple
    response_socket: object
    ack_payload: tuple[str, int, str]  # host, port, stake


@dataclass
class SequencedItem:
    t_id: int
    payload: RunFuncPayload

    def __hash__(self):
        return hash(self.t_id)

    def __lt__(self, other):
        return self.t_id < other.t_id
