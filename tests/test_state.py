import unittest

from tests.utils import commit_state, transaction, rerun_conflicts
from worker.operator_state.in_memory_state import InMemoryOperatorState

operator_names = {"test"}


class TestState(unittest.IsolatedAsyncioTestCase):

    async def test_state(self):
        state = InMemoryOperatorState(operator_names)
        value_to_put = "value1"
        await state.put(key=1, value=value_to_put, t_id=1, operator_name="test")
        reply = await state.get(key=1, t_id=1, operator_name="test")
        # same t_id reads its own changes
        assert reply == value_to_put
        reply = await state.get(key=1, t_id=2, operator_name="test")
        # different t_id reads snapshot
        assert reply is None  # None because t-id = 1 wrote first and not yet committed
        await state.commit(set())
        reply = await state.get(key=1, t_id=2, operator_name="test")
        # after commit t_id = 2 should read ti_d = 1 changes
        assert reply == value_to_put
        # test fallback commit
        # tid: {operator_name: {key: value}}
        state.fallback_commit_buffer[2] = {"test": {2: value_to_put}}
        await state.commit_fallback_transaction(2)
        reply = await state.get(key=2, t_id=2, operator_name="test")
        assert reply == value_to_put
        await state.delete(key=2, operator_name="test")
        exists = await state.exists(key=1, operator_name="test")
        assert exists
        exists = await state.exists(key=3, operator_name="test")
        assert not exists
        non_exists_is_none = await state.get(key=3,  t_id=3, operator_name="test")
        assert non_exists_is_none is None

    async def test_reordering(self):
        # example from Aria paper Figure 6
        state = InMemoryOperatorState(operator_names)
        x_key = 1
        y_key = 2
        z_key = 3
        value_to_put = "irrelevant"
        # Top example
        # T1
        await state.get(key=x_key, t_id=1, operator_name="test")
        await state.put(key=y_key, value=value_to_put, t_id=1, operator_name="test")
        # T2
        await state.get(key=y_key, t_id=2, operator_name="test")
        await state.put(key=z_key, value=value_to_put, t_id=2, operator_name="test")
        # T3
        await state.get(key=y_key, t_id=3, operator_name="test")
        await state.get(key=z_key, t_id=3, operator_name="test")
        conflicts = state.check_conflicts()
        assert conflicts == {2, 3}
        conflicts = state.check_conflicts_deterministic_reordering()
        assert conflicts == set()
        # Bottom example
        state = InMemoryOperatorState(operator_names)
        # T1
        await state.get(key=x_key, t_id=1, operator_name="test")
        await state.put(key=y_key, value=value_to_put, t_id=1, operator_name="test")
        # T2
        await state.put(key=x_key, value=value_to_put, t_id=2, operator_name="test")
        await state.get(key=z_key, t_id=2, operator_name="test")
        # T3
        await state.get(key=y_key, t_id=3, operator_name="test")
        await state.put(key=z_key, value=value_to_put, t_id=3, operator_name="test")
        conflicts = state.check_conflicts()
        assert conflicts == {1} or conflicts == {2} or conflicts == {3}
        conflicts = state.check_conflicts_deterministic_reordering()
        assert conflicts == {1} or conflicts == {2} or conflicts == {3}

    async def test_two_workers_ycsb(self):
        # w1 gets the even keys w2 gets the odd
        starting_money: int = 10_000
        w1_state = InMemoryOperatorState(operator_names)
        w2_state = InMemoryOperatorState(operator_names)

        # inserts
        await w1_state.put(key=0, value=starting_money, t_id=1, operator_name="test")
        await w1_state.put(key=2, value=starting_money, t_id=3, operator_name="test")
        await w2_state.put(key=1, value=starting_money, t_id=2, operator_name="test")
        await w2_state.put(key=3, value=starting_money, t_id=4, operator_name="test")
        await commit_state(w1_state, w2_state)
        # transaction mix
        transactions = {6: {'k1': 1, 'k1_state': w2_state, 'k2': 2, 'k2_state': w1_state, 't_id': 6},
                        7: {'k1': 0, 'k1_state': w1_state, 'k2': 1, 'k2_state': w2_state, 't_id': 7},
                        8: {'k1': 1, 'k1_state': w2_state, 'k2': 3, 'k2_state': w2_state, 't_id': 8},
                        9: {'k1': 0, 'k1_state': w1_state, 'k2': 1, 'k2_state': w2_state, 't_id': 9},
                        10: {'k1': 0, 'k1_state': w1_state, 'k2': 2, 'k2_state': w1_state, 't_id': 10},
                        11: {'k1': 3, 'k1_state': w2_state, 'k2': 0, 'k2_state': w1_state, 't_id': 11}}
        # transfer from key 1 to 2
        await transaction(**transactions[6])
        # transfer from key 0 to 1
        await transaction(**transactions[7])
        w1_conflicts, w2_conflicts = await commit_state(w1_state, w2_state)
        await rerun_conflicts(w1_conflicts, w2_conflicts, transactions)
        # transfer from key 1 to 3
        await transaction(**transactions[8])
        # transfer from key 0 to 1
        await transaction(**transactions[9])
        # transfer from key 0 to 2
        await transaction(**transactions[10])
        # transfer from key 3 to 0
        await transaction(**transactions[11])
        w1_conflicts, w2_conflicts = await commit_state(w1_state, w2_state)
        while w1_conflicts or w2_conflicts:
            w1_state.cleanup()
            w2_state.cleanup()
            await rerun_conflicts(w1_conflicts, w2_conflicts, transactions)
            w1_conflicts, w2_conflicts = await commit_state(w1_state, w2_state)
        assert w1_conflicts == set() and w2_conflicts == set()
        assert w1_state.data == {'test': {0: 9998, 2: 10002}}
        assert w2_state.data == {'test': {1: 10000, 3: 10000}}

    async def test_two_workers_ycsb_2(self):
        # w1 gets the even keys w2 gets the odd
        starting_money: int = 100
        w1_state = InMemoryOperatorState(operator_names)
        w2_state = InMemoryOperatorState(operator_names)

        # inserts
        await w1_state.put(key=0, value=starting_money, t_id=1, operator_name="test")
        await w1_state.put(key=2, value=starting_money, t_id=3, operator_name="test")
        await w2_state.put(key=1, value=starting_money, t_id=2, operator_name="test")
        await w2_state.put(key=3, value=starting_money, t_id=4, operator_name="test")
        await commit_state(w1_state, w2_state)
        # transaction mix
        transactions = {5: {'k1': 2, 'k1_state': w1_state, 'k2': 0, 'k2_state': w1_state, 't_id': 5},
                        6: {'k1': 3, 'k1_state': w2_state, 'k2': 2, 'k2_state': w1_state, 't_id': 6},
                        7: {'k1': 0, 'k1_state': w1_state, 'k2': 2, 'k2_state': w1_state, 't_id': 7},
                        8: {'k1': 3, 'k1_state': w2_state, 'k2': 1, 'k2_state': w2_state, 't_id': 8}}

        await transaction(**transactions[5])
        await transaction(**transactions[6])
        await transaction(**transactions[7])
        await transaction(**transactions[8])
        w1_conflicts, w2_conflicts = await commit_state(w1_state, w2_state)
