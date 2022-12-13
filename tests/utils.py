from worker.operator_state.in_memory_state import InMemoryOperatorState


async def commit_state(w1_state: InMemoryOperatorState, w2_state: InMemoryOperatorState):
    w1_conflicts = w1_state.check_conflicts_deterministic_reordering()
    w2_conflicts = w2_state.check_conflicts_deterministic_reordering()
    await w1_state.commit(w2_conflicts)
    await w2_state.commit(w1_conflicts)
    return w1_conflicts, w2_conflicts


async def transaction(k1, k1_state, k2, k2_state, t_id):
    k1_value = await k1_state.get(key=k1, t_id=t_id, operator_name="test")
    await k1_state.put(key=k1, value=k1_value - 1, t_id=t_id, operator_name="test")
    k2_value = await k2_state.get(key=k2, t_id=t_id, operator_name="test")
    await k2_state.put(key=k2, value=k2_value + 1, t_id=t_id, operator_name="test")


async def rerun_conflicts(w1_conflicts, w2_conflicts, transactions):
    for w1_conflict in w1_conflicts:
        await transaction(**transactions[w1_conflict])
    for w2_conflict in w2_conflicts:
        await transaction(**transactions[w2_conflict])
