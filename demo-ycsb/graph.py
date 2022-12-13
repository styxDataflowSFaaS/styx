from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph

from ycsb import ycsb_operator

####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('ycsb_benchmark', operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
g.add_operators(ycsb_operator)
