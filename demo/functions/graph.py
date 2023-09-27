from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph

from demo.functions.order import order_operator
from demo.functions.stock import stock_operator
from demo.functions.user import user_operator

####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('shopping-cart', operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
g.add_operators(user_operator, stock_operator, order_operator)
