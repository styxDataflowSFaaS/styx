import json
import random

from locust import HttpUser, SequentialTaskSet, between, task, constant

FRONTEND_URL = 'http://frontend-load-balancer'


def create_user(session):
    with session.client.post(f"{FRONTEND_URL}/user/create", name="/user/create",
                             catch_response=True) as response:
        try:
            session.user_key = response.json()['user_key']
        except json.JSONDecodeError:
            response.failure("SERVER ERROR")


def add_balance_to_user(session):
    # balance_to_add: float = random.uniform(10000.0, 100000.0)
    balance_to_add = 1000
    session.client.post(f"{FRONTEND_URL}/user/add_credit/{session.user_key}/{balance_to_add}",
                        name="/user/add_credit/[user_key]/[amount]")


def create_item(session):
    with session.client.post(f"{FRONTEND_URL}/stock/create", name="/stock/create",
                             catch_response=True) as response:
        try:
            item_key = response.json()['item_key']
        except json.JSONDecodeError:
            response.failure("SERVER ERROR")
        else:
            session.item_keys.append(item_key)


def add_stock(session, item_idx: int):
    # stock_to_add = random.randint(100, 1000)
    stock_to_add = 100
    session.client.post(f"{FRONTEND_URL}/stock/add_stock/{session.item_keys[item_idx]}/{stock_to_add}",
                        name="/stock/add_stock/[item_key]/[amount]")


def create_order(session):
    with session.client.post(f"{FRONTEND_URL}/order/create_order/{session.user_key}",
                             name="/order/create_order/[user_key]", catch_response=True) as response:
        try:
            session.order_key = response.json()['order_key']
        except json.JSONDecodeError:
            response.failure("SERVER ERROR")


def add_item_to_order(session, item_idx: int):
    with session.client.post(f"{FRONTEND_URL}/order/add_item/{session.order_key}/{session.item_keys[item_idx]}",
                             name="/order/add_item/[item_key]", catch_response=True) as response:
        if 400 <= response.status_code < 500:
            response.failure(response.text)
        else:
            response.success()


def checkout_order(session):
    with session.client.post(f"{FRONTEND_URL}/order/checkout/{session.order_key}", name="/order/checkout/[order_key]",
                             catch_response=True) as response:
        if 400 <= response.status_code < 500:
            response.failure(response.text)
        else:
            response.success()


class LoadTest1(SequentialTaskSet):
    """
    Scenario where a stock admin creates an item and adds stock to it
    """
    item_keys: list[str]

    def on_start(self):
        """ on_start is called when a Locust start before any task is scheduled """
        self.item_keys = list()

    def on_stop(self):
        """ on_stop is called when the TaskSet is stopping """
        self.on_start()

    @task
    def admin_creates_item(self): create_item(self)

    @task
    def admin_adds_stock_to_item(self): add_stock(self, 0)


class LoadTest2(SequentialTaskSet):
    """
    Scenario where a user checks out an order with one item inside that an admin has added stock to before
    """
    item_keys: list[str]
    user_key: str
    order_key: str

    def on_start(self):
        self.item_keys = list()
        self.user_key = ""
        self.order_key = ""

    def on_stop(self):
        self.on_start()

    @task
    def admin_creates_item(self): create_item(self)  # stock service

    @task
    def admin_adds_stock_to_item(self): add_stock(self, 0)  # stock service

    @task
    def user_creates_account(self): create_user(self)  # user service

    @task
    def user_adds_balance(self): add_balance_to_user(self)  # user service

    @task
    def user_creates_order(self): create_order(self)  # order

    @task
    def user_adds_item_to_order(self): add_item_to_order(self, 0)  # order

    @task
    def user_checks_out_order(self): checkout_order(self)  # order


class MicroservicesUser(HttpUser):
    # wait_time = between(0, 1)  # how much time a user waits (seconds) to run another TaskSequence
    wait_time = constant(1)
    # [SequentialTaskSet]: [weight of the SequentialTaskSet]
    tasks = {
        # LoadTest1: 50,
        LoadTest2: 100
    }
