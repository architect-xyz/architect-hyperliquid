import time
import asyncio
import uuid
from datetime import datetime
from eth_typing import ChecksumAddress
import grpc
import logging
import argparse
import json
from pprint import pprint
from decimal import Decimal
from concurrent import futures
from architect_py.scalars import OrderDir
from architect_py.grpc_client.Cpty.CptyRequest import CancelOrder, PlaceOrder
from architect_py.grpc_client.Cpty.CptyResponse import (
    Symbology,
    UpdateAccountSummary,
    ReconcileOpenOrder,
)
from architect_py.grpc_client.Oms.Order import Order, OrderType
from architect_py.grpc_client.definitions import (
    CptyLoginRequest,
    CptyLogoutRequest,
    ExecutionInfo,
    AccountStatistics,
    AccountPosition,
    OrderId,
    OrderStatus,
    OrderSource,
    TimeInForce,
    TimeInForceEnum,
)
from architect_py.grpc_client.grpc_server import (
    add_CptyServicer_to_server,
    CptyServicer,
    add_OrderflowServicer_to_server,
    OrderflowServicer,
)
import eth_account
import hyperliquid
import hyperliquid.info
import hyperliquid.exchange
from hyperliquid.utils.types import Cloid
from eth_account.signers.local import LocalAccount

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s")
logger.setLevel(logging.DEBUG)


class MockCptyServicer(CptyServicer, OrderflowServicer):
    hyperliquid_account_address: ChecksumAddress
    hyperliquid_info: hyperliquid.info.Info
    hyperliquid_exchange: hyperliquid.exchange.Exchange

    architect_trader_id: uuid.UUID
    architect_account_id: uuid.UUID
    architect_symbology: Symbology
    architect_symbols: dict[str, str]  # map of hyperliquid symbol => architect symbol

    next_connection_id: int
    broadcast_queues: dict[int, asyncio.Queue]
    orderflow_queues: dict[int, asyncio.Queue]

    def __init__(
        self,
        *,
        base_url: str,
        account: LocalAccount,
        account_address: ChecksumAddress,
        account_id: uuid.UUID,
        trader_id: uuid.UUID,
    ):
        self.hyperliquid_account_address = account_address
        self.hyperliquid_info = hyperliquid.info.Info(base_url)
        self.hyperliquid_exchange = hyperliquid.exchange.Exchange(
            account, base_url, account_address=account_address
        )

        # load hyperliquid markets and convert to Architect symbology
        self.architect_symbols = {}
        execution_info: dict[str, dict[str, ExecutionInfo]] = {}
        meta = self.hyperliquid_info.meta()
        for perp_market in meta["universe"]:
            tradable_product = (
                f"{perp_market['name']}-USDC Hyperliquid Perpetual/USDC Crypto"
            )
            self.architect_symbols[perp_market["name"]] = tradable_product
            step_size = Decimal(10) ** (-perp_market["szDecimals"])
            MAX_DECIMALS = 6
            tick_size = Decimal(10) ** (-(MAX_DECIMALS - perp_market["szDecimals"]))
            execution_info[tradable_product] = {
                "HYPERLIQUID": {
                    "execution_venue": "HYPERLIQUID",
                    "exchange_symbol": perp_market["name"],
                    "tick_size": {"simple": tick_size},
                    "step_size": step_size,
                    "min_order_quantity": "0",
                    "min_order_quantity_unit": {"unit": "base"},
                    "is_delisted": False,
                    "initial_margin": None,
                    "maintenance_margin": None,
                }
            }

        self.architect_trader_id = trader_id
        self.architect_account_id = account_id
        self.architect_symbology = Symbology(execution_info=execution_info)
        self.next_connection_id = 1
        self.broadcast_queues = {}
        self.orderflow_queues = {}

    def architect_to_hyperliquid_order(self, ax_order: Order):
        if ax_order.symbol in self.architect_symbology.execution_info:
            execution_info = self.architect_symbology.execution_info[ax_order.symbol][
                "HYPERLIQUID"
            ]
        else:
            logger.error("no execution info for symbol: %s", ax_order.symbol)
            return None
        return {
            "coin": execution_info["exchange_symbol"],
            "is_buy": ax_order.dir == OrderDir.BUY,
            "sz": float(ax_order.quantity),
            "limit_px": float(ax_order.limit_price),
            "order_type": {"limit": {"tif": "Gtc"}},  # CR alee: not always
            "reduce_only": False,
            "cloid": Cloid(f"{ax_order.id.seqid}:{ax_order.id.seqno}"),
        }

    def hyperliquid_to_architect_order(self, hl_order):
        if hl_order["coin"] in self.architect_symbols:
            symbol = self.architect_symbols[hl_order["coin"]]
        else:
            logger.error("unknown coin: %s in order %s", hl_order["coin"], hl_order)
            return None
        if hl_order["side"] == "B":
            dir = OrderDir.BUY
        elif hl_order["side"] == "A":
            dir = OrderDir.SELL
        else:
            logger.error("unknown side: %s in order %s", hl_order["side"], hl_order)
            return None
        if hl_order["cloid"] is not None:
            cloid_parts = hl_order["cloid"].split(":")
        if len(cloid_parts) == 2:
            seqid = cloid_parts[0]
            seqno = int(cloid_parts[1])
            order_id = OrderId(seqid=seqid, seqno=seqno)
        else:
            # CR alee: check that this is unique enough
            order_id = OrderId(
                seqid="4fc22baf-2529-49be-a0c2-73a4c379cdcb", seqno=int(hl_order["oid"])
            )
        sz = Decimal(hl_order["sz"])
        orig_sz = Decimal(hl_order["origSz"])
        dt = datetime.fromtimestamp(hl_order["timestamp"] / 1000.0)
        return Order.new(
            account=str(self.architect_account_id),
            dir=dir,
            id=order_id,
            status=OrderStatus.Open,
            quantity=orig_sz,
            symbol=symbol,
            source=OrderSource.Reconciled,
            time_in_force=TimeInForceEnum.GTC,  # CR alee: not always
            recv_time=int(dt.timestamp()),
            recv_time_ns=int((dt.timestamp() % 1) * 1_000_000_000),
            trader=str(self.architect_trader_id),
            execution_venue="HYPERLIQUID",
            filled_quantity=(orig_sz - sz),
            limit_price=Decimal(hl_order["limitPx"]),
            order_type=OrderType.LIMIT,  # CR alee: not always
            post_only=False,  # CR alee: unrecoverable info
            exchange_order_id=str(hl_order["oid"]),
        )

    async def periodically_snapshot_accounts(self):
        while True:
            user_state = self.hyperliquid_info.user_state(
                address=self.hyperliquid_account_address
            )
            logger.debug("user_state: %s", json.dumps(user_state))
            dt = datetime.fromtimestamp(user_state["time"] / 1000.0)
            balances = {
                "USDC Crypto": Decimal(user_state["marginSummary"]["totalRawUsd"])
            }
            positions = {}
            for ap in user_state["assetPositions"]:
                p = ap["position"]
                symbol = self.architect_symbols[p["coin"]]
                if p["liquidationPx"] is not None:
                    liquidation_price = Decimal(p["liquidationPx"])
                else:
                    liquidation_price = None
                positions[symbol] = AccountPosition(
                    quantity=Decimal(p["szi"]),
                    cost_basis=Decimal(p["entryPx"])
                    * Decimal(p["szi"]),  # CR alee: might have to multiply by qty
                    liquidation_price=liquidation_price,
                )
            account_summary = UpdateAccountSummary(
                account=str(self.architect_account_id),
                is_snapshot=True,
                timestamp=int(dt.timestamp()),
                timestamp_ns=int((dt.timestamp() % 1) * 1_000_000_000),
                balances=balances,
                positions=positions,
                statistics=AccountStatistics(
                    equity=Decimal(user_state["marginSummary"]["accountValue"]),
                    position_margin=Decimal(
                        user_state["marginSummary"]["totalMarginUsed"]
                    ),
                ),
            )
            logger.debug("account_summary: %s", account_summary)
            for queue in self.broadcast_queues.values():
                queue.put_nowait(account_summary)
            await asyncio.sleep(3)

    async def snapshot_open_orders(self):
        ax_open_orders = []
        hl_open_orders = self.hyperliquid_info.frontend_open_orders(
            address=self.hyperliquid_account_address
        )
        logger.debug("open orders snapshot: %s", json.dumps(hl_open_orders))
        for oo in hl_open_orders:
            ax_order = self.hyperliquid_to_architect_order(oo)
            if ax_order is not None:
                ax_open_orders.append(ax_order)
        return ax_open_orders

    async def Cpty(self, request_iterator, context):
        connection_id = self.next_connection_id
        self.next_connection_id += 1
        try:
            context.set_code(grpc.StatusCode.OK)
            context.send_initial_metadata({})
            # send Architect symbology
            yield self.architect_symbology
            # send open orders snapshot
            open_orders = await self.snapshot_open_orders()
            yield ReconcileOpenOrder(
                orders=open_orders, snapshot_for_account=str(self.architect_account_id)
            )
            # register this RPC handler for broadcast events
            queue = asyncio.Queue()
            self.broadcast_queues[connection_id] = queue
            # start a task to handle cpty requests
            cpty_task = asyncio.create_task(
                self.serve_cpty_requests(
                    connection_id, queue, request_iterator, context
                )
            )
            while True:
                cpty_response = await queue.get()
                yield cpty_response
            # serve CptyRequests
        finally:
            del self.broadcast_queues[connection_id]

    async def serve_cpty_requests(
        self, connection_id: int, queue: asyncio.Queue, request_iterator, context
    ):
        async for req in request_iterator:
            logger.debug("(connection_id=%d) received: %s", connection_id, req)
            if isinstance(req, PlaceOrder):
                hl_order = self.architect_to_hyperliquid_order(req)
                if hl_order is not None:
                    result = self.hyperliquid_exchange.order(**hl_order)
                    print(result)
                else:
                    logger.error(
                        "failed to convert architect order to hyperliquid order: %s",
                        req,
                    )
            elif isinstance(req, CancelOrder):
                pass

    def SubscribeOrderflow(self, request, context):
        context.set_code(grpc.StatusCode.OK)
        context.send_initial_metadata({})
        time.sleep(100)


async def main():
    parser = argparse.ArgumentParser(prog="architect-hyperliquid")
    parser.add_argument("--config", type=str, required=True)
    parser.add_argument("--log-level", type=str, default="DEBUG")
    args = parser.parse_args()

    logger.setLevel(args.log_level)
    logger.info("loading config from %s...", args.config)
    with open(args.config, "r") as f:
        config = json.load(f)
    logger.info("config loaded")

    if "base_url" in config:
        base_url = config["base_url"]
    else:
        base_url = "https://api.hyperliquid.xyz"
    logger.info("using base url: %s", base_url)

    account: LocalAccount = eth_account.Account.from_key(config["secret_key"])
    address = config["account_address"]
    if address == "":
        address = account.address
    logger.info("running with account address: %s", address)
    if address != account.address:
        logger.info("running with agent address: %s", account.address)

    servicer = MockCptyServicer(
        base_url=base_url,
        account=account,
        account_address=address,
        account_id=uuid.UUID(config["account_id"]),
        trader_id=uuid.UUID(config["trader_id"]),
    )
    await servicer.snapshot_open_orders()
    server = grpc.aio.server()
    add_CptyServicer_to_server(servicer, server)
    add_OrderflowServicer_to_server(servicer, server)
    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)
    logger.info("starting server on %s...", listen_addr)
    await server.start()
    logger.info("server started")
    server_task = asyncio.create_task(server.wait_for_termination(), name="server")
    snapshot_task = asyncio.create_task(
        servicer.periodically_snapshot_accounts(), name="snapshot"
    )
    await asyncio.wait(
        [server_task, snapshot_task], return_when=asyncio.FIRST_COMPLETED
    )


if __name__ == "__main__":
    asyncio.run(main())
