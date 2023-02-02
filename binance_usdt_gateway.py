"""
1. 只支持全仓模式
2. 只支持单向持仓模式
3. 只支持正向合约
"""

import urllib
import hashlib
import hmac
import time
from copy import copy
from datetime import datetime, timedelta
from enum import Enum
from threading import Lock
from typing import Any, Dict, List, Tuple
from vnpy.trader.utility import round_to
import pytz

from requests.exceptions import SSLError
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Product,
    Status,
    OrderType,
    Interval,
    Offset
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    AccountData,
    ContractData,
    PositionData,
    BarData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)
from vnpy.trader.event import EVENT_TIMER
from vnpy.event import Event, EventEngine

from vnpy_rest import Request, RestClient, Response
from vnpy_websocket import WebsocketClient
from asyncio import run_coroutine_threadsafe
from csv import writer
from os.path import exists
from os import mkdir

# 中国时区
CHINA_TZ = pytz.timezone("Asia/Shanghai")

# 实盘正向合约REST API地址
F_REST_HOST: str = "https://fapi.binance.com"

# 实盘正向合约Websocket API地址
F_WEBSOCKET_TRADE_HOST: str = "wss://fstream.binance.com/ws/"
F_WEBSOCKET_DATA_HOST: str = "wss://fstream.binance.com/stream"

# 模拟盘正向合约REST API地址
F_TESTNET_REST_HOST: str = "https://testnet.binancefuture.com"

# 模拟盘正向合约Websocket API地址
F_TESTNET_WEBSOCKET_TRADE_HOST: str = "wss://stream.binancefuture.com/ws/"
F_TESTNET_WEBSOCKET_DATA_HOST: str = "wss://stream.binancefuture.com/stream"

# 委托状态映射
STATUS_BINANCES2VT: Dict[str, Status] = {
    "NEW": Status.NOTTRADED,
    "PARTIALLY_FILLED": Status.PARTTRADED,
    "FILLED": Status.ALLTRADED,
    "CANCELED": Status.CANCELLED,
    "REJECTED": Status.REJECTED,   # 这个并非BN的订单状态，而是自己封装的
    "EXPIRED": Status.EXPIRED
}

# 委托类型映射
ORDERTYPE_VT2BINANCES: Dict[OrderType, Tuple[str, str]] = {
    OrderType.STOP: ("STOP_MARKET", "GTC"),
    OrderType.LIMIT: ("LIMIT", "GTC"),
    OrderType.MARKET: ("MARKET", "GTC"),
    OrderType.FAK: ("LIMIT", "IOC"),  # IOC - Immediate or Cancel 无法立即成交(吃单)的部分就撤销
    OrderType.FOK: ("LIMIT", "FOK"),
}
ORDERTYPE_BINANCES2VT: Dict[Tuple[str, str], OrderType] = {v: k for k, v in ORDERTYPE_VT2BINANCES.items()}

# 买卖方向映射
DIRECTION_VT2BINANCES: Dict[Direction, str] = {
    Direction.LONG: "BUY",
    Direction.SHORT: "SELL"
}
DIRECTION_BINANCES2VT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2BINANCES.items()}

# 数据频率映射
INTERVAL_VT2BINANCES: Dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",
}

# 时间间隔映射
TIMEDELTA_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}

# 合约数据全局缓存字典
symbol_contract_map: Dict[str, ContractData] = {}


# 鉴权类型
class Security(Enum):
    NONE: int = 0
    SIGNED: int = 1
    API_KEY: int = 2


class BinanceUsdtGateway(BaseGateway):
    """
    vn.py用于对接币安正向合约的交易接口。
    """

    default_setting: Dict[str, Any] = {
        "key": "",
        "secret": "",
        "服务器": ["REAL", "TESTNET"],
        "代理地址": "",
        "代理端口": 0,
        "杠杆": 0,  # 0表示不用调
    }

    exchanges: Exchange = [Exchange.BINANCE]

    def __init__(self, event_engine: EventEngine, gateway_name: str = "BINANCE_USDT") -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.trade_ws_api: "BinanceUsdtTradeWebsocketApi" = BinanceUsdtTradeWebsocketApi(self)
        self.market_ws_api: "BinanceUsdtDataWebsocketApi" = BinanceUsdtDataWebsocketApi(self)
        self.rest_api: "BinanceUsdtRestApi" = BinanceUsdtRestApi(self)

        self.orders: Dict[str, OrderData] = {}
        self.time_count = 0  # 时间计数器，用来计算每180秒更新一次account的变量

    def connect(self, setting: dict) -> None:
        """
        连接交易接口：交易ws和行情ws不定时自动重连，是在继承的父类WebsocketClient中执行，不稳定就重连，经常重连是有问题的
        具体语句在start函数中的run_coroutine_threadsafe(self._run(), self._loop)
        """
        key: str = setting["key"]
        secret: str = setting["secret"]
        server: str = setting["服务器"]
        proxy_host: str = setting["代理地址"]
        proxy_port: int = setting["代理端口"]
        leverage: int = setting["杠杆"]

        self.rest_api.connect(key, secret, server, proxy_host, proxy_port, leverage)
        self.market_ws_api.connect(proxy_host, proxy_port, server)

        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.market_ws_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        self.rest_api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        pass

    def query_position(self) -> None:
        """查询持仓"""
        pass

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        return self.rest_api.query_history(req)

    def query_income(self, start_time, end_time, limit):
        self.rest_api.query_income(start_time, end_time, limit)

    def query_fund(self, symbol, start_time, end_time, limit):
        self.rest_api.query_fund(symbol, start_time, end_time, limit)

    def close(self) -> None:
        """关闭连接"""
        self.rest_api.stop()
        self.trade_ws_api.stop()
        self.market_ws_api.stop()

    def process_timer_event(self, event: Event) -> None:
        """定时事件处理"""
        self.rest_api.keep_user_stream()
        self.time_count += 1
        if self.time_count >= 300:  # 每300秒从交易所拉取账户信息
            self.rest_api.query_time()   # 二次校准time_offset,避免发到交易所的订单因时间戳超前或者延迟而被拒
            self.rest_api.query_account()
            self.time_count = 0

    def on_order(self, order: OrderData) -> None:
        """推送委托数据"""
        self.orders[order.orderid] = copy(order)
        super().on_order(order)

    def get_order(self, orderid: str) -> OrderData:
        """查询委托数据"""
        return self.orders.get(orderid, None)


class BinanceUsdtRestApi(RestClient):
    """币安正向合约的REST API"""

    def __init__(self, gateway: BinanceUsdtGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: BinanceUsdtGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.trade_ws_api: BinanceUsdtTradeWebsocketApi = self.gateway.trade_ws_api

        self.key: str = ""
        self.secret: str = ""

        self.user_stream_key: str = ""
        self.keep_alive_count: int = 0
        self.recv_window: int = 5000
        self.time_offset: int = 0

        self.order_count: int = 1_000_000
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0

        self.resend_max_count = 20    # 在单次进程中因BN交易所忙碌而尝试重发的总次数
        self.resend_count = 0
        self.restart_max_count = 20
        self.restart_count = 0
        self.requery_max_count = 3
        self.requery_count = 0

    def sign(self, request: Request) -> Request:
        """生成币安签名"""
        security: Security = request.data["security"]
        if security == Security.NONE:
            request.data = None
            return request

        if request.params:
            path: str = request.path + "?" + urllib.parse.urlencode(request.params)
        else:
            request.params = dict()
            path: str = request.path

        if security == Security.SIGNED:
            timestamp: int = int(time.time() * 1000)

            if self.time_offset > 0:
                timestamp -= abs(self.time_offset)
            elif self.time_offset < 0:
                timestamp += abs(self.time_offset)

            request.params["timestamp"] = timestamp

            query: str = urllib.parse.urlencode(sorted(request.params.items()))
            signature: bytes = hmac.new(self.secret, query.encode(
                "utf-8"), hashlib.sha256).hexdigest()

            query += "&signature={}".format(signature)
            path: str = request.path + "?" + query

        request.path = path
        request.params = {}
        request.data = {}

        # 添加请求头
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "X-MBX-APIKEY": self.key,
            "Connection": "close"
        }

        if security in [Security.SIGNED, Security.API_KEY]:
            request.headers = headers

        return request

    def connect(
            self,
            key: str,
            secret: str,
            server: str,
            proxy_host: str,
            proxy_port: int,
            leverage: int
    ) -> None:
        """连接REST服务器"""
        self.key = key
        self.secret = secret.encode()
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host
        self.server = server
        self.leverage = leverage

        self.connect_time = (
                int(datetime.now().strftime("%y%m%d%H%M%S")) * self.order_count
        )

        if self.server == "REAL":
            self.init(F_REST_HOST, proxy_host, proxy_port)
        else:
            self.init(F_TESTNET_REST_HOST, proxy_host, proxy_port)

        self.start()

        self.gateway.write_log("REST API启动成功")

        self.query_time()
        self.query_account()
        self.query_position()
        self.query_order()
        self.start_user_stream()
        self.query_contract()  # 新增了调整合约杠杆，调杠杠的时间长些，因此与start_user_stream函数运行顺序对调

    def set_leverage(self, symbol, leverage):
        """ 调整开仓杠杆 """
        data: dict = {
            "security": Security.SIGNED
        }

        path: str = "/fapi/v1/leverage"

        params = {
            "symbol": symbol,
            "leverage": leverage,
            "recvWindow": 20000  # 20000毫秒前的请求,bn都会处理(如果没有就会报那个时间窗口不一致的异常)
        }

        self.add_request(
            method="POST",
            path=path,
            callback=self.on_set_leverage,
            data=data,
            params=params,
            on_failed=self.on_set_leverage_failed
        )

    def on_set_leverage_failed(self, status_code: str, request: Request) -> None:
        """
        调整杠杆失败后报错
        """
        symbol = request.path.split('&symbol=')[1].split('&timestamp=')[0]
        msg = f"{symbol}调整杠杆失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)

    def query_time(self) -> None:
        """查询时间"""
        data: dict = {
            "security": Security.NONE
        }

        path: str = "/fapi/v1/time"

        return self.add_request(
            "GET",
            path,
            callback=self.on_query_time,
            on_failed=self.on_query_time_failed,   # 查询失败暂不处理以免请求过多被bn墙了
            data=data
        )

    def on_query_time_failed(self, status_code: str, request: Request) -> None:
        if self.requery_count <= self.requery_max_count:
            self.requery_count += 1
            msg = f'查询时间失败:{status_code},第{self.requery_count}次重新查询服务器时间, ' \
                  f'request:{request.response.__dict__}'
            self.gateway.write_log(msg)
            self.query_time()  # 重试查询服务器时间

    def query_account(self) -> None:
        """查询资金"""
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v2/account"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_account,
            data=data
        )

    def query_position(self) -> None:
        """查询持仓"""
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v1/positionRisk"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_position,
            data=data
        )

    def query_order(self) -> None:
        """查询未成交委托"""
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v1/openOrders"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_order,
            data=data
        )

    def query_contract(self) -> None:
        """查询合约信息"""
        data: dict = {
            "security": Security.NONE
        }

        path: str = "/fapi/v1/exchangeInfo"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_contract,
            data=data
        )

    def query_fund(self, symbol, start_time, end_time, limit):
        """
        查询资金费率
        """

        all_fund = []  # 记录所有时间的资金费

        # 如果有开始时间就用开始时间，没有就不填
        if start_time:
            start_time_a = time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            start_time_t = int(time.mktime(start_time_a)) * 1000
        else:
            start_time_t = ''

        # 如果有结束时间就用结束时间，没有就用现在的时间
        if end_time:
            end_time_a = time.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            end_time_t = int(time.mktime(end_time_a)) * 1000
        else:
            end_time_t = int(time.time()) * 1000

        while True:
            path = '/fapi/v1/fundingRate'
            params: dict = {
                "startTime": start_time_t,
                "endTime": end_time_t,
                "symbol": symbol,
                "limit": limit
            }

            resp: Response = self.request(
                "GET",
                path=path,
                data={"security": Security.NONE},
                params=params
            )

            # 如果请求失败则终止循环
            if resp.status_code // 100 != 2:
                msg: str = f"获取资金费失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data: dict = resp.json()
                if not data:
                    st = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time_t / 1000))
                    msg: str = f"获取{symbol}资金费为空，开始时间：{st}"
                    self.gateway.write_log(msg)
                    break
                else:
                    fund = []  # 记录每一次的资金费
                    for d in data:
                        fund.append([
                            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(d["fundingTime"] / 1000)),
                            float(d["fundingRate"])
                        ])

                    all_fund.extend(fund)

                    # 如果收到了最后一批数据或超过指定的结束时间，则终止循环
                    if len(data) < limit or data[-1]["fundingTime"] > end_time_t:
                        break

                    # 更新开始时间
                    start_time_t = data[-1]["fundingTime"] + 1

        if all_fund:
            msg: str = f"获取{symbol}资金费成功，{all_fund[0][0]} - {all_fund[-1][0]}"
            self.gateway.write_log(msg)

            save_path = "FUND"
            if not exists(save_path):
                mkdir(save_path)

            name = symbol + "_fundingRate" + ".csv"
            f = open(save_path + "/" + name, 'w', newline='')
            title = "fundingTime, fundingRate"
            f.write(title + "\n")
            csv_writer = writer(f)
            for ic in all_fund:
                csv_writer.writerow(ic)
            f.close()
            self.gateway.write_log("资金流水csv保存成功")

    def query_income(self, start_time, end_time, limit):
        """
        查询账户损益资金流水
        """
        all_income = []

        start_time_a = time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time_t = int(time.mktime(start_time_a)) * 1000

        # 如果有结束时间，就填结束时间，没有就至今
        if end_time:
            end_time_a = time.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            end_time_t = int(time.mktime(end_time_a)) * 1000
        else:
            end_time_t = int(time.time()) * 1000

        # 请求参数
        while True:
            path = '/fapi/v1/income'
            params: dict = {
                "startTime": start_time_t,
                "endTime": end_time_t,
                "limit": limit
            }

            resp: Response = self.request(
                "GET",
                path=path,
                data={"security": Security.SIGNED},
                params=params
            )

            # 如果请求失败则终止循环
            if resp.status_code // 100 != 2:
                msg: str = f"获取资金流水失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data: dict = resp.json()
                if not data:
                    st = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time_t / 1000))
                    msg: str = f"获取资金流水为空，开始时间：{st}"
                    self.gateway.write_log(msg)
                    break

                buf = []  # 每一次限制的income信息

                for row in data:

                    # symbol,incomeType,income,asset,time,info,tranId,tradeId
                    income = []  # 每一条income信息
                    for i in row.values():
                        income.append(i)

                    if income[1] == "TRANSFER":
                        income[1] = "划转"
                    elif income[1] == "WELCOME_BONUS":
                        income[1] == "体验金"
                    elif income[1] == "REALIZED_PNL":
                        income[1] = "已实现盈亏"
                    elif income[1] == "FUNDING_FEE":
                        income[1] = "资金费用"
                    elif income[1] == "COMMISSION":
                        income[1] = "手续费"
                    elif income[1] == "INSURANCE_CLEAR":
                        income[1] = "爆仓清算"
                    elif income[1] == "REFERRAL_KICKBACK":
                        income[1] = "推荐人返佣"
                    elif income[1] == "COMMISSION_REBATE":
                        income[1] = "被推荐人返现"
                    elif income[1] == "DELIVERED_SETTELMENT":
                        income[1] = "下架结算"
                    elif income[1] == "COIN_SWAP_DEPOSIT":
                        income[1] = "资产转换转入"
                    elif income[1] == "COIN_SWAP_WITHDRAW":
                        income[1] = "资产转换转出"

                    income[4] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(income[4] / 1000))

                    buf.append(income)

                begin = buf[0][4]
                end = buf[-1][4]

                all_income.extend(buf)
                msg: str = f"获取资金流水成功，{begin} - {end}"
                self.gateway.write_log(msg)

                # 如果收到了最后一批数据或超过指定的结束时间，则终止循环
                print(data[-1]['time'])
                print(end_time_t)
                if len(data) < limit or data[-1]['time'] > end_time_t:
                    break

                # 更新开始时间
                time_str = buf[-1][4]
                start_time_a = time.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                start_time_t = int(time.mktime(start_time_a)) * 1000

        today = datetime.today()
        name = "income_" + str(today.strftime("%Y-%m-%d")) + ".csv"
        f = open(name, 'w', newline='')
        title = "symbol,incomeType,income,asset,time,info,tranId,tradeId"
        f.write(title + "\n")
        csv_writer = writer(f)
        for ic in all_income:
            csv_writer.writerow(ic)
        f.close()
        self.gateway.write_log("资金流水csv保存成功")

    def _new_order_id(self) -> int:
        """生成本地委托号"""
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""

        # 生成本地委托号
        order_offset = ''
        if req.offset == Offset.OPEN:
            order_offset = "Open"
        elif req.offset == Offset.CLOSE:
            order_offset = 'Close'

        orderid: str = order_offset + str(self.connect_time + self._new_order_id())

        # 推送提交中事件
        order: OrderData = req.create_order_data(
            orderid,
            self.gateway_name
        )
        self.gateway.on_order(order)

        data: dict = {
            "security": Security.SIGNED
        }

        # 生成委托请求
        # 停止单参数
        if req.type == OrderType.STOP:
            params: dict = {
                "symbol": req.symbol,
                "side": DIRECTION_VT2BINANCES[req.direction],
                "quantity": float(req.volume),
                "newClientOrderId": orderid,
                "type": "STOP_MARKET",
                "stopPrice": req.price,
                "closePosition": False,  # false才可以填量，否则就是持仓的全部量了
                "workingType": "MARK_PRICE"
            }

        # 非停止单参数
        else:
            params: dict = {
                "symbol": req.symbol,
                "side": DIRECTION_VT2BINANCES[req.direction],
                "quantity": float(req.volume),
                "newClientOrderId": orderid,
            }

            if req.type == OrderType.MARKET:
                params["type"] = "MARKET"
            else:
                order_type, time_condition = ORDERTYPE_VT2BINANCES[req.type]
                params["type"] = order_type
                params["timeInForce"] = time_condition
                params["price"] = float(req.price)

        # 对于stop order和非stop order订单，全部增加延时的容忍度; Binance默认的recvWindow是5000毫秒
        params["recvWindow"] = 20000  # 单位是毫秒

        path: str = "/fapi/v1/order"

        # 发单改为了同步请求
        resp = self.request(
            method="POST",
            path=path,
            params=params,
            data=data
        )

        # 对错误的处理，建议先人工处理，然后明确哪些错误是可以重新发单的再编写
        if isinstance(resp, Response):

            # 发单成功时，返回的是vt_orderid; 当发单不成功时，返回error msg
            # 当下单成功时，只要成功下单都会有返回vt_orderid
            if resp.status_code // 100 == 2:
                self.resend_count = 0    # 下单成功时，重置内部错误重发次数（避免需要频繁重启）
                return order.vt_orderid

            # 当网络状态码不为2，是有问题的
            else:
                try:
                    data = resp.json()
                    # 可以获取bn错误码的情况
                    # 限价单遇到内部错误时，需要进行重发，此时订单会显示提交中(是由于系统负责量过大，没有接受到请求，需要重试发单)
                    if data['code'] == -1001:
                        # bn服务器忙碌，1~2秒后再发单，不应该sleep太久，一般此时正是行情剧烈波动的时候
                        time.sleep(1)
                        if self.resend_count < self.resend_max_count:
                            self.resend_count += 1
                            msg = f"{req.symbol}因bn服务器忙碌，第{self.resend_count}次重发单。委托下单{orderid}失败，" \
                                  f"网络状态码：{resp.status_code}，网络返回：{resp.text}。bn信息：{data.get('msg')}"
                            self.gateway.write_log(msg)
                            return self.send_order(req)  # 递归调用发单函数
                        else:
                            msg = f"{req.symbol}-{orderid} error:因bn服务器忙碌的重发次数已达到上限{self.resend_max_count}次"
                            self.gateway.write_log(msg)
                            return msg

                    # 重复发失败的，返回error字段
                    else:
                        msg = f"{req.symbol}-{orderid} error:bn错误代码不是-1001，" \
                              f"bn错误代码：{data.get('code')}，bn信息：{data.get('msg')}"
                        self.gateway.write_log(msg)

                        # 时间戳 out of the recvWindow，疑似网络错误，需要重启
                        # if data['code'] == -1021:
                        #    pass
                        return msg

                # 不能获取bn错误码的情况，返回error字段
                except:
                    msg = f"{req.symbol}-{orderid} error:无法获取bn错误码，网络返回：{resp.status_code}，信息：{resp.text}"
                    self.gateway.write_log(msg)
                    return msg

        # request发不出的情况，返回error字段
        else:
            msg = f"{req.symbol}-{orderid} error:网络连接失败，委托下单失败。{orderid} 信息{resp},{type(resp)}"
            self.gateway.write_log(msg)
            return msg

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        data: dict = {
            "security": Security.SIGNED
        }

        params: dict = {
            "symbol": req.symbol,
            "origClientOrderId": req.orderid,
            "recvWindow": 20000  # 对于stop order和非stop order订单，全部增加延时的容忍度; Binance默认的recvWindow是5000毫秒
        }

        path: str = "/fapi/v1/order"

        order: OrderData = self.gateway.get_order(req.orderid)

        self.add_request(
            method="DELETE",
            path=path,
            callback=self.on_cancel_order,
            params=params,
            data=data,
            on_failed=self.on_cancel_failed,
            extra=order
        )

    def start_user_stream(self) -> Request:
        """ 生成交易ws的listenKey """
        data: dict = {
            "security": Security.API_KEY
        }

        path: str = "/fapi/v1/listenKey"

        self.add_request(
            method="POST",
            path=path,
            callback=self.on_start_user_stream,
            on_failed=self.on_start_user_stream_failed,
            data=data
        )

    def on_start_user_stream_failed(self, status_code: str, request: Request) -> None:
        """
        当遇到code：-1001（Internal error）的错误返回时，需要尝试重新申请 listenKey
        """
        time.sleep(1)
        self.gateway.write_log(f'status_code:{status_code}, request:{request.__dict__}')

        if self.restart_count <= self.restart_max_count:
            self.restart_count += 1
            self.start_user_stream()

    def keep_user_stream(self) -> Request:
        """
        延长交易ws的listenKey有效期（Binance文档上说要每小时延长一次有效期）
        listenkey是用来订阅ws交易信息的，交易信息和行情信息是两个不同的ws
        如果交易ws的listenKey仍在有效期内的话，单次延长listenKey有效期是出错可能不会有问题
        """
        self.keep_alive_count += 1
        if self.keep_alive_count < 600:  # 目前每隔10分钟延长listenKey有效期
            return
        self.keep_alive_count = 0

        data: dict = {
            "security": Security.API_KEY
        }

        params: dict = {
            "listenKey": self.user_stream_key
        }

        path: str = "/fapi/v1/listenKey"

        self.add_request(
            method="PUT",
            path=path,
            callback=self.on_keep_user_stream,
            params=params,
            data=data,
            on_error=self.on_keep_user_stream_error
        )

    def on_set_leverage(self, data: dict, request: Request) -> None:
        """ 可以直接选择pass的函数，目前调试中暂时有写日志操作 """
        symbol = data["symbol"]
        leverage = data["leverage"]
        msg = f"{symbol}杠杆调为：{leverage}"
        self.gateway.write_log(msg)

    def on_query_time(self, data: dict, request: Request) -> None:
        """时间查询回报"""
        self.requery_count = 0  # 查询成功时重置查询服务器时间次数
        local_time: int = int(time.time() * 1000)
        server_time: int = int(data["serverTime"])
        self.time_offset: int = local_time - server_time

    def on_query_account(self, data: dict, request: Request) -> None:
        """资金查询回报"""
        for asset in data["assets"]:
            account: AccountData = AccountData(
                accountid=asset["asset"],  # 资产
                balance=float(asset["walletBalance"]),  # 余额
                frozen=float(asset["maintMargin"]),  # 维持保证金
                unpro=float(asset["unrealizedProfit"]),  # 未实现盈亏
                mbalance=float(asset["marginBalance"]),  # 保证金余额
                abalance=float(asset["availableBalance"]),  # 可用下单余额
                gateway_name=self.gateway_name
            )

            if account.balance:
                self.gateway.on_account(account)

        # self.gateway.write_log("账户资金查询成功")

    def on_query_position(self, data: dict, request: Request) -> None:
        """持仓查询回报"""
        for d in data:
            position: PositionData = PositionData(
                symbol=d["symbol"],
                exchange=Exchange.BINANCE,
                direction=Direction.NET,
                volume=float(d["positionAmt"]),
                price=float(d["entryPrice"]),
                pnl=float(d["unRealizedProfit"]),
                gateway_name=self.gateway_name,
            )

            if position.volume:
                volume = d["positionAmt"]
                if '.' in volume:
                    position.volume = float(d["positionAmt"])
                else:
                    position.volume = int(d["positionAmt"])

                self.gateway.on_position(position)

        self.gateway.write_log("持仓信息查询成功")

    def on_query_order(self, data: dict, request: Request) -> None:
        """未成交委托查询回报"""
        for d in data:
            key: Tuple[str, str] = (d["type"], d["timeInForce"])
            order_type: OrderType = ORDERTYPE_BINANCES2VT.get(key, None)
            # 过滤不支持类型的委托
            if not order_type:
                continue

            order: OrderData = OrderData(
                orderid=d["clientOrderId"],
                symbol=d["symbol"],
                exchange=Exchange.BINANCE,
                price=float(d["price"]),
                volume=float(d["origQty"]),
                type=order_type,
                direction=DIRECTION_BINANCES2VT[d["side"]],
                traded=float(d["executedQty"]),
                status=STATUS_BINANCES2VT.get(d["status"], None),
                datetime=generate_datetime(d["time"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_order(order)

        self.gateway.write_log("委托信息查询成功")

    def on_query_contract(self, data: dict, request: Request) -> None:
        """合约信息查询回报"""
        for d in data["symbols"]:
            base_currency: str = d["baseAsset"]
            quote_currency: str = d["quoteAsset"]
            name: str = f"{base_currency.upper()}/{quote_currency.upper()}"

            pricetick: int = 1
            min_volume: int = 1
            min_notional: int = 10
            max_limit_volume: int = 1000  # 暂用较大值补全缺省值

            for f in d["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    pricetick = float(f["tickSize"])
                elif f["filterType"] == "LOT_SIZE":
                    min_volume = float(f["minQty"])           # 也可以用float(f["stepSize"])
                    max_limit_volume = float(f["maxQty"])     # 限价单单次最大下单volume
                elif f["filterType"] == "MIN_NOTIONAL":
                    min_notional = float(f["notional"])

            contract: ContractData = ContractData(
                symbol=d["symbol"],
                exchange=Exchange.BINANCE,
                name=name,
                product=Product.FUTURES,
                size=1,
                pricetick=pricetick,
                max_limit_volume=max_limit_volume,
                min_notional=min_notional,
                min_volume=min_volume,
                stop_supported=False,
                net_position=True,
                history_data=True,
                gateway_name=self.gateway_name,
            )
            self.gateway.on_contract(contract)

            symbol_contract_map[contract.symbol] = contract

        self.gateway.write_log("合约信息查询成功")

        if self.leverage >= 1:
            self.gateway.write_log("开始调整杠杆")
            c = 0
            for i in symbol_contract_map.keys():
                # python aio 默认并行连接上限为100
                self.set_leverage(i, self.leverage)
                c += 1
                if c >= 80:
                    time.sleep(3)
                    c = 0
            self.gateway.write_log("杠杆调整完成")

    def on_send_order(self, data: dict, request: Request) -> None:
        """委托下单回报"""
        pass

    def on_send_order_failed(self, status_code: str, request: Request) -> None:
        """委托下单失败服务器报错回报"""
        order: OrderData = request.extra
        msg: str = f"{order.symbol}委托失败，订单详情{order}，" \
                   f"状态码：{status_code}，信息：{request.response.text}, " \
                   f"request:{request.response.__dict__}"
        self.gateway.write_log(msg)

        order.status = Status.REJECTED
        self.gateway.on_order(order)

    def on_send_order_error(
            self, exception_type: type, exception_value: Exception, tb, request: Request
    ) -> None:
        """委托下单回报函数报错回报"""
        order: OrderData = request.extra
        msg: str = f"{order.symbol}委托错误，订单详情{order}，request:{request.response.__dict__}"
        self.gateway.write_log(msg)

        order.status = Status.REJECTED
        self.gateway.on_order(order)

        if not issubclass(exception_type, (ConnectionError, SSLError)):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_order(self, data: dict, request: Request) -> None:
        """委托撤单回报"""
        pass

    def on_cancel_failed(self, status_code: str, request: Request) -> None:
        """
        tick级别撤单和trade事件是异步的，在tick发撤单后，trade已成交但未接收trade事件时，会出该报错
        有可能发出撤单指令后，交易ws就断了，此时可能会卡在等待撤单指令的返回，订单状态会一直显示提交中，而不会被拒单
        找不到委托一般是，收到trade事件的时间，晚于了发起撤单请求时间，很多时候在撤单的瞬间成交了
        """
        if request.extra:
            order: OrderData = request.extra
            if order.status != Status.ALLTRADED:      # 只有当order状态不是全部成交状态时，才需要推送拒单
                # 如果订单状态处于提交中的话，那么可能就是交易ws断了，没有收到交易所返回的order事件
                if order.status != Status.REJECTED:   # 包括提交中，未成交，部分成交，已撤销，已过期
                    msg = f"{order.symbol}撤单失败，{order.vt_orderid}，订单状态{order.status}，" \
                          f"状态码：{status_code}，信息：{request.response.text}，request:{request.response.__dict__}"
                    order.status = Status.DISCONNECT  # 设置为交易ws断开状态，是检查是否确实断开了
                    self.gateway.on_order(order)
                else:  # 返回状态本身就是拒单的情况应该不存在，BN没有拒单这个订单状态
                    msg = f"{order.symbol}撤单失败，{order.vt_orderid}，推送的状态就是拒单，" \
                          f"状态码：{status_code}，信息：{request.response.text}，request:{request.response.__dict__}"
                    self.gateway.on_order(order)

                self.gateway.write_log(msg)
        else:
            msg = f"撤单失败，状态码：{status_code}，信息：{request.response.text}，request:{request.response.__dict__}"
            self.gateway.write_log(msg)

    def on_start_user_stream(self, data: dict, request: Request) -> None:
        """
        生成listenKey回报，BN返回的就是listenKey的dict
        """
        self.restart_count = 0
        self.user_stream_key = data["listenKey"]
        self.gateway.write_log(f"新申请的listenkey:{self.user_stream_key}")
        self.keep_alive_count = 0

        if self.server == "REAL":
            self.url = F_WEBSOCKET_TRADE_HOST + self.user_stream_key
        else:
            self.url = F_TESTNET_WEBSOCKET_TRADE_HOST + self.user_stream_key

        # 重连如果连接上限超过100次的话，便不会再重连了，是异步协程的默认限制
        self.trade_ws_api.connect(self.url, self.proxy_host, self.proxy_port)

    def on_keep_user_stream(self, data: dict, request: Request) -> None:
        """
        延长交易ws listenKey有效期回报，延长listenKey时，BN返回{}
        """
        pass

    def on_keep_user_stream_error(
            self, exception_type: type, exception_value: Exception, tb, request: Request
    ) -> None:
        """
        延长交易ws listenKey有效期函数报错回报
        """
        self.gateway.write_log(f"延长交易ws失败,request:{request.response.__dict__}")
        if not issubclass(exception_type, TimeoutError):
            self.gateway.write_log(f"on_keep_user_stream_error, request:{request.response.__dict__}")
            self.on_error(exception_type, exception_value, tb, request)

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        history: List[BarData] = []
        limit: int = 1000

        start_time: int = int(datetime.timestamp(req.start))

        while True:
            # 创建查询参数
            params: dict = {
                "symbol": req.symbol,
                "interval": INTERVAL_VT2BINANCES[req.interval],
                "limit": limit
            }

            params["startTime"] = start_time * 1000
            path: str = "/fapi/v1/klines"
            if req.end:
                end_time = int(datetime.timestamp(req.end))
                params["endTime"] = end_time * 1000  # 转换成毫秒

            resp: Response = self.request(
                "GET",
                path=path,
                data={"security": Security.NONE},
                params=params
            )

            # 如果请求失败则终止循环
            if resp.status_code // 100 != 2:
                msg: str = f"获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data: dict = resp.json()
                if not data:
                    msg: str = f"获取历史数据为空，开始时间：{start_time}"
                    self.gateway.write_log(msg)
                    break

                buf: List[BarData] = []

                for row in data:
                    bar: BarData = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=generate_datetime(row[0]),
                        interval=req.interval,
                        volume=float(row[5]),
                        turnover=float(row[7]),
                        open_price=float(row[1]),
                        high_price=float(row[2]),
                        low_price=float(row[3]),
                        close_price=float(row[4]),
                        gateway_name=self.gateway_name
                    )
                    buf.append(bar)

                begin: datetime = buf[0].datetime
                end: datetime = buf[-1].datetime

                history.extend(buf)
                msg: str = f"获取历史数据成功，{req.symbol} - {req.interval.value}，{begin} - {end}"
                self.gateway.write_log(msg)

                # 如果收到了最后一批数据则终止循环
                if len(data) < limit:
                    break

                # 更新开始时间
                start_dt = bar.datetime + TIMEDELTA_MAP[req.interval]
                start_time = int(datetime.timestamp(start_dt))

        return history


class BinanceUsdtTradeWebsocketApi(WebsocketClient):
    """币安正向合约的交易Websocket API"""

    def __init__(self, gateway: BinanceUsdtGateway) -> None:
        """构造函数"""
        super().__init__()
        self.gateway: BinanceUsdtGateway = gateway
        self.gateway_name: str = gateway.gateway_name

    def connect(self, url: str, proxy_host: str, proxy_port: int) -> None:
        """连接交易Websocket频道"""
        self.init(url, proxy_host, proxy_port)
        self.start()

    def on_connected(self) -> None:
        """连接成功回报"""
        self.gateway.write_log("交易Websocket API连接成功")

    def on_disconnected(self) -> None:
        """调用disconnect函数后，如果主协程循环正常的话，会在跳出当前协程循环时，调用该函数重连"""
        self.gateway.write_log("交易Websocket API断开")
        self.gateway.write_log(f"断开时的listenKey：{self.gateway.rest_api.user_stream_key}")
        self.gateway.rest_api.start_user_stream()

    def disconnect(self) ->None:
        """"主动断开交易Websocket链接，防止重连着多个交易ws"""
        self._active = False

        if self._ws:
            coro = self._ws.close()
            run_coroutine_threadsafe(coro, self._loop)

    def on_packet(self, packet: dict) -> None:
        """推送数据回报"""
        if packet["e"] == "ACCOUNT_UPDATE":
            self.on_account(packet)
        elif packet["e"] == "ORDER_TRADE_UPDATE":
            self.on_order(packet)
        elif packet["e"] == "listenKeyExpired":
            self.gateway.write_log(f"listenKey到期了，{packet}")
            self.disconnect()  # 过期时仅断开交易ws，断开后会调用on_disconnected重连的

    def on_account(self, packet: dict) -> None:
        """资金更新推送"""
        for acc_data in packet["a"]["B"]:
            account: AccountData = AccountData(
                accountid=acc_data["a"],
                balance=float(acc_data["wb"]),
                frozen=float(acc_data["wb"]) - float(acc_data["cw"]),
                gateway_name=self.gateway_name
            )

            if account.balance:
                self.gateway.on_account(account)

        for pos_data in packet["a"]["P"]:
            if pos_data["ps"] == "BOTH":
                volume = pos_data["pa"]
                if '.' in volume:
                    volume = float(volume)
                else:
                    volume = int(volume)

                position: PositionData = PositionData(
                    symbol=pos_data["s"],
                    exchange=Exchange.BINANCE,
                    direction=Direction.NET,
                    volume=volume,
                    price=float(pos_data["ep"]),
                    pnl=float(pos_data["cr"]),
                    gateway_name=self.gateway_name,
                )
                self.gateway.on_position(position)

    def on_order(self, packet: dict) -> None:
        """
        委托更新推送，如果网络问题部分的on_order没有返回，则确实会导致pos与实际下单不一致了
        """
        # 过滤不支持类型的委托
        ord_data: dict = packet["o"]
        key: Tuple[str, str] = (ord_data["o"], ord_data["f"])
        order_type: OrderType = ORDERTYPE_BINANCES2VT.get(key, None)
        if not order_type:
            return

        order_offset = Offset.NONE
        if 'Open' in str(ord_data["c"]):
            order_offset = Offset.OPEN
        elif 'Close' in str(ord_data["c"]):
            order_offset = Offset.CLOSE

        order: OrderData = OrderData(
            symbol=ord_data["s"],
            exchange=Exchange.BINANCE,
            orderid=str(ord_data["c"]),
            type=order_type,
            direction=DIRECTION_BINANCES2VT[ord_data["S"]],
            offset=order_offset,
            price=float(ord_data["p"]),
            volume=float(ord_data["q"]),
            traded=float(ord_data["z"]),
            status=STATUS_BINANCES2VT[ord_data["X"]],
            datetime=generate_datetime(packet["E"]),
            gateway_name=self.gateway_name
        )

        self.gateway.on_order(order)

        # 将成交数量四舍五入到正确精度
        trade_volume: float = float(ord_data["l"])
        contract: ContractData = symbol_contract_map.get(order.symbol, None)
        if contract:
            trade_volume = round_to(trade_volume, contract.min_volume)

        if not trade_volume:
            return

        trade: TradeData = TradeData(
            symbol=order.symbol,
            exchange=order.exchange,
            orderid=order.orderid,
            tradeid=ord_data["t"],
            direction=order.direction,
            offset=order_offset,
            price=float(ord_data["L"]),
            volume=trade_volume,
            datetime=generate_datetime(ord_data["T"]),
            gateway_name=self.gateway_name,
        )
        self.gateway.on_trade(trade)


class BinanceUsdtDataWebsocketApi(WebsocketClient):
    """币安正向合约的行情Websocket API"""

    def __init__(self, gateway: BinanceUsdtGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: BinanceUsdtGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.ticks: Dict[str, TickData] = {}
        self.reqid: int = 0

    def connect(
            self,
            proxy_host: str,
            proxy_port: int,
            server: str
    ) -> None:
        """连接行情Websocket频道"""
        if server == "REAL":
            self.init(F_WEBSOCKET_DATA_HOST, proxy_host, proxy_port)
        else:
            self.init(F_TESTNET_WEBSOCKET_DATA_HOST, proxy_host, proxy_port)

        self.start()

    def on_connected(self) -> None:
        """连接成功回报"""
        self.gateway.write_log("行情Websocket API连接成功")

        if self.ticks:
            channels = []
            for symbol in self.ticks.keys():
                channels.append(f"{symbol}@ticker")
                channels.append(f"{symbol}@depth5")

            # 新建/重新订阅行情ws
            req: dict = {
                "method": "SUBSCRIBE",
                "params": channels,
                "id": self.reqid
            }
            self.send_packet(req)

    def on_disconnected(self) -> None:
        """
        考虑是否在这里进行行情ws的重连
        """
        self.gateway.write_log("行情Websocket API断开")

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        if req.symbol in self.ticks:
            return

        if req.symbol not in symbol_contract_map:
            self.gateway.write_log(f"找不到该合约代码{req.symbol}")
            return

        self.reqid += 1

        # 创建TICK对象
        tick: TickData = TickData(
            symbol=req.symbol,
            name=symbol_contract_map[req.symbol].name,
            exchange=Exchange.BINANCE,
            datetime=datetime.now(CHINA_TZ),
            gateway_name=self.gateway_name,
        )
        self.ticks[req.symbol.lower()] = tick

        channels = [
            f"{req.symbol.lower()}@ticker",
            f"{req.symbol.lower()}@depth5"
        ]

        req: dict = {
            "method": "SUBSCRIBE",
            "params": channels,
            "id": self.reqid
        }
        self.send_packet(req)

    def on_packet(self, packet: dict) -> None:
        """推送数据回报"""
        stream: str = packet.get("stream", None)

        if not stream:
            return

        data: dict = packet["data"]

        symbol, channel = stream.split("@")
        tick: TickData = self.ticks[symbol]

        if channel == "ticker":
            tick.volume = float(data['v'])
            tick.turnover = float(data['q'])
            tick.open_price = float(data['o'])
            tick.high_price = float(data['h'])
            tick.low_price = float(data['l'])
            tick.last_price = float(data['c'])
            tick.datetime = generate_datetime(float(data['E']))
        else:
            bids: list = data["b"]
            for n in range(min(5, len(bids))):
                price, volume = bids[n]
                tick.__setattr__("bid_price_" + str(n + 1), float(price))
                tick.__setattr__("bid_volume_" + str(n + 1), float(volume))

            asks: list = data["a"]
            for n in range(min(5, len(asks))):
                price, volume = asks[n]
                tick.__setattr__("ask_price_" + str(n + 1), float(price))
                tick.__setattr__("ask_volume_" + str(n + 1), float(volume))

        if tick.last_price:
            tick.localtime = datetime.now()
            self.gateway.on_tick(copy(tick))


def generate_datetime(timestamp: float) -> datetime:
    """生成时间"""
    dt: datetime = datetime.fromtimestamp(timestamp / 1000)
    dt: datetime = CHINA_TZ.localize(dt)
    return dt
