import asyncio
import json
import os
from typing import Any
from urllib.parse import urlparse

import httpx
from httpx import AsyncClient, Response

from .accounts_pool import Account, AccountsPool
from .logger import logger
from .utils import utc
from .xclid import XClIdGen
from pkg.XClientTransaction.x_client_transaction.utils import USER_AGENTS
import random
from pkg.redis.client import RedisClient

ReqParams = dict[str, str | int] | None
TMP_TS = utc.now().isoformat().split(".")[0].replace("T", "_").replace(":", "-")[0:16]


class HandledError(Exception): ...


class AbortReqError(Exception): ...


class XClIdGenStore:
    items: dict[str, XClIdGen] = {}  # username -> XClIdGen

    @classmethod
    async def get(cls, username: str, fresh=False) -> XClIdGen:
        if username in cls.items and not fresh:
            return cls.items[username]

        tries = 0
        while tries < 3:
            try:
                clid_gen = await XClIdGen.create()
                cls.items[username] = clid_gen
                return clid_gen
            except httpx.HTTPStatusError:
                tries += 1
                await asyncio.sleep(1)

        raise AbortReqError(
            "Faield to create XClIdGen. See: https://github.com/vladkens/twscrape/issues/248"
        )


class Ctx:
    def __init__(self, acc: Account, clt: AsyncClient):
        self.req_count = 0
        self.acc = acc
        self.clt = clt

    async def aclose(self):
        await self.clt.aclose()

    async def req(self, method: str, url: str, params: ReqParams = None) -> Response:
        # if code 404 on first try then generate new x-client-transaction-id and retry
        # https://github.com/vladkens/twscrape/issues/248
        path = urlparse(url).path or "/"

        tries = 0
        while tries < 3:
            gen = await XClIdGenStore.get(self.acc.username, fresh=tries > 0)
            hdr = {"x-client-transaction-id": gen.calc(method, path)}
            rep = await self.clt.request(method, url, params=params, headers=hdr)
            if rep.status_code != 404:
                return rep

            tries += 1
            logger.debug(f"Retrying request with new x-client-transaction-id: {url}")
            await asyncio.sleep(1)

        raise AbortReqError(
            "Faield to get XClIdGen. See: https://github.com/vladkens/twscrape/issues/248"
        )


def req_id(rep: Response):
    lr = str(rep.headers.get("x-rate-limit-remaining", -1))
    ll = str(rep.headers.get("x-rate-limit-limit", -1))
    sz = max(len(lr), len(ll))
    lr, ll = lr.rjust(sz), ll.rjust(sz)

    username = getattr(rep, "__username", "<UNKNOWN>")
    return f"{lr}/{ll} - {username}"


def dump_rep(rep: Response):
    count = getattr(dump_rep, "__count", -1) + 1
    setattr(dump_rep, "__count", count)

    acc = getattr(rep, "__username", "<unknown>")
    outfile = f"{count:05d}_{rep.status_code}_{acc}.txt"
    outfile = f"/tmp/twscrape-{TMP_TS}/{outfile}"
    os.makedirs(os.path.dirname(outfile), exist_ok=True)

    msg = []
    msg.append(f"{count:,d} - {req_id(rep)}")
    msg.append(f"{rep.status_code} {rep.request.method} {rep.request.url}")
    msg.append("\n")
    # msg.append("\n".join([str(x) for x in list(rep.request.headers.items())]))
    msg.append("\n".join([str(x) for x in list(rep.headers.items())]))
    msg.append("\n")

    try:
        msg.append(json.dumps(rep.json(), indent=2))
    except json.JSONDecodeError:
        msg.append(rep.text)

    txt = "\n".join(msg)
    with open(outfile, "w") as f:
        f.write(txt)


class QueueClient:
    def __init__(self, pool: AccountsPool | None, queue: str, debug=False, proxy: str | None = None, redis_client: RedisClient = None):
        self.pool = pool
        self.queue = queue
        self.debug = debug
        self.ctx: Ctx | None = None
        self.proxy = proxy
        self.redis_client = redis_client

    async def __aenter__(self):
        if self.pool is None:
            await self._get_ctx_no_account()
        else:
            await self._get_ctx()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._close_ctx()

    async def _close_ctx(self, reset_at=-1, inactive=False, msg: str | None = None):
        if self.ctx is None:
            return

        if self.pool is None:
            return

        ctx, self.ctx, self.req_count = self.ctx, None, 0
        username = ctx.acc.username
        await ctx.aclose()

        if inactive:
            await self.pool.mark_inactive(username, msg)
            return

        if reset_at > 0:
            await self.pool.lock_until(ctx.acc.username, self.queue, reset_at, ctx.req_count)
            return

        await self.pool.unlock(ctx.acc.username, self.queue, ctx.req_count)

    async def _get_ctx(self):
        if self.ctx:
            return self.ctx

        acc = await self.pool.get_for_queue_or_wait(self.queue)
        if acc is None:
            return None

        clt = acc.make_client(proxy=self.proxy)
        self.ctx = Ctx(acc, clt)
        return self.ctx

    async def _get_ctx_no_account(self):
        """
        Get context without account, used for guest requests
        """
        if self.ctx:
            return self.ctx

        cks = {
            'kdt': 'oUO2P7981NBLWqsveKUGB00j35leLN7Rte3U2E5J',
            'dnt': '1',
            'guest_id': 'v1:175043670643918763',
            'guest_id_marketing': 'v1:175043670643918763',
            'guest_id_ads': 'v1:175043670643918763',
            'att': '1-XDQRxUxS00pnBZ1c8nUuzpz6hPdUpbsg2kO7LA7z',
            'gt': '1936991488006902098',
            '__cf_bm': 'MBuGBk6DAQPGy4eluqKqUohHklWSoCDqxv2xFPa3rQQ-1750651157-1.0.1.1-xcgl4sX1fnHD7eoIEqLJqceV01LMdtjTJpRYBY.pjwOcxrJPWIs9E4ukew72WAKOFssnrcVM30skdAk_Vh6.JJ6hadCkKH5gkKB5aHSoBjc',
            'personalization_id': '"v1_TZ3gbehfb2YV6Md3KqsnhA=="'
        }
        acc = Account(
            username="guest-user",
            password="guest-user",
            email="guest-user",
            email_password="guest-user",
            user_agent= random.choice(USER_AGENTS),
            active=True,
            cookies=cks
        )

        clt = acc.make_client(proxy=self.proxy)

        if self.redis_client is None:
            raise ValueError("Redis client is required for guest requests")

        # get x-guest-token
        redis_key = "x-crawler:x_guest_token"
        token = self.redis_client.get(redis_key)
        if not token:
            # get x-guest-token
            rep = await clt.post("https://api.x.com/1.1/guest/activate.json")
            rep.raise_for_status()
            guest_token = rep.json()["guest_token"]
            self.redis_client.set(redis_key, guest_token, 60)  # 1 minute
            token = guest_token

        clt.headers.update({
            "x-guest-token": str(token),
        })
        self.ctx = Ctx(acc,clt)
        return self.ctx

    async def _check_rep(self, rep: Response) -> None:
        """
        This function can raise Exception and request will be retried or aborted
        Or if None is returned, response will passed to api parser as is
        """

        if self.debug:
            dump_rep(rep)

        try:
            res = rep.json()
        except json.JSONDecodeError:
            res: Any = {"_raw": rep.text}


        limit_remaining = int(rep.headers.get("x-rate-limit-remaining", -1))
        limit_reset = int(rep.headers.get("x-rate-limit-reset", -1))
        # limit_max = int(rep.headers.get("x-rate-limit-limit", -1))

        err_msg = "OK"
        if "errors" in res:
            err_msg = set([f"({x.get('code', -1)}) {x['message']}" for x in res["errors"]])
            err_msg = "; ".join(list(err_msg))

        log_msg = f"{rep.status_code:3d} - {req_id(rep)} - {err_msg}"
        logger.trace(log_msg)

        # for dev: need to add some features in api.py
        if err_msg.startswith("(336) The following features cannot be null"):
            logger.error(f"[DEV] Update required: {err_msg}")
            exit(1)

        # general api rate limit
        if limit_remaining == 0 and limit_reset > 0:
            logger.debug(f"Rate limited: {log_msg}")
            await self._close_ctx(limit_reset)
            raise HandledError()

        # no way to check is account banned in direct way, but this check should work
        if err_msg.startswith("(88) Rate limit exceeded") and limit_remaining > 0:
            logger.warning(f"Ban detected: {log_msg}")
            await self._close_ctx(-1, inactive=True, msg=err_msg)
            raise HandledError()

        if err_msg.startswith("(326) Authorization: Denied by access control"):
            logger.warning(f"Ban detected: {log_msg}")
            await self._close_ctx(-1, inactive=True, msg=err_msg)
            raise HandledError()

        if err_msg.startswith("(32) Could not authenticate you"):
            logger.warning(f"Session expired or banned: {log_msg}")
            await self._close_ctx(-1, inactive=True, msg=err_msg)
            raise HandledError()

        if err_msg == "OK" and rep.status_code == 403:
            logger.warning(f"Session expired or banned: {log_msg}")
            await self._close_ctx(-1, inactive=True, msg=None)
            raise HandledError()

        # something from twitter side - abort all queries, see: https://github.com/vladkens/twscrape/pull/80
        if err_msg.startswith("(131) Dependency: Internal error"):
            # looks like when data exists, we can ignore this error
            # https://github.com/vladkens/twscrape/issues/166
            if rep.status_code == 200 and "data" in res and "user" in res["data"]:
                err_msg = "OK"
            else:
                logger.warning(f"Dependency error (request skipped): {err_msg}")
                raise AbortReqError()

        # content not found
        if rep.status_code == 200 and "_Missing: No status found with that ID" in err_msg:
            return  # ignore this error

        # something from twitter side - just ignore it, see: https://github.com/vladkens/twscrape/pull/95
        if rep.status_code == 200 and "Authorization" in err_msg:
            logger.warning(f"Authorization unknown error: {log_msg}")
            return

        if err_msg != "OK":
            logger.warning(f"API unknown error: {log_msg}")
            return  # ignore any other unknown errors

        try:
            rep.raise_for_status()
        except httpx.HTTPStatusError:
            logger.error(f"Unhandled API response code: {log_msg}")
            await self._close_ctx(utc.ts() + 60 * 15)  # 15 minutes
            raise HandledError()

    async def get(self, url: str, params: ReqParams = None) -> Response | None:
        return await self.req("GET", url, params=params)

    async def post_form(self, url: str, params: ReqParams = None) -> Response | None:
        self.ctx.clt.headers.update(
            {
                "content-type": "application/x-www-form-urlencoded"
            }
        )
        return await self.req("POST", url, params=params)

    async def req(self, method: str, url: str, params: ReqParams = None) -> Response | None:
        unknown_retry, connection_retry = 0, 0

        while True:
            ctx = await self._get_ctx()  # not need to close client, class implements __aexit__
            if ctx is None:
                return None

            try:
                rep = await ctx.req(method, url, params=params)
                setattr(rep, "__username", ctx.acc.username)
                await self._check_rep(rep)

                ctx.req_count += 1  # count only successful
                unknown_retry, connection_retry = 0, 0
                return rep
            except AbortReqError:
                # abort all queries
                return
            except HandledError:
                # retry with new account
                continue
            except (httpx.ReadTimeout, httpx.ProxyError):
                # http transport failed, just retry with same account
                continue
            except (httpx.ConnectError, httpx.ConnectTimeout) as e:
                # if proxy missconfigured or ???
                connection_retry += 1
                if connection_retry >= 3:
                    raise e
            except Exception as e:
                unknown_retry += 1
                if unknown_retry >= 3:
                    msg = [
                        "Unknown error. Account timeouted for 15 minutes.",
                        "Create issue please: https://github.com/vladkens/twscrape/issues",
                        f"If it mistake, you can unlock accounts with `twscrape reset_locks`. Err: {type(e)}: {e}",
                    ]

                    logger.warning(" ".join(msg))
                    await self._close_ctx(utc.ts() + 60 * 15)  # 15 minutes

    async def get_with_no_account(self, url: str, params: ReqParams = None, token:str="") -> Response | None:
        return await self.req_with_no_account("GET", url, params=params)

    async def req_with_no_account(self, method: str, url: str, params: ReqParams = None) -> Response | None:
        unknown_retry, connection_retry = 0, 0

        while True:
            ctx = await self._get_ctx_no_account()  # not need to close client, class implements __aexit__
            if ctx is None:
                return None

            try:
                rep = await ctx.req(method, url, params=params)
                setattr(rep, "__username", ctx.acc.username)
                await self._check_rep(rep)

                ctx.req_count += 1  # count only successful
                unknown_retry, connection_retry = 0, 0
                return rep
            except AbortReqError:
                # abort all queries
                return
            except HandledError:
                # retry with new account
                continue
            except (httpx.ReadTimeout, httpx.ProxyError):
                # http transport failed, just retry with same account
                continue
            except (httpx.ConnectError, httpx.ConnectTimeout) as e:
                # if proxy missconfigured or ???
                connection_retry += 1
                if connection_retry >= 3:
                    raise e
            except Exception as e:
                unknown_retry += 1
                if unknown_retry >= 3:
                    logger.error(f"Unknown error,  Err: {type(e)}: {e}")
                    return None