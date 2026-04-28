import atexit
import smtplib
from email.mime.text import MIMEText
import os
import ipaddress
from random import choice, randint
import traceback
from requests_ip_rotator import ApiGateway as BaseApiGateway
import requests
import json
from typing import Callable, Any, Dict, Tuple, TypeVar, List, Optional, Coroutine, Awaitable
from typing import ParamSpec, cast
import inspect
import asyncio
from functools import partial, wraps
import time
import logging
from dotenv import load_dotenv

load_dotenv("/workspace/env")

__all__ = [
    "async_wrapper",
    "safe_get",
    "safe_get_list",
    "list_wrapper",
    "timing_wrapper",
    "wrapper_json_dumps",
    "cleanup",
    "safe_run",
    "notify_email",
    "start_session",
    "GraphQLApiGateway",
]

T = TypeVar("T")
R = TypeVar("R")
P = ParamSpec("P")


def list_wrapper(func: Callable[[T], Optional[R]], *, skip_none: bool = True) -> Callable[[List[T]], List[R]]:
    def wrapped(objs: List[T], *args, **kwargs) -> List[R]:
        results = [func(obj, *args, **kwargs) for obj in objs]
        return [res for res in results if res is not None] if skip_none else results
    return wrapped


def async_wrapper(func: Callable[P, R]) -> Callable[..., Coroutine[Any, Any, List[R]]]:
    @wraps(func)
    async def inner(objs, *args: P.args, sem: asyncio.Semaphore, timing: bool = False, **kwargs: P.kwargs) -> list[R]:
        loop = asyncio.get_running_loop()

        async def run(obj):
            return await loop.run_in_executor(None, partial(func, obj, *args, **kwargs))

        if timing:
            start_all = time.time()

            async def timed_task(obj):
                async with sem:
                    t0 = time.time()
                    result = await run(obj)
                    dur = time.time() - t0
                return result, dur

            results_and_durations = await asyncio.gather(*(timed_task(obj) for obj in objs))
            wall = time.time() - start_all
            results, durs = zip(*results_and_durations) if results_and_durations else ([], [])
            total_ind = sum(durs)
            avg_conc = total_ind / wall if wall > 0 else float("inf")

            logging.error(
                f"{func.__name__}: {len(results)} calls in {wall:.2f}s "
                f"(sum {total_ind:.2f}s) → avg conc {avg_conc:.2f}"
            )
            return list(results)
        else:
            async def sem_task(obj):
                async with sem:
                    return await run(obj)

            return await asyncio.gather(*[sem_task(obj) for obj in objs])

    return cast(Callable[..., Coroutine[Any, Any, List[R]]], inner)


def safe_get(d: dict, path: list, log_prefix="parse_data") -> Any:
    current = d
    for key in path:
        if isinstance(current, dict):
            if key not in current:
                logging.error(f"[{log_prefix}] Missing key in dict: '{key}'", extra={"data": str(current)})
                return None
            current = current[key]
        else:
            logging.error(f"[{log_prefix}] Expected dict but got {type(current)} at key '{key}'", extra={"data": str(current)})
            return None
    return current


def safe_get_list(buckets: List[Dict], mapping: Dict[str, list], log_prefix="parse_data_fields", soft_errors=True) -> List[Dict[str, Any]]:
    final_list = []
    if buckets:
        for count, bucket in enumerate(buckets):
            entry = {}

            current = bucket
            for key, f_path in mapping.items():
                rem_path = f_path.copy()
                for path in f_path:
                    if not isinstance(current, dict):
                        if not soft_errors:
                            logging.error(f"[{log_prefix}] At index {count}, expected dict but got {type(current)} at key '{key}'", extra={"data": str(current)})
                            current = None
                        break
                    if path not in current:
                        if not soft_errors:
                            logging.error(f"[{log_prefix}] At index {count}, missing key '{key}'", extra={"data": str(current)})
                            current = None
                        break

                    if isinstance(current[path], list):
                        rem_path.pop(0)
                        intermediate = safe_get_list(current[path], rem_path[0], log_prefix="parse_nested_data_fields")

                        current = {}
                        for dict_ind in intermediate:
                            for dict_key, dict_value in dict_ind.items():
                                if dict_key not in current:
                                    current[dict_key] = [dict_value]
                                else:
                                    current[dict_key].extend([dict_value])

                        current = str(current)
                        break

                    else:
                        current = current[path]
                        rem_path.pop(0)

                entry[key] = current

                current = bucket
            final_list.append(entry)
    else:
        return []
    return final_list


def start_session(aws_access_key, aws_secret_key, constructor, url_base='https://apis.justwatch.com/graphql') -> requests.Session:
    gw = constructor(url_base, access_key_id=aws_access_key, access_key_secret=aws_secret_key)
    gw.start()
    session = requests.Session()
    session.mount(url_base, gw)
    return session


MAX_IPV4 = ipaddress.IPv4Address._ALL_ONES


class GraphQLApiGateway(BaseApiGateway):
    def send(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        endpoint = choice(self.endpoints)

        # Only hit /ProxyStage, not /ProxyStage/graphql
        request.url = f"https://{endpoint}/"

        request.headers["Host"] = endpoint

        x_forwarded_for = request.headers.get("X-Forwarded-For")
        if x_forwarded_for is None:
            x_forwarded_for = ipaddress.IPv4Address._string_from_ip_int(randint(0, MAX_IPV4))

        request.headers.pop("X-Forwarded-For", None)
        request.headers["X-My-X-Forwarded-For"] = x_forwarded_for

        return super().send(request, stream, timeout, verify, cert, proxies)


def wrapper_json_dumps(path: str, path_params: str, verbose: bool = False):
    def decorator(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            result = fn(*args, **kwargs)
            if verbose:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(result.json(), f, indent=2, ensure_ascii=False)
                with open(path_params, "w", encoding="utf-8") as f:
                    json.dump(str(args), f, indent=2, ensure_ascii=False)
            return result
        return wrapped
    return decorator


def timing_wrapper(fn: Callable[P, R]) -> Callable[P, Tuple[R, float]]:
    @wraps(fn)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> Tuple[R, float]:
        start_time = time.time()
        result = fn(*args, **kwargs)
        elapsed_time = time.time() - start_time
        logging.error(f"Function {fn.__name__} took {elapsed_time:.2f} seconds to execute.")
        return result
    return wrapped


def shutdown_pod(pod_id: str):
    url = f"https://rest.runpod.io/v1/pods/{pod_id}"
    headers = {"Authorization": "Bearer " + str(os.getenv("RUNPOD_POD_SHUTDOWN_KEY"))}
    r = requests.delete(url, headers=headers)
    r.raise_for_status()


def cleanup(shutdown: bool = False, notify: bool = True, msg: str = "Cleanup executed."):
    if shutdown:
        pod_id = os.getenv("RUNPOD_POD_ID")
        if pod_id:
            shutdown_pod(pod_id)

    if notify:
        notify_email(msg)
        print("Notification email sent.")


def notify_email(msg: str):
    email = "juriskjd@gmail.com"
    target = "juliusjreinhold@outlook.com"
    app_pw = os.environ["GMAIL_APP_PW"]

    m = MIMEText(msg)
    m["Subject"] = "RunPod Shutdown Notice"
    m["From"] = email
    m["To"] = target

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
        s.login(email, app_pw)
        s.send_message(m)


def safe_run(cleanup_fn, shutdown_after=True):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            error_msg = []

            try:
                result = func(*args, **kwargs)
                if asyncio.iscoroutine(result):
                    asyncio.run(result)

            except Exception:
                tb = traceback.format_exc()
                error_msg.append("Task failed with error:\n" + tb)
                raise

            else:
                error_msg.append("Task completed successfully.")

            finally:
                atexit.register(
                    cleanup_fn,
                    shutdown=shutdown_after,
                    notify=True,
                    msg="Script exited.\n" + "\n".join(error_msg)
                )

        return wrapper

    return decorator
