import copy
from typing import List, Any, Tuple, Union, Callable
from random import random, sample
import pathlib
import os
import sys
from os import listdir
from os.path import isfile
import shutil
import pickle
from itertools import chain
import json
from enum import Enum
from shutil import rmtree
from google.cloud import bigquery
from decimal import Decimal
import subprocess
import pendulum
import requests as http
from hashlib import sha256
import uuid
import datetime
from datetime import timedelta
from faker import Faker
from bson import ObjectId
from pymongo import MongoClient
from mongoengine.queryset.visitor import Q
from mongoengine.errors import ValidationError
from flask_restx import marshal
from math import log10, sqrt
from retry import retry
import pandas as pd
from common import OBJECT_ID_FIELDS

true = True
false = False
null = undefined = None

fake = Faker()

global webserver

_HERE_ = pathlib.Path().parent.resolve()
_OUTPUT_DIR_ = os.path.join(_HERE_, "server/outputs/")
_UTILS_DIR_ = os.path.join(_HERE_, "server/utils/")
_DATA_DIR_ = os.path.join(_HERE_, "server/data/")
_USER_DIR_ = os.path.join(_DATA_DIR_, "user")
_STATIC_DIR_ = os.path.join(_HERE_, "static/")

sys.path.append(_UTILS_DIR_)

from schemas import get_reduced_post_schema, get_profile_schema
from conf import *
from common import OBJECT_ID_FIELDS


def get_hash(obj: Any):
    obj = str(obj).encode()
    hasher = sha256()
    hasher.update(obj)
    return hasher.hexdigest()


def safely_divide(a, b):
    try:
        return a / b
    except ZeroDivisionError:
        return 0


def distribute_weights(weights_dict: dict) -> dict:
    weights = weights_dict.values()
    weight_classes = weights_dict.keys()
    total_weights = sum(weights)
    relative_weights = [safely_divide(weight, total_weights) for weight in weights]
    return dict(zip(weight_classes, relative_weights))


class ModelParameters(Enum):
    """Model parameters including weights"""

    USER_TYPES = ["USER", "CREATOR"]

    CONTENT_TYPES = [
        "humor",
        "política",
        "series",
        "fútbol",
        "criptomonedas",
        "comida",
        "nfts",
        "música",
        "juegos",
        "anime",
        "cine",
        "memes",
        "tecnología",
        "economía",
        "historia",
        "estilodevida",
        "opinión",
        "animalesymascotas",
        "arteydiseño",
        "actualidad",
    ]

    KEYWORD_MAPPING_EN_ES = {
        "humor": "humor",
        "politics": "política",
        "series": "series",
        "football": "fútbol",
        "cryptocurrencies": "criptomonedas",
        "food": "comida",
        "nfts": "nfts",
        "music": "música",
        "games": "juegos",
        "anime": "anime",
        "cinema": "cine",
        "memes": "memes",
        "tech": "tecnología",
        "economy": "economía",
        "history": "historia",
    }

    NAMES_MAPPING_EN_ES = {
        "humor": "humor",
        "movies_and_series": "cine",
        "cryptocurrencies": "criptomonedas",
        "food": "comida",
        "nfts": "nfts",
        "music": "música",
        "videogames": "juegos",
        "anime": "anime",
        "mems": "memes",
        "technology": "tecnología",
        "history": "historia",
        "finance": "economía",
        "lifestyle": "estilodevida",
        "opinion": "opinión",
        "pets": "animalesymascotas",
        "art_and_design": "arteydiseño",
        "news": "actualidad",
    }

    _KEYWORD_MAPPING = {**KEYWORD_MAPPING_EN_ES, **NAMES_MAPPING_EN_ES}

    _RAW_CREATOR_WEIGHTS = {
        "creator_post_count": 0.02275960171,
        "creator_post_engagements": 0.02418207681,
        "creator_post_earnings": 0.04125177809,
        "creator_follower_count": 0.02275960171,
        "creator_has_avatar": 0.0213371266,
        "creator_is_ambassador": 0.1,
        "creator_post_frequency": 0.1,
        "creator_profile_complete": 0.1,
    }

    _METRIC_WEIGHTS = distribute_weights(_RAW_CREATOR_WEIGHTS)

    METRIC_FIELDS = [field for field in _METRIC_WEIGHTS]

    METRIC_WEIGHTS = [weight for _, weight in _METRIC_WEIGHTS.items()]

    _RAW_POST_WEIGHTS = {
        "post_engagements": 0.02418207681,
        "post_earnings": 0.04125177809,
    }

    _POST_METRIC_WEIGHTS = distribute_weights(_RAW_POST_WEIGHTS)

    POST_METRIC_FIELDS = [field for field in _POST_METRIC_WEIGHTS]

    POST_METRIC_WEIGHTS = [weight for _, weight in _POST_METRIC_WEIGHTS.items()]

    NOMINAL_SHARED_INTEREST_POOL_SIZE = 500

    NOMINAL_SHARED_INTEREST_POST_LIMIT = 5000

    REBUILD_DATA_STORE = False

    CLEAR_DATA_OFFSET = 3000

    CLEAR_SUB_DATA_OFFSET = 1000

    CACHE_HYDRATION_OFFSET_SECONDS = 0

    WEB_CACHE_HYDRATION_OFFSET_SECONDS = 60

    NOMINAL_NUM_EPOCHS = 10

    POST_TIP_NOMINAL = 0.5

    STAGING_SAMPLE_SIZE = 100

    POST_STAGING_SAMPLE_SIZE = 500

    RESULT_CACHING_OFFSET = {"user": 5, "post": 5, "internal": 5}

    RESULT_CACHE_LOAD_LIMIT = 100

    LOAD_CACHED_DATA_OFFSET = 5

    CREATOR_SUGGESTIONS_LIMIT = 20

    LINSPACE_INTERPOLATION_CONSTANT = 1

    INTERESTS_RANDOM_SAMPLING_SIZE = 5

    MONGO_DATA_DAYS_TO_LOAD = 100

    BATCH_IDS_EXPIRE_OFFSET = 3600

    NOMINAL_DATA_LIMIT = 10000
    MODERATION_TRENDING_CUTOFF = 0.1

    SCORE_ZERO_BUFFER = 1

    MIXPANEL_EVENT_LIST = ["give_a_comment", "content_view_open"]

    MIXPANEL_EVENT_LIMIT = 100000

    MIXPANEL_EVENT_FILTER = ""


CONTENT_TYPES = ModelParameters.CONTENT_TYPES.value
POST_STATUS = ["deleted", "drafted", "published"]
PROFILE_STATUS = ["active", "deleted"]
FEEDTYPES = [
    "video",
    "image",
    "text",
    "textImage",
    "textVideo",
    "titleText",
    "titleImage",
    "titleVideo",
    "titleTextImage",
    "titleTextVideo",
]


def clean_preference_list(_preferences):
    _preferences = _preferences or []
    clean_preferences = []
    for item in _preferences:
        item = ensure_list(item)
        clean_preferences.extend(item)
    return clean_preferences


def get_page_chunks(
    items: List[Any],
    page_size: int,
) -> List[List[Any]]:
    """partition a block of items into chunks of pages"""
    items = ensure_list(items)
    block_length = len(items)
    whole_chunks = block_length // page_size
    chunks = []
    for i in range(whole_chunks):
        start = i * page_size
        stop = (i + 1) * page_size
        chunk = items[start:stop]
        chunks.append(chunk)
    rem = block_length % page_size
    if rem:
        partial_chunk = items[-rem:]
        chunks.append(partial_chunk)
    return chunks


def keyword_preprocessor(_preferences: List[str]) -> List[str]:

    _preferences = clean_preference_list(_preferences)
    _KEYWORD_MAPPING = ModelParameters._KEYWORD_MAPPING.value

    if _preferences:
        preferences = []

        for keyword in _preferences:
            keyword = str(keyword).lower()
            if keyword in ModelParameters.CONTENT_TYPES.value:
                # recognized ES keyword
                preferences.append(keyword)
            elif keyword in _KEYWORD_MAPPING:
                # convert EN keyword to ES keyword
                es_keyword = _KEYWORD_MAPPING[keyword]
                preferences.append(es_keyword)
            else:
                # unrecognized keyword; select random ES keyword
                random_keyword = sample(ModelParameters.CONTENT_TYPES.value, 1)
                preferences.append(random_keyword)
    else:
        preferences = sample(
            ModelParameters.CONTENT_TYPES.value,
            ModelParameters.INTERESTS_RANDOM_SAMPLING_SIZE.value,
        )
    return preferences


def rand_selection(arr: List[Any]) -> Tuple[Any, List[Any]]:
    """randomly select an item from an array"""
    if len(arr) > 0:
        item = sample(arr, 1)
    else:
        item = None
    return item, arr


def get_mongo_read_threshold():
    """
    Get the earliest date to pull Mongo data into models
    :return:
    """
    _MONGO_DATA_OFFSET = timedelta(days=ModelParameters.MONGO_DATA_DAYS_TO_LOAD.value)
    _MONGO_READ_THRESHOLD = datetime.datetime.now() - _MONGO_DATA_OFFSET
    return _MONGO_READ_THRESHOLD


def rand_allocation(
    arr: List[Any],
    allocation_size: int = ModelParameters.NOMINAL_SHARED_INTEREST_POOL_SIZE.value,
) -> List[Any]:
    """randomly allocate a selection from the array"""
    size = len(arr)
    selection_size = min([allocation_size, size]) if size > 0 else 0
    if not selection_size:
        return []
    else:
        return sample(arr, selection_size)


def rand_num(size: int = 10) -> str:
    """generate a random [10] digit string"""
    return str(random()).split(".")[1][:size]


def make_decision():
    """make a random decision"""
    choice = [True, False]
    return rand_selection(choice)[0]


def create_folder(path):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        error = e


def now():
    return datetime.datetime.now()


def elapsed_secs(t):
    """get the number of seconds elapsed since time t"""
    return (now() - t).total_seconds()


def read_nested(obj: dict, path: str) -> Any:
    obj = _force_dict(obj)
    steps = path.split(".")
    for step in steps:
        obj = obj.get(step, {})
    return obj or None


def build_filesystem(rebuild: bool = False):
    """ensure the local filesystem has the required directories"""
    create_dirs = [_DATA_DIR_, _USER_DIR_]
    for _dir in create_dirs:
        if rebuild:
            rmtree(_dir, ignore_errors=True)
        create_folder(_dir)
    return create_dirs


def serialize(item: Any) -> bytes:
    try:
        return pickle.dumps(item)
    except Exception as e:
        error = str(e)
        print(error)
        return b""


def deserialize(item: bytes) -> Any:
    try:
        return pickle.loads(item, encoding="ISO-8859-1")
    except Exception as e:
        error = e
        return None


def save_item(item_class: str, item: Any) -> None:
    """persist an item to the disk"""
    _ITEM_DIR_ = os.path.join(_DATA_DIR_, item_class)
    create_folder(_ITEM_DIR_)
    item_path = os.path.join(_ITEM_DIR_, f"{item.id}.pkl")
    with open(item_path, "wb") as handle:
        handle.write(serialize(item))


def delete_item_dir(item_class: str) -> None:
    """delete item directory from the disk"""
    _ITEM_DIR_ = os.path.join(_DATA_DIR_, item_class)
    shutil.rmtree(path=_ITEM_DIR_, ignore_errors=True)


def clean_model_environs(conf, preference):
    task_type = conf.get("_type")
    # 1. delete server secret
    item_class = f"internal.results_cache.{task_type}.{preference}"
    server_item_class = f"{item_class}.json"
    secret_name = get_secret_name_from_item_class(item_class=server_item_class, **conf)
    webserver.delete_remote_secret(secret_name=secret_name)
    # 2. delete local secrets
    delete_item_dir(item_class=item_class)


class ItemWrapper:
    def __init__(self, item):
        self.id = rand_num()
        self.item = item


class ArgsWrapper(object):
    def __init__(self) -> None:
        super().__init__()
        self._args = {}

    def add_arg(self, arg_name, arg_value):
        self._args[arg_name] = arg_value
        setattr(self, arg_name, arg_value)

    def build_from_dict(self, source: dict):
        for key, value in source.items():
            self.add_arg(key, value)
        return self

    def to_dict(self):
        return dict(self._args)


def log_request(server, info):
    request_name = f"request{rand_num(size=6)}.json"
    request_file = os.path.join(_STATIC_DIR_, request_name)
    with open(request_file, "w") as handle:
        handle.write(json.dumps(info))
    request_hash = get_hash(request_name)
    qualifier = "" if "localhost" in server else "relevance-algorithm"
    log_address = f"{server}/{qualifier}/files/{request_name}?token={request_hash}"
    request_message = log(item=log_address)
    save_item(item_class="requests.log", item=ItemWrapper(request_message))


def log_error(err):
    error_message = log(item=err)
    save_item(item_class="log", item=ItemWrapper(error_message))


def load_item(path: str) -> Any:
    """load an item into memory"""
    try:
        with open(path, "rb") as handle:
            item = deserialize(handle.read())
        return item
    except:
        return None


def load_item_by_id(item_class: str, item_id: str) -> Any:
    try:
        _ITEM_DIR_ = os.path.join(_DATA_DIR_, item_class)
        create_folder(_ITEM_DIR_)
        _file = os.path.join(_ITEM_DIR_, f"{item_id}.pkl")
        return load_item(_file)
    except:
        return None


def load_items(
    item_class: str, load_limit: int = 1000, batch_ids: List[str] = []
) -> List[Any]:
    """load a class of items into memory"""
    _ITEM_DIR_ = os.path.join(_DATA_DIR_, item_class)
    create_folder(_ITEM_DIR_)
    try:
        if not batch_ids:
            all_files = [
                os.path.join(_ITEM_DIR_, f)
                for f in os.listdir(_ITEM_DIR_)
                if os.path.isfile(os.path.join(_ITEM_DIR_, f)) and f.endswith(".pkl")
            ]
        else:
            all_files = [
                os.path.join(_ITEM_DIR_, f"{_id}.pkl")
                for _id in batch_ids
                if os.path.isfile(os.path.join(_ITEM_DIR_, f"{_id}.pkl"))
            ]
    except FileNotFoundError:
        all_files = []
    directory_size = len(all_files)
    use_size = min([load_limit, directory_size])
    files_to_load = rand_allocation(arr=all_files, allocation_size=use_size)
    return [item for item in [load_item(_file) for _file in files_to_load] if item]


def log(item, display: bool = False):
    """print message with timestamp"""
    formatted = f"{datetime.datetime.now().isoformat()} {str(item)}"
    if display:
        print(formatted)
    return formatted


def web_print(item: Any):
    global webserver
    webserver.put_secret(secret_name="printout", secret_value=item)


def weblog(
    mode: str,
    entry: dict = None,
    info: str = None,
    display: bool = False,
    use_cache: bool = True,
):
    global webserver

    logfile = "data_pipe_log.json"

    def retrieve(_use_cache):
        config = dict(
            secret_name=logfile, decode=True, load_json=True, use_cache=_use_cache
        )
        return webserver.retrieve_secret(**config) or []

    def update(_log):
        webserver.temp_files[logfile] = _log

    if mode == "w" and entry is not None:
        # pull current log
        current_log = retrieve(use_cache)
        # add log entry
        current_log.append(entry)
        update(current_log)
        # optional print to console
        if display and info:
            print(log(info))
    if mode == "d":
        webserver.delete_remote_secret(secret_name=logfile)
    if mode == "bq":
        write_to_BQ(table_name="data_pipe_logs", table_data=retrieve(use_cache))


def safe_max(arr: list) -> float:
    try:
        return max(arr)
    except ValueError:
        return 0


def safe_min(arr: list) -> float:
    try:
        return min(arr)
    except ValueError:
        return 0


def get_today():
    return datetime.datetime.now().isoformat().split("T")[0]


def array_empty(arr):
    arr = ensure_list(arr)
    if len(arr) == 0:
        resolved = True
    elif len(arr) == 1 and not arr[0]:
        resolved = True
    else:
        resolved = False
    return resolved if arr is not None else False


def split_comma_separated(text):
    text = text if (isinstance(text, str) or not text) else ",".join(text)
    return [i for i in text.replace(" ", "").split(",") if i] if text else []


def load_bq_creators(args, table_name: str, delete_secret: bool = True):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args.secrets.write_temp_secret(
        "svc.json"
    )
    client = bigquery.Client()
    project = "tcloud-1211"
    dataset_id = "staging"
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_name)
    table = client.get_table(table_ref)
    items = [dict(row) for row in list(client.list_rows(table))]
    if delete_secret:
        args.secrets.delete_secret("svc.json")
    return items


def pull_model_data(args, item_class: str):
    base_profile = dict(
        env=args.environment,
        args=args,
        start_date=args.mongo_data_start_date,
        end_date=args.mongo_data_end_date,
        docker_mongo_ip=args.docker_mongo_ip,
    )
    if item_class == "post":
        mongo_fetch_profile = dict(
            **base_profile,
            data_model="post",
            status=args.post_status,
            keywords=split_comma_separated(args.post_keywords),
            tags=split_comma_separated(args.post_tags),
            categories=split_comma_separated(args.post_categories),
            fields=split_comma_separated(args.post_fields),
            userid=args.post_user_id,
            limit=args.post_limit,
        )
    else:
        mongo_fetch_profile = dict(
            **base_profile,
            data_model="profile",
            fields=split_comma_separated(args.profile_fields),
            tags=split_comma_separated(args.profile_interests),
            categories=split_comma_separated(args.profile_categories),
            status=args.profile_status,
            limit=args.profile_limit,
        )
    items = retrieve_documents_from_mongo(**mongo_fetch_profile)
    return items


def re_eval(item: Any) -> Any:
    try:
        item = eval(item)
    except:
        return item
    return re_eval(item)


def get_secret_name_from_item_class(item_class, **kwargs):
    env = kwargs.get("env", kwargs.get("environment", "")).lower()
    if env in ["prod", "stage", "local"]:
        secret_name = f"{env}.{item_class}"
    else:
        secret_name = item_class
    return secret_name


def read_item_from_webserver(item_class: str, **kwargs):
    global webserver
    # pull from webserver
    secret_name = get_secret_name_from_item_class(item_class, **kwargs)
    items = re_eval(webserver.retrieve_secret(secret_name=secret_name, as_pkg=True))
    result = None
    if items:
        _items = ItemWrapper(items)
        # cache on disk for faster read next time
        save_item(item_class=item_class, item=_items)
        result = [_items]
    return result


def check_stale_web_content(
    task,
    offset_constant: int = ModelParameters.WEB_CACHE_HYDRATION_OFFSET_SECONDS.value,
):
    task_context = f"web_cache_hydration_{task}"
    last_hydration_timestamp = (
        get_event_timestamp(context=task_context, load_from_web=False) or now()
    )
    expired = elapsed_secs(last_hydration_timestamp) >= offset_constant
    if expired:
        set_event_timestamp(context=task_context, sync_to_web=False)
    return expired


def load_single_from_cache(item_class: str, load_from_web: bool = True, **kwargs):
    global webserver
    result = load_items(item_class=item_class, load_limit=1)
    if not result and load_from_web:
        result = read_item_from_webserver(item_class=item_class, **kwargs)
    item = result[-1].item if result else None
    return item


def load_many_from_cache(
    item_class: str, load_limit: int, load_from_web: bool = True, **kwargs
):
    global webserver
    web_refresh_task = f"web_refresh_{item_class}"
    result = load_items(item_class=item_class, load_limit=load_limit)
    if (not result or check_stale_web_content(web_refresh_task)) and load_from_web:
        result = read_item_from_webserver(item_class=item_class, **kwargs)
    item = [ensure_list(entry.item) for entry in result] if result else None
    return list(chain.from_iterable(item)) if item else []


def get_requests_count(context: str = "", load_from_web: bool = True):
    prefix = f"{context}_" if context else ""
    requests_count = load_single_from_cache(
        item_class=f"internal.{prefix}requests_count", load_from_web=load_from_web
    )
    return requests_count or 0


def set_requests_count(
    context: str = "", reset: bool = False, sync_to_web: bool = True
):
    global webserver
    current_count = get_requests_count(context=context, load_from_web=sync_to_web)
    current_count = (current_count + 1) if not reset else 0
    prefix = f"{context}_" if context else ""
    load_key = f"internal.{prefix}requests_count"
    save_item(item_class=load_key, item=ItemWrapper(current_count))
    if sync_to_web:
        webserver.put_secret(
            secret_name=load_key, secret_value=current_count, as_pkg=True
        )
    return current_count


def get_event_timestamp(context, load_from_web: bool = True):
    prefix = f"{context}_" if context else ""
    cached_timestamp = load_single_from_cache(
        item_class=f"internal.{prefix}event_timestamp", load_from_web=load_from_web
    )
    return cached_timestamp


def set_event_timestamp(context: str = "", sync_to_web: bool = True):
    global webserver
    current_timestamp = datetime.datetime.now()
    prefix = f"{context}_" if context else ""
    load_key = f"internal.{prefix}event_timestamp"
    save_item(
        item_class=load_key,
        item=ItemWrapper(current_timestamp),
    )
    if sync_to_web:
        webserver.put_secret(
            secret_name=load_key, secret_value=current_timestamp, as_pkg=True
        )
    return current_timestamp


def pagination_handler(results, **kwargs):
    """
    This function implements cursor-based pagination for the API
    :param results:
    :param kwargs:
    :return: current_page, current_cursor, boolean_if_more_feed, feed_count and page_limit
    """
    pagination_cursor = kwargs.get("pagination_cursor", 0)
    pagination_limit = kwargs.get("pagination_limit")
    feed_count = 0 if not results else len(results)
    if pagination_cursor >= 0 and pagination_limit and feed_count:
        start_index = pagination_cursor
        stop_index = new_cursor = pagination_cursor + pagination_limit
        page = results[start_index:stop_index]
    else:
        page = results
        new_cursor = None
    return (
        page,
        new_cursor,
        False if not new_cursor else bool(new_cursor < feed_count),
        feed_count,
        pagination_limit,
    )


def patch_data(item: dict, patch: dict) -> dict:
    item.update(patch)
    return item


def write_mongo_vector(data: List[dict], _type: str, preference: str, **kwargs):

    # add data updates prior to writing

    if data:

        last_trained_date = datetime.datetime.now().isoformat()

        data = [
            patch_data(
                item=item,
                patch=dict(
                    lastTrainedAt=last_trained_date,
                ),
            )
            for item in data
        ]

        db_operation(
            args=_force_dict(kwargs),
            context="write_mongo_vector",
            op_args=[data, _type, preference],
        )


def cache_results_by_preference(
    preferences: List[str], training_output: dict, _type: str, **kwargs
) -> None:
    global webserver

    data = training_output[_type]

    for preference in preferences:
        primer_class = f"internal.results_cache.{_type}.{preference}"
        load_key = get_secret_name_from_item_class(primer_class, **kwargs)
        # write to webserver
        webserver.put_secret(secret_name=load_key, secret_value=data, as_pkg=True)
        # write to mongo vector
        write_mongo_vector(data, _type, preference, **kwargs)


def train_model_data(
    _type: str, task_config: dict, preferences: List[str], **kwargs
) -> None:

    task_config.update(kwargs)

    if kwargs.get("local_training_run"):
        print(task_config)

    cache_results_by_preference(
        preferences=preferences,
        training_output=training_controller(**task_config),
        _type=_type,
        **kwargs,
    )


def data_picker(arr, sample_size):
    return arr if len(arr) < sample_size else sample(arr, sample_size)


def build_post_args(
    _metadata: dict, creators_list: List[str], creator_preferences: List[str] = []
):
    args = ArgsWrapper()
    args.add_arg("post_user_id", creators_list)
    args.add_arg("post_tags", creator_preferences)
    args.add_arg("post_categories", creator_preferences)
    args.add_arg("from_mongo", True)
    args.add_arg("mongo_data_start_date", get_mongo_read_threshold())
    args.add_arg("mongo_data_end_date", None)
    args.add_arg("environment", _metadata.get("env_name", "INTEGRATION"))
    args.add_arg("docker_mongo_ip", _metadata.get("docker_mongo_ip", ""))
    args.add_arg("post_keywords", _metadata.get("post_keywords", ""))
    args.add_arg("post_fields", _metadata.get("post_fields", ""))
    args.add_arg("post_limit", ModelParameters.POST_STAGING_SAMPLE_SIZE.value)
    args.add_arg("secrets", SecretFileManager())
    args.add_arg("post_status", "published")
    return args


def build_profile_args(
    _metadata: dict, profile_status: str = "", profile_interests: List[str] = []
):
    args = ArgsWrapper()
    args.add_arg("profile_interests", profile_interests)
    args.add_arg("profile_categories", profile_interests)
    args.add_arg("from_mongo", True)
    args.add_arg("mongo_data_start_date", get_mongo_read_threshold())
    args.add_arg("mongo_data_end_date", None)
    args.add_arg("environment", _metadata.get("env_name", "INTEGRATION"))
    args.add_arg("docker_mongo_ip", _metadata.get("docker_mongo_ip", ""))
    args.add_arg("profile_fields", _metadata.get("profile_fields", ""))
    args.add_arg("profile_limit", ModelParameters.STAGING_SAMPLE_SIZE.value)
    args.add_arg("secrets", SecretFileManager())
    args.add_arg("profile_status", profile_status)
    return args


def mark_unmark_ambassador(
    usernames: List[str], ambassador_status: bool, **kwargs
) -> None:
    """
    this function will update the Ambassador status of a group of users
    """
    args = kwargs
    args["secrets"] = SecretFileManager()
    db_operation(
        args,
        context="mark_unmark_ambassador",
        op_args=[usernames, ambassador_status],
    )


def mark_unmark_ambassadors_handler(conf):
    usernames = conf.get("usernames")
    mark = conf.get("mark")
    kwargs = {
        key: value for key, value in conf.items() if key not in ["usernames", "mark"]
    }
    if usernames and isinstance(usernames, list) and isinstance(mark, bool):
        mark_unmark_ambassador(usernames=usernames, ambassador_status=mark, **kwargs)
    return None


def force_model_retrain_handler(conf):
    target_keywords = conf["preferences"]
    for preference in target_keywords:
        # 1. clean remote and local caches
        # clean_model_environs(conf=conf, preference=preference)
        # 2. retrain model for this preference
        conf["username"] = "internal"
        conf["preferences"] = [preference]
        conf["task_config"]["preferences"] = [preference]
        conf["country"] = "AR"
        train_model_data(**conf)
        # delete job package on webserver
        job_key = conf["job_key"]
        webserver.delete_remote_secret(secret_name=job_key)


def write_temp_file(_data):
    rand = rand_num(size=20)
    rand_file = os.path.join(_HERE_, f"{rand}.tmps")
    with open(rand_file, "wb") as handle:
        handle.write(_data)
    return rand_file


def write_json_file(_data):
    rand = rand_num(size=20)
    rand_file = os.path.join(_HERE_, f"{rand}.json")
    with open(rand_file, "w") as handle:
        handle.write(json.dumps(_data))
    return rand_file


def read_json_file(filepath: str, delete: bool = False):
    with open(filepath, "r") as handle:
        _data = json.loads(handle.read())
    if delete:
        try:
            os.remove(filepath)
        except:
            pass
    return _data


def unpack_webserver_item(item: Any) -> Any:
    item_bytes = ensure_string(item).encode("ISO-8859-1")
    item_converted = deserialize(item_bytes)
    return item_converted if item_converted is not None else None


class SecretFileManager:
    def __init__(self, force_secure: bool = False) -> None:
        self.force_secure = force_secure
        self.http_protocol = "http" if not self.force_secure else "https"
        self.secrets_url_format = (
            f"{self.http_protocol}://35.225.91.166:81/files/%s?token=%s"
        )
        self.secrets_token = os.environ.get("API_KEY")
        self.temp_files = {}

    def live_retrieve(
        self,
        secret_name: str,
        decode=False,
        as_pkg: bool = False,
        load_json: bool = False,
        use_cache: bool = False,
    ):
        retrieve_url = self.secrets_url_format % (secret_name, self.secrets_token)
        try:
            _data = http.get(retrieve_url).content
        except Exception as err:
            print(err)
            _data = b""
        result = _data if not decode else _data.decode()
        if load_json and result:
            result = json.loads(result)
        if as_pkg and _data:
            pkg = re_eval(_data)
            result = [unpack_webserver_item(item) for item in pkg]
        if use_cache:
            self.temp_files[secret_name] = result
        return result

    def retrieve_secret(
        self,
        secret_name: str,
        decode=False,
        as_pkg: bool = False,
        load_json: bool = False,
        use_cache: bool = False,
    ) -> bytes:
        secret_name = secret_name if not as_pkg else f"{secret_name}.json"
        if use_cache and not check_stale_web_content(secret_name):
            result = self.temp_files.get(secret_name)
            if not result:
                result = self.live_retrieve(
                    secret_name,
                    decode=decode,
                    as_pkg=as_pkg,
                    load_json=load_json,
                    use_cache=use_cache,
                )
        else:
            result = self.live_retrieve(
                secret_name,
                decode=decode,
                as_pkg=as_pkg,
                load_json=load_json,
                use_cache=use_cache,
            )
        return result

    def put_secret(
        self, secret_name: str, secret_value: Any, as_pkg: bool = False
    ) -> None:
        secret_name = secret_name if not as_pkg else f"{secret_name}.json"
        put_url = self.secrets_url_format % (secret_name, self.secrets_token)
        if as_pkg:
            secret_value = ensure_list(secret_value)
            _data = [serialize(item).decode("ISO-8859-1") for item in secret_value]
        else:
            _data = secret_value
        try:
            http.put(
                url=put_url,
                data=json.dumps({"data": _data}),
                headers={"Content-Type": "application/json"},
            )
        except Exception as err:
            print(err)

    def write_temp_secret(self, secret_name: str) -> str:
        secret = self.retrieve_secret(secret_name)
        secret_path = write_temp_file(secret)
        if secret:
            self.temp_files[secret_name] = secret_path
        return secret_path

    def delete_secret(self, secret_name: str) -> None:
        target = self.temp_files.get(secret_name)
        try:
            os.remove(target)
        except:
            pass

    def delete_remote_secret(self, secret_name: str) -> int:
        delete_url = self.secrets_url_format % (secret_name, self.secrets_token)
        try:
            return http.delete(delete_url).status_code
        except Exception as err:
            print(err)
            return 500


webserver = SecretFileManager()


def gen_unique_id() -> str:
    """generate random id for user or content"""
    return str(random()).split(".")[1][:16]


def gen_users() -> List[Any]:
    return [gen_unique_id() for _ in range(20)]


def decide_nsfw():
    """make a random decision"""
    choice = [True, False]
    return rand_selection(choice)[0][0]


def rand_between(a, b):
    return int(a + random() * (b - a))


def _build_profile(creator_ids: List[str]) -> dict:
    NAME = fake.name()
    ABOUT = fake.sentence()

    data = {
        "profileId": str(ObjectId()),
        "accountId": str(ObjectId()),
        "status": rand_selection(PROFILE_STATUS)[0][0],
        "username": rand_selection(creator_ids)[0][0],
        "name": NAME,
        "about": ABOUT,
        "link": fake.url(),
        "birthdate": "2023-01-25T22:10:39.680Z",
        "gender": rand_selection(["female", "male"])[0][0],
        "location": {
            "country": "AR",
            "state": "Buenos Aires",
            "city": "Buenos Aires",
            "latitude": "",
            "longitude": "",
        },
        "language": {"language": "spanish", "properties": ""},
        "interests": rand_allocation(CONTENT_TYPES, 2),
        "nsfw": decide_nsfw(),
        "apparelSettings": {"banner": "", "background": "", "avatar": fake.image_url()},
        "notificationSettings": {"regular": decide_nsfw(), "push": decide_nsfw()},
        "blockedUsers": [],
        "createdAt": datetime.datetime.utcnow().isoformat() + "Z",
        "updatedAt": datetime.datetime.utcnow().isoformat() + "Z",
    }

    return data


def _build_post(creator_ids: List[str]) -> dict:
    TITLE = fake.sentence()
    THUMBNAIL = fake.image_url()
    TYPE = rand_selection(FEEDTYPES)[0][0]
    creator = rand_selection(creator_ids)[0][0]
    _id = str(ObjectId())

    data = {
        "_id": _id,
        "postId": _id,
        "creatorId": creator,
        "createdAt": datetime.datetime.utcnow().isoformat() + "Z",
        "updaterId": "public",
        "updatedAt": datetime.datetime.utcnow().isoformat() + "Z",
        "__STATE__": "PUBLIC",
        "userId": creator,
        "title": TITLE,
        "body": {
            "children": [],
            "direction": "ltr",
            "format": "",
            "indent": 0,
            "type": "root",
            "version": 1,
        },
        "nsfw": decide_nsfw(),
        "thumbnail": THUMBNAIL,
        "slug": fake.slug(),
        "tags": rand_allocation(CONTENT_TYPES, 2),
        "keywords": ["prensa", "taringa"],
        "status": rand_selection(POST_STATUS)[0][0],
        "abstract": {
            "contentId": "someId",
            "authorId": creator,
            "feedType": TYPE,
            "title": TITLE,
            "mediaLink": fake.url(),
            "text": fake.sentence(),
            "longContent": False,
        },
    }

    return data


def _marshal(item, schema):
    try:
        return marshal(item, schema)
    except:
        _dict = item.to_mongo().to_dict()
        return {field: _dict.get(field) for field in schema}


def _force_dict(item, debug: bool = False, extra_info: Any = None):
    if isinstance(item, ArgsWrapper):
        item = item.to_dict()
        if debug:
            print(item, extra_info)
    else:
        if hasattr(item, "__dict__"):
            item = vars(item)
        else:
            item = dict(item)
    return item


def retrieve_documents_from_mongo(
    data_model: str,
    env: str,
    args: Any,
    start_date=None,
    end_date=None,
    status: str = None,
    keywords: List[str] = None,
    tags: List[str] = None,
    categories: List[str] = None,
    fields: List[str] = None,
    userid: List[str] = None,
    docker_mongo_ip: str = "127.0.0.1",
    limit: int = 1,
) -> list:

    _IS_LOCAL = env == "LOCAL"

    if _IS_LOCAL:

        db_operation(
            args=_force_dict(args),
            context="local_db_check",
            op_args=[data_model, limit, data_model, args],
        )

    results, requested_fields = query_mongo(
        args,
        start_date=start_date,
        end_date=end_date,
        status=status,
        keywords=keywords,
        tags=tags,
        categories=categories,
        fields=fields,
        userid=userid,
        data_model=data_model,
    )

    _schema = {}

    if data_model == "post":
        _schema = get_reduced_post_schema()
    if data_model == "profile":
        _schema = get_profile_schema()

    if requested_fields:
        _schema = {
            field: value
            for field, value in _schema.items()
            if field in requested_fields
        }

    results_marshalled = [_marshal(result, _schema) for result in results]
    return results_marshalled[:limit]


def get_BQ_client():
    global webserver
    secret_path = webserver.temp_files.get("svc.json")
    if not secret_path:
        webserver.write_temp_secret("svc.json")
        secret_path = webserver.temp_files.get("svc.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = secret_path
    client = bigquery.Client()
    return client


def write_to_BQ(table_name, table_data):
    global webserver
    client = get_BQ_client()
    table_id = f"tcloud-1211.staging.{table_name}".lower()
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    table_df = pd.DataFrame(table_data)
    job = client.load_table_from_dataframe(table_df, table_id, job_config=job_config)
    result = job.result()
    webserver.delete_secret("svc.json")
    return result


@retry(
    Exception,
    tries=MAX_PIPE_OPERATION_RETRIES,
    delay=PIPE_OPERATION_RETRY_DELAY_SECONDS,
)
def _db_pipe_operation(environment, *args) -> Any:
    global webserver

    process_started = now()
    context, db_type, *_ = args

    query_params, mode, store_key = ARGS_BUILDERS[db_type](*args)
    _result = webserver.temp_files.get(store_key)

    if not _result or check_stale_web_content(store_key):

        pipe_cmd_template = PIPE_TEMPLATES[db_type]
        pipe_cmd = pipe_cmd_template % query_params

        if mode in SYNCHRONOUS_PIPE_OPERATIONS:
            # synchronous fetches
            raw_output = subprocess.check_output(pipe_cmd, shell=True)
            _result = re_eval(raw_output)
        else:
            # asynchronous inserts, updates and deletions
            subprocess.Popen(pipe_cmd, shell=True)
            _result = None

        # cache the result by unique args
        webserver.temp_files[store_key] = _result

    duration = elapsed_secs(process_started)
    process_log_str = f"db pipe process [environment={environment}, data_environment={db_type}, context={context}, \
                  mode={mode}] lasted: {duration} secs."
    process_log = {
        "timestamp": now().isoformat(),
        "environment": environment,
        "data_environment": db_type,
        "context": context,
        "mode": mode,
        "duration": duration,
    }
    weblog(mode="w", entry=process_log, info=process_log_str, display=True)
    return _result


def db_pipe_operation(
    environment: str, context: str, instructions: Union[dict, List[dict]]
) -> List[Any]:
    results = []

    instructions = ensure_list(instructions)

    for instruction in instructions:

        result = None

        db_type = instruction.get("db_type", "mongodb")
        mode = instruction.get("mode")

        # enable output propagation
        propagate = instruction.get("propagate")
        if propagate and mode == "insert" and len(results) > 0:
            # load the last result into current process
            last_result = results[-1]
            if isinstance(last_result, str):
                last_result = [
                    item
                    for item in [re_eval(row) for row in last_result.split("\n")]
                    if item
                ]
            if db_type != "bigquery":
                instruction["query"] = last_result
            else:
                instruction["table_data"] = last_result

        if db_type != "bigquery":
            args_schema = ARGS_SCHEMAS[db_type]
            args = (context, db_type)
            args += tuple(
                instruction.get(arg, data_type())
                for arg, data_type in args_schema.items()
            )
            result = _db_pipe_operation(environment, *args)
        else:
            if mode == "insert":
                table_name = instruction.get("table_name")
                table_data = instruction.get("table_data")
                result = write_to_BQ(table_name=table_name, table_data=table_data)
        results.append(result)
    return results


def get_mongo_conn_string(mongo_conn_strings, env, docker_mongo_ip):
    mongo_conn_string = (
        mongo_conn_strings.get(env)
        if env != "local"
        else "mongodb://%s:27017/default_local" % docker_mongo_ip
    )
    return mongo_conn_string


def ensure_string(item: Any) -> Any:
    if isinstance(item, list):
        return [str(i) for i in item if i]
    else:
        return str(item) if item is not None else ""


def db_operation(args: dict, context: str, op_args: list):
    print(context, ": started")

    global webserver

    env = str(args.get("environment") or args.get("env")).lower()
    mixpanel_env = args.get("mxp_environment") or args.get("mxp_env") or "test"
    docker_mongo_ip = args.get("docker_mongo_ip")

    mongo_db_map = {
        "integration": "taringa-platform-development",
        "stage": "taringa-platform-staging",
        "prod": "taringa-platform-prod",
        "local": "default_local",
    }

    mongo_conn_strings = webserver.retrieve_secret(
        secret_name="ENV_MAP.json", decode=True, load_json=True, use_cache=True
    )

    mixpanel_secrets = webserver.retrieve_secret(
        secret_name="MXP_PIPE_SRV_ACCTS.json",
        decode=True,
        load_json=True,
        use_cache=True,
    )

    mixpanel_secret = mixpanel_secrets.get(mixpanel_env)

    mongo_conn_string = get_mongo_conn_string(mongo_conn_strings, env, docker_mongo_ip)

    mongo_db_name = mongo_db_map.get(env)

    _result = None

    def get_collection_name(data_model):
        collection_map = {
            "post": "content",
            "profile": "profiles",
            "user": "profiles",
        }
        return collection_map[data_model]

    def generate_creators_or_posts(creators, data_model, size, args):

        # build creator ids (from BigQuery if there are no creators in the DB)
        if not creators:
            creators = load_bq_creators(args, table_name="creators_interests")

        creator_ids = [creator["username"] for creator in creators]

        # select object builder
        _object_builder = eval(f"_build_{data_model}")

        # build objects and return
        return [_object_builder(creator_ids=creator_ids) for _ in range(size)]

    if context == "mark_creators":
        users_with_posts, users_with_complete_profile = op_args
        collection = "profiles"
        base_config = dict(
            conn_string=mongo_conn_string,
            target_db=mongo_db_name,
            target_collection=collection,
        )
        db_pipe_operation(
            env,
            context,
            [
                dict(
                    **base_config,
                    mode="update",
                    filter={"_id": {"$in": ensure_string(users_with_posts)}},
                    patch={"$set": {"isCreator": true}},
                    object_id_fields=["_id.$in"],
                    db_type="pymongo",
                ),
                dict(
                    **base_config,
                    mode="update",
                    filter={"_id": {"$in": ensure_string(users_with_complete_profile)}},
                    patch={"$set": {"profileCompleted": true}},
                    object_id_fields=["_id.$in"],
                    db_type="pymongo",
                ),
            ],
        )

    if context == "local_db_check":
        data_model, limit, data_model, args = op_args
        collection = get_collection_name(data_model)

        base_config = dict(
            conn_string=mongo_conn_string,
            target_db=mongo_db_name,
        )

        (existing_creators, existing_posts) = db_pipe_operation(
            env,
            context,
            [
                dict(
                    **base_config,
                    target_collection="profiles",
                    mode="find",
                ),
                dict(
                    **base_config,
                    target_collection="content",
                    mode="find",
                ),
            ],
        )

        local_collection_size = len(
            existing_creators if collection == "profile" else existing_posts
        )
        delta = limit - local_collection_size

        if delta > 0:
            array = generate_creators_or_posts(
                creators=existing_creators, data_model=data_model, size=delta, args=args
            )
            db_pipe_operation(
                env,
                context,
                dict(
                    conn_string=mongo_conn_string,
                    target_db=mongo_db_name,
                    target_collection=collection,
                    mode="insert",
                    query=array,
                    object_id_fields=OBJECT_ID_FIELDS,
                ),
            )

    if context == "query_mongo":
        data_model, final_query, fields = op_args
        collection = get_collection_name(data_model)
        query = {}
        projection = {}
        if final_query:
            query = final_query
        if fields:
            projection = {"_id": 1, **{field: 1 for field in fields}}
        (_result,) = db_pipe_operation(
            env,
            context,
            dict(
                conn_string=mongo_conn_string,
                target_db=mongo_db_name,
                target_collection=collection,
                mode="find",
                query=query,
                projection=projection,
                object_id_fields=["_id"],
                date_fields=["createdAt.$gt", "createdAt.$lt"],
            ),
        )

    if context == "mark_unmark_ambassador":
        usernames, ambassador_status = op_args
        user_exists = {"username": {"$in": usernames}}
        user_profile_complete = {"profileCompleted": true}
        user_is_creator = {"isCreator": true}
        validated = {**user_exists, **user_profile_complete, **user_is_creator}

        collection = get_collection_name("profile")

        (_result,) = db_pipe_operation(
            env,
            context,
            dict(
                conn_string=mongo_conn_string,
                target_db=mongo_db_name,
                target_collection=collection,
                mode="update",
                filter=validated,
                patch={"$set": {"isAmbassador": ambassador_status}},
            ),
        )

    if context == "fetch_profile_by_username":
        (item_id,) = op_args
        collection = get_collection_name("profile")
        (_result,) = db_pipe_operation(
            env,
            context,
            dict(
                conn_string=mongo_conn_string,
                target_db=mongo_db_name,
                target_collection=collection,
                mode="find_one",
                query={"username": ensure_string(item_id)},
            ),
        )

    if context == "fetch_post_by_id":
        (item_id,) = op_args
        collection = get_collection_name("post")
        (_result,) = db_pipe_operation(
            env,
            context,
            dict(
                conn_string=mongo_conn_string,
                target_db=mongo_db_name,
                target_collection=collection,
                mode="find_one",
                query={"_id": ensure_string(item_id)},
                object_id_fields=["_id"],
            ),
        )

    if context == "fetch_random_posts":
        (exclude_ids, required_size) = op_args
        collection = "content"
        (_result,) = db_pipe_operation(
            env,
            context,
            dict(
                conn_string=mongo_conn_string,
                target_db=mongo_db_name,
                target_collection=collection,
                mode="find",
                query={
                    "_id": {"$nin": ensure_string(exclude_ids)},
                },
                object_id_fields=["_id.$nin"],
                db_type="pymongo",
                limit=required_size,
            ),
        )
        print(f"found: {len(_result)} random posts")

    if context == "filter_creators_by_preference":
        preferences, existing_creators, required_size, country = op_args
        collection = "profiles"
        (_result,) = db_pipe_operation(
            env,
            context,
            dict(
                conn_string=mongo_conn_string,
                target_db=mongo_db_name,
                target_collection=collection,
                mode="find",
                query={
                    "interests": {"$in": preferences},
                },
                db_type="pymongo",
                limit=required_size,
            ),
        )

    if context == "fetch_post_comments":
        (post_id,) = op_args
        post_id = ensure_list(post_id)
        collection = "comments"
        (_result,) = db_pipe_operation(
            env,
            context,
            dict(
                conn_string=mongo_conn_string,
                target_db=mongo_db_name,
                target_collection=collection,
                mode="find",
                query={
                    "contentId": {"$in": ensure_string(post_id)},
                    "banned": {"$nin": [true]},
                },
                object_id_fields=["contentId.$in"],
                db_type="pymongo",
            ),
        )

    if context == "fetch_user_comments":
        (user_id,) = op_args
        user_id = ensure_list(user_id)
        collection = "comments"
        (_result,) = db_pipe_operation(
            env,
            context,
            dict(
                conn_string=mongo_conn_string,
                target_db=mongo_db_name,
                target_collection=collection,
                mode="find",
                query={
                    "authorId": {"$in": ensure_string(user_id)},
                    "banned": {"$nin": [true]},
                },
                object_id_fields=["authorId.$in"],
                db_type="pymongo",
            ),
        )

    if context == "fetch_user_followers":
        (user_id,) = op_args
        user_id = ensure_list(user_id)
        collection = "followers"
        (_result,) = db_pipe_operation(
            env,
            context,
            dict(
                conn_string=mongo_conn_string,
                target_db=mongo_db_name,
                target_collection=collection,
                mode="find",
                query={"userId": {"$in": ensure_string(user_id)}},
                object_id_fields=["userId.$in"],
                db_type="pymongo",
            ),
        )

    if context == "fetch_user_posts":
        (user_id,) = op_args
        user_id = ensure_list(user_id)
        collection = "content"
        (_result,) = db_pipe_operation(
            env,
            context,
            dict(
                conn_string=mongo_conn_string,
                target_db=mongo_db_name,
                target_collection=collection,
                mode="find",
                query={"userId": {"$in": ensure_string(user_id)}},
                object_id_fields=["userId.$in"],
                db_type="pymongo",
            ),
        )

    if context == "write_mongo_vector":
        data, _type, preference = op_args
        collection = f"{_type}_{preference}_vector"

        base_config = dict(
            conn_string=mongo_conn_string,
            target_db=mongo_db_name,
            target_collection=collection,
        )

        db_pipe_operation(
            env,
            context,
            [
                dict(
                    **base_config,
                    mode="drop",
                ),
                dict(
                    **base_config,
                    mode="insert",
                    query=data,
                    object_id_fields=OBJECT_ID_FIELDS,
                    date_fields=["createdAt", "updatedAt", "lastTrainedAt"],
                ),
            ],
        )

        _result = collection

    if context == "write_moderation_data":
        (data,) = op_args
        collection = "moderation_output"

        base_config = dict(
            conn_string=mongo_conn_string,
            target_db=mongo_db_name,
            target_collection=collection,
        )

        db_pipe_operation(
            env,
            context,
            [
                dict(
                    **base_config,
                    mode="drop",
                ),
                dict(
                    **base_config,
                    mode="insert",
                    query=data,
                    object_id_fields=["_id"],
                    date_fields=["createdAt"],
                    db_type="pymongo",
                ),
            ],
        )

        _result = collection

    if context == "mixpanel_export":
        (from_date, to_date, event_list, event_limit, event_filter) = op_args
        (_result,) = db_pipe_operation(
            env,
            context,
            dict(
                secret_object=mixpanel_secret,
                from_date=from_date,
                to_date=to_date,
                event_list=event_list,
                mode="export",
                event_limit=event_limit,
                event_filter=event_filter,
                db_type="mixpanel",
            ),
        )

        _result = [
            item for item in [re_eval(row) for row in _result.split("\n")] if item
        ]

    if context == "mixpanel_export_mongo":
        (
            from_date,
            to_date,
            event_list,
            event_limit,
            event_filter,
            mongo_env,
            mongo_collection,
        ) = op_args
        _result = db_pipe_operation(
            env,
            context,
            [
                dict(
                    secret_object=mixpanel_secret,
                    from_date=from_date,
                    to_date=to_date,
                    event_list=event_list,
                    mode="export",
                    event_limit=event_limit,
                    event_filter=event_filter,
                    db_type="mixpanel",
                ),
                dict(
                    conn_string=get_mongo_conn_string(
                        mongo_conn_strings, mongo_env, docker_mongo_ip
                    ),
                    target_db=mongo_db_map[mongo_env],
                    target_collection=mongo_collection,
                    mode="insert",
                    propagate=true,
                ),
            ],
        )

    if context == "mixpanel_export_bigquery":
        (
            from_date,
            to_date,
            event_list,
            event_limit,
            event_filter,
            bq_table_name,
        ) = op_args
        _result = db_pipe_operation(
            env,
            context,
            [
                dict(
                    secret_object=mixpanel_secret,
                    from_date=from_date,
                    to_date=to_date,
                    event_list=event_list,
                    mode="export",
                    event_limit=event_limit,
                    event_filter=event_filter,
                    db_type="mixpanel",
                ),
                dict(
                    table_name=bq_table_name,
                    mode="insert",
                    propagate=true,
                    db_type="bigquery",
                ),
            ],
        )

    if context == "write_to_BQ":
        (table_name, table_data) = op_args
        (_result,) = db_pipe_operation(
            env,
            context,
            dict(
                mode="insert",
                table_name=table_name,
                table_data=table_data,
                db_type="bigquery",
            ),
        )

    if context == "fetch_users_with_complete_profile":
        (user_id,) = op_args
        user_id = ensure_list(user_id)
        collection = "profiles"
        (_result,) = db_pipe_operation(
            env,
            context,
            dict(
                conn_string=mongo_conn_string,
                target_db=mongo_db_name,
                target_collection=collection,
                mode="find",
                query={
                    "_id": {"$in": ensure_string(user_id)},
                    "name": {"$nin": [None, ""]},
                    "about": {"$nin": [None, ""]},
                },
                object_id_fields=["_id.$in"],
                db_type="pymongo",
            ),
        )

    if context == "fetch_users_ambassadors":
        (user_id,) = op_args
        user_id = ensure_list(user_id)
        collection = "profiles"
        (_result,) = db_pipe_operation(
            env,
            context,
            dict(
                conn_string=mongo_conn_string,
                target_db=mongo_db_name,
                target_collection=collection,
                mode="find",
                query={
                    "_id": {"$in": ensure_string(user_id)},
                    "isAmbassador": true,
                },
                object_id_fields=["_id.$in"],
                db_type="pymongo",
            ),
        )

    if context == "fetch_reported_contents":
        (collection,) = op_args
        query = {"reportCount": {"$gt": 0}, "moderationStatus": "uncheck"}
        (_result,) = db_pipe_operation(
            env,
            context,
            dict(
                conn_string=mongo_conn_string,
                target_db=mongo_db_name,
                target_collection=collection,
                mode="find",
                query=query,
            ),
        )

    if context == "fetch_contents_period":
        collection, start_date, end_date = op_args
        query = {
            "createdAt": {"$gte": start_date, "$lte": end_date},
            "status": "published",
        }
        (_result,) = db_pipe_operation(
            env,
            context,
            dict(
                conn_string=mongo_conn_string,
                target_db=mongo_db_name,
                target_collection=collection,
                mode="find",
                query=query,
                date_fields=["createdAt.$gte", "createdAt.$lte"],
            ),
        )

    if context == "fetch_reported_comments":
        (collection,) = op_args
        query = {"reportCount": {"$gt": 0}, "moderationStatus": "uncheck"}
        (_result,) = db_pipe_operation(
            env,
            context,
            dict(
                conn_string=mongo_conn_string,
                target_db=mongo_db_name,
                target_collection=collection,
                mode="find",
                query=query,
            ),
        )

    if context == "trend_ids_mongo_lookup":
        collection, lookup_ids, start_date, end_date = op_args
        query = {
            "createdAt": {"$gte": start_date, "$lte": end_date},
            "abstract.contentId": {"$in": lookup_ids},
            "moderation": {"$exists": False},
        }
        projection = {
            "_id": 1,
            "abstract.contentId": 1,
            "userId": 1,
            "createdAt": 1,
        }
        (_result,) = db_pipe_operation(
            env,
            context,
            dict(
                conn_string=mongo_conn_string,
                target_db=mongo_db_name,
                target_collection=collection,
                mode="find",
                projection=projection,
                query=query,
                object_id_fields=["abstract.contentId.$in"],
                date_fields=["createdAt.$gte", "createdAt.$lte"],
                db_type="pymongo",
            ),
        )

    print(context, ": finished")
    return _result


def query_mongo(
    args,
    start_date=None,
    end_date=None,
    status=None,
    keywords=None,
    tags=None,
    categories=None,
    fields=None,
    userid=None,
    data_model=None,
):
    """
    Query MongoDB based on the provided parameters.
    """

    # contextually build required queries for posts and profiles

    req_query = {}
    if start_date:
        req_query = {"createdAt": {"$gt": start_date.isoformat()}}

    status_query = {}
    if status:
        if data_model == "post":
            status_query = {"status": status}
        else:
            if status == "creator":
                status_query = {"isCreator": true}
            if status == "ambassador":
                status_query = {"isAmbassador": true}
        req_query = {**req_query, **status_query}

    # contextually build optional queries for posts and profiles

    opt_query = {"$or": []}

    if end_date:
        opt_query["$or"].append({"createdAt": {"$lt": end_date.isoformat()}})

    if keywords and data_model == "post":
        opt_query["$or"].append({"keywords": {"$in": keywords}})

    if tags:
        updated_tags = tags
        tags_query = (
            {"tags": {"$in": updated_tags}}
            if data_model == "post"
            else {"interests": {"$in": updated_tags}}
        )
        opt_query["$or"].append(tags_query)

    if categories:
        updated_categories = categories
        categories_query = (
            {"categories": {"$in": updated_categories}}
            if data_model == "post"
            else {"interests": {"$in": updated_categories}}
        )
        opt_query["$or"].append(categories_query)

    if userid:
        id_query = (
            {"userId": {"$in": userid}}
            if data_model == "post"
            else {"username": {"$in": userid}}
        )
        opt_query["$or"].append(id_query)

    # construct final query and retrieve results

    final_query = {**req_query, **opt_query}

    result = db_operation(
        args=_force_dict(args),
        context="query_mongo",
        op_args=[data_model, final_query, fields],
    )

    return result, fields


def parse_items_by_id(items, id_field, _id):
    parsed_items = [item for item in items if item[id_field] == _id]
    return {"items": parsed_items, "items_count": len(parsed_items)}


def build_aggregate(collection: List[dict], id_field: str) -> dict:
    _ids = {item[id_field] for item in collection}
    aggregated = {
        _id: parse_items_by_id(collection, id_field=id_field, _id=_id) for _id in _ids
    }
    return aggregated


def get_post_comments(post_id, **kwargs):
    post_comments = db_operation(
        args=_force_dict(kwargs),
        context="fetch_post_comments",
        op_args=[post_id],
    )
    return build_aggregate(collection=post_comments, id_field="contentId")


def get_user_comments(user_id, **kwargs):
    user_comments = db_operation(
        args=_force_dict(kwargs),
        context="fetch_user_comments",
        op_args=[user_id],
    )
    return build_aggregate(collection=user_comments, id_field="authorId")


def get_user_followers(user_id, **kwargs):
    user_followers = db_operation(
        args=_force_dict(kwargs),
        context="fetch_user_followers",
        op_args=[user_id],
    )
    return build_aggregate(collection=user_followers, id_field="userId")


def get_user_posts(user_id, **kwargs):
    user_posts = db_operation(
        args=_force_dict(kwargs),
        context="fetch_user_posts",
        op_args=[user_id],
    )
    return build_aggregate(collection=user_posts, id_field="userId")


def get_user_ambassadors(user_id, **kwargs):
    user_profiles = db_operation(
        args=_force_dict(kwargs),
        context="fetch_users_ambassadors",
        op_args=[user_id],
    )
    return build_aggregate(collection=user_profiles, id_field="_id")


def get_user_complete_profile(user_id, **kwargs):
    user_profiles = db_operation(
        args=_force_dict(kwargs),
        context="fetch_users_with_complete_profile",
        op_args=[user_id],
    )
    return build_aggregate(collection=user_profiles, id_field="_id")


def args_to_dict(*args, **kwargs):
    return {**{i: args[i] for i in range(len(args))}, **dict(kwargs)}


def make_json_safe(data: Any):
    data_str = repr(data)
    data_str = data_str.replace("ObjectId", "str")
    for item in ["datetime.datetime", "datetime"]:
        data_str = data_str.replace(item, "args_to_dict")
    return eval(data_str)


def push_signal(**config):

    """
    this decorator will transmit telemetry (measurement) artifacts to the webserver
    """

    telemetry_type = config.get("telemetry_type")
    signal_name = config.get("signal_name")
    context = dict(factor_list=["experiment_date"], experiment_date=get_today())

    def wrapper(func):
        def handle_func(*args, **kwargs):
            func_data = func(*args, **kwargs)
            if telemetry_type == "telemetry_test":
                update_artifact(
                    item_class=f"telemetry::{telemetry_type}",
                    update={signal_name: make_json_safe(func_data)},
                    **context,
                )
            return func_data

        return handle_func

    return wrapper


def unpack_aggregate(aggregate: dict) -> list:
    items = []
    if aggregate:
        for _, content in aggregate.items():
            items.extend(content["items"])
    return items


def unpack_aggregate_list(aggregate_list: List[dict]) -> list:
    items = []
    for aggregate in aggregate_list:
        items.extend(unpack_aggregate(aggregate))
    return items


@push_signal(
    telemetry_type="create_post_artifact",
    signal_name=f"post_artifact_{gen_unique_id()}",
)
def create_post_artifacts(posts: List[dict], **kwargs):

    global webserver

    _POST_IDS = [post["_id"] for post in posts]

    artifact_body = {
        "_POST_IDS": _POST_IDS,
        "_POST_COMMENTS": get_post_comments(post_id=_POST_IDS, **kwargs),
    }

    build_artifact(item_class="post", artifact_body=artifact_body, **kwargs)

    return artifact_body


def get_ids(items: List[dict]) -> List[Union[ObjectId, str]]:
    return [item["_id"] for item in items]


def create_shared_property_pool(
    use_property: str,
    props: tuple,
    pool_size: int = ModelParameters.NOMINAL_SHARED_INTEREST_POOL_SIZE.value,
    post_limit: int = ModelParameters.NOMINAL_SHARED_INTEREST_POST_LIMIT.value,
    include_posts: bool = False,
    **kwargs,
) -> tuple:
    """Given a user, create a pool of users and their posts (optional) based on shared properties"""

    global webserver

    preferences, country = props

    creators_with_shared_properties = get_creators_from_db(
        required_size=pool_size,
        creator_preferences=preferences if use_property == "preferences" else None,
        country=country,
        **kwargs,
    )

    if include_posts:

        creator_batch_ids = [user["_id"] for user in creators_with_shared_properties]
        creator_posts = unpack_aggregate_list(
            get_creator_posts(creator_batch_ids=creator_batch_ids, **kwargs)
        )

        if (posts_size := len((post_ids := get_ids(creator_posts)))) < post_limit:
            required_size = post_limit - posts_size
            extra_posts = db_operation(
                args=_force_dict(kwargs),
                context="fetch_random_posts",
                op_args=[post_ids, required_size],
            )
            creator_posts.extend(extra_posts)

        selected_posts = sample(creator_posts, min(post_limit, len(creator_posts)))

    else:
        selected_posts = []

    return creators_with_shared_properties, selected_posts


def convert_timestamp(timestamps: dict):
    created_at = timestamps.get("createdAt")
    updated_at = timestamps.get("updatedAt")
    timestamp = updated_at if updated_at else created_at
    return timestamp


def build_paired_dataset(arr: list) -> List[list]:
    paired_data = get_page_chunks(items=arr, page_size=2)
    return [entry for entry in paired_data if len(entry) == 2]


def compute_periodicity(paired_timestamps: List[list]) -> float:
    """
    Function to compute post periodicity given a paired timestamp dataset
    :param paired_timestamps:
    :return: average interval between posts (float)
    """
    intervals_seconds = []
    for j, i in paired_timestamps:
        interval = (j - i).total_seconds()
        intervals_seconds.append(interval)
    return safely_divide(sum(intervals_seconds), len(intervals_seconds))


def get_post_periodicity(posts: List[dict]) -> float:

    if posts:

        post_timestamps = [
            {"createdAt": post["createdAt"], "updatedAt": post["updatedAt"]}
            for post in posts
        ]

        post_timestamps_converted = [
            convert_timestamp(timestamps=timestamps) for timestamps in post_timestamps
        ]

        post_timestamps_sorted = sorted(post_timestamps_converted, reverse=True)
        post_timestamps_paired = build_paired_dataset(arr=post_timestamps_sorted)
        periodicity = compute_periodicity(post_timestamps_paired)
    else:
        periodicity = 0

    return periodicity


@push_signal(
    telemetry_type="post_metrics", signal_name=f"post_metrics_{gen_unique_id()}"
)
def post_metrics(post: dict, **kwargs):
    """collect post metrics"""
    global webserver
    created_timestamp = post["createdAt"]
    secs_since_published = elapsed_secs(created_timestamp)
    post_recency = log10(secs_since_published)
    metrics = {
        "post_engagements": from_artifact(
            contexts=["_POST_COMMENTS"],
            _id=post["_id"],
            item_class="post",
            _type="items_count",
            **kwargs,
        ),
        "post_earnings": 0,
        "post_recency_factor": safely_divide(1, post_recency),
    }
    print("post metrics", metrics)
    return metrics


@push_signal(
    telemetry_type="user_metrics", signal_name=f"user_metrics_{gen_unique_id()}"
)
def aggregate_metrics(user: dict, **kwargs) -> dict:
    """aggregate model metrics for a user"""

    global webserver

    user_id = ObjectId(user["_id"])

    (
        creator_post_engagements,
        creator_follower_count,
        creator_is_ambassador,
        creator_profile_complete,
    ) = from_artifact(
        contexts=[
            "_USER_COMMENTS",
            "_USER_FOLLOWERS",
            "_USER_AMBASSADORS",
            "_USER_COMPLETE_PROFILE",
        ],
        _id=user_id,
        item_class="user",
        _type="items_count",
        **kwargs,
    )

    creator_posts = from_artifact(
        contexts=["_USER_POSTS"],
        _id=user_id,
        item_class="user",
        _type="items",
        **kwargs,
    )

    creator_post_earnings = 0
    creator_post_count = len(creator_posts)
    creator_post_periodicity = get_post_periodicity(posts=creator_posts)

    metrics = {
        "creator_post_count": creator_post_count,
        "creator_post_engagements": creator_post_engagements,
        "creator_post_earnings": creator_post_earnings,
        "creator_follower_count": creator_follower_count,
        "creator_has_avatar": 1 if user.get("apparelSettings", {}).get("avatar") else 0,
        "creator_is_ambassador": creator_is_ambassador,
        "creator_profile_complete": creator_profile_complete,
        "creator_post_frequency": safely_divide(1, creator_post_periodicity),
    }
    print("user metrics:", metrics)
    return metrics


def _read_scores(item_dict, fields):
    return [item_dict.get(field) or 0.0 for field in fields]


def get_polar_scores(
    aggregates: dict, read_fields: List[str], polar_fn: Callable
) -> List[float]:
    polar_scores = []
    for field in read_fields:
        all_scores = [aggregates[item_id].get(field) or 0.0 for item_id in aggregates]
        polar_score = polar_fn(all_scores)
        polar_scores.append(polar_score)
    return polar_scores


def transform_scores(item_scores: List[float], max_scores: List[float]) -> List[float]:
    """normalize item scores"""
    normalized_scores = []
    i = -1
    for score in item_scores:
        i += 1
        max_score = max_scores[i]
        normalized_score = safely_divide(score, max_score)
        normalized_scores.append(normalized_score)
    return normalized_scores


def interpolate_scores(
    item_scores: List[float],
    max_scores: List[float],
    min_scores: List[float],
    reducer_constant=None,
) -> List[float]:
    """interpolate item scores"""
    interpolated_scores = []
    i = -1
    for score in item_scores:
        i += 1
        max_score = max_scores[i]
        min_score = min_scores[i]
        interpolated_score = safely_divide((score - min_score), (max_score - score))
        if reducer_constant is not None:
            _score = interpolated_score / (interpolated_score + reducer_constant)
        else:
            _score = interpolated_score
        interpolated_scores.append(_score)
    return interpolated_scores


@push_signal(
    telemetry_type="build_network_scores",
    signal_name=f"network_scores_{gen_unique_id()}",
)
def build_network_scores(aggregates: dict, read_fields: Any) -> dict:
    """calculate the network scores for all items over a set of fields"""
    max_scores = get_polar_scores(aggregates, read_fields, polar_fn=safe_max)
    network_scores = {}
    for item_id in aggregates:
        item_scores = _read_scores(aggregates.get(item_id, {}), read_fields)
        network_scores[item_id] = transform_scores(item_scores, max_scores)
    return network_scores


@push_signal(
    telemetry_type="build_interpolated_network_scores",
    signal_name=f"interpolated_network_scores_{gen_unique_id()}",
)
def build_interpolated_network_scores(aggregates: dict, read_fields: Any) -> dict:
    """calculate the interpolated network scores for all items over a set of fields"""
    max_scores = get_polar_scores(aggregates, read_fields, polar_fn=safe_max)
    min_scores = get_polar_scores(aggregates, read_fields, polar_fn=safe_min)
    network_scores = {}
    for item_id in aggregates:
        item_scores = _read_scores(aggregates.get(item_id, {}), read_fields)
        network_scores[item_id] = interpolate_scores(
            item_scores,
            max_scores,
            min_scores,
            reducer_constant=ModelParameters.LINSPACE_INTERPOLATION_CONSTANT.value,
        )
    return network_scores


def pad_score(score: float) -> float:
    return ModelParameters.SCORE_ZERO_BUFFER.value + score


def _build_wps(item_class: str, scores: List[float], extra_score: float) -> float:
    weights = (
        ModelParameters.METRIC_WEIGHTS.value
        if item_class == "user"
        else ModelParameters.POST_METRIC_WEIGHTS.value
    )
    dataset = zip(scores, weights)
    wps = sum([pad_score(k) * v for k, v in dataset])
    final_score = wps if not extra_score else sqrt(wps * pad_score(extra_score))
    return final_score


def ensure_complete_data(result_item, check_fields):
    _item = result_item["item"]
    if _item:
        discard = False
        unwanted = ["", None, False]
        searchable_fields = [field for field in check_fields if field in _item]
        for field in searchable_fields:
            if _item[field] in unwanted:
                discard = True
                break
        return result_item if not discard else None
    else:
        return None


def search_by_id(
    input_data: List[dict], item_id: Union[ObjectId, str]
) -> Union[dict, None]:
    result = [item for item in input_data if item["_id"] == item_id]
    return result[0] if result else None


def serialize_obj_id_fields(item: dict) -> dict:
    serializable_fields = [field for field in item if field in OBJECT_ID_FIELDS]
    for field in serializable_fields:
        value = item[field]
        item[field] = str(value)
    return item


@push_signal(
    telemetry_type="build_result_item", signal_name=f"result_item_{gen_unique_id()}"
)
def build_result_item(item_id, item_class, item_scores, input_data, extra_score=None):

    check_fields = {
        "user": [
            "username",
            "avatar",
            "name",
            "isCreator",
            "profileCompleted",
        ],
        "post": ["contentId"],
    }

    item = search_by_id(input_data, item_id)

    result_item = {
        "wps": _build_wps(item_class, item_scores, extra_score),
        "item": serialize_obj_id_fields(item),
    }

    return ensure_complete_data(result_item, check_fields=check_fields)


@push_signal(
    telemetry_type="build_weight_product_sum",
    signal_name=f"weight_product_sum_{gen_unique_id()}",
)
def build_weight_product_sum(
    network_scores: dict, item_class: str, items: List[dict], args: Any
) -> List[Any]:
    """calculate weight-product sums for all items"""

    raw_result = [
        build_result_item(
            item_id=item_id,
            item_class=item_class,
            item_scores=item_scores,
            input_data=items,
            extra_score=(args.extra_scores or {}).get(item_id),
        )
        for item_id, item_scores in network_scores.items()
    ]
    return [result for result in raw_result if result]


def training_flow(item_class: str, items: List[dict], **kwargs) -> List[Any]:
    """training flow to return a list of recommended entities"""

    # this argument only exists during a local training run
    local_training_run = kwargs.get("local_training_run")

    # 1. collect metrics
    aggregator = aggregate_metrics if item_class == "user" else post_metrics
    metrics = {item["_id"]: aggregator(item, **kwargs) for item in items}

    # 2. discover and mark creators
    if item_class == "user":
        users_with_posts = [
            user for user in metrics if metrics[user]["creator_post_frequency"] > 0
        ]
        users_with_complete_profile = [
            user for user in metrics if metrics[user]["creator_profile_complete"]
        ]
        db_operation(
            args=kwargs,
            context="mark_creators",
            op_args=[users_with_posts, users_with_complete_profile],
        )

    # 3. build network scores
    metric_fields = (
        ModelParameters.METRIC_FIELDS.value
        if item_class == "user"
        else ModelParameters.POST_METRIC_FIELDS.value
    )
    network_scores = build_network_scores(aggregates=metrics, read_fields=metric_fields)

    if item_class == "post":
        post_recency_scores = build_interpolated_network_scores(
            aggregates=metrics, read_fields=["post_recency_factor"]
        )
        extra_scores = {
            post_id: post_scores[0]
            for post_id, post_scores in post_recency_scores.items()
        }
    else:
        extra_scores = None

    args = ArgsWrapper()
    kwargs["extra_scores"] = extra_scores
    args.build_from_dict(source=kwargs)

    # 3. build weight-product sum (WPS) and rank results
    ranked_results = build_weight_product_sum(network_scores, item_class, items, args)

    # 4. get recommendations (sort DESC by WPS rank)
    recommendations = sorted(ranked_results, key=lambda i: i["wps"], reverse=True)

    return recommendations


def ensure_list(item: Any) -> list:
    return item if isinstance(item, list) else [item]


def get_creators_from_db(
    required_size: int = ModelParameters.STAGING_SAMPLE_SIZE.value,
    creator_preferences: List[str] = None,
    existing_creators: list = [],
    country: str = None,
    **kwargs,
) -> List[Any]:

    preferences = ensure_list(creator_preferences or kwargs.get("preferences"))

    return db_operation(
        args=_force_dict(kwargs),
        context="filter_creators_by_preference",
        op_args=[preferences, existing_creators, required_size, country],
    )


def build_context_key(**kwargs):
    factor_list = kwargs.get("factor_list") or ["task_type", "preferences"]
    factors = {kwargs.get(factor) for factor in factor_list}
    return get_hash(factors)


def build_artifact(item_class: str, artifact_body: dict, **kwargs):
    global webserver
    context_key = build_context_key(**kwargs)
    artifact = dict()
    artifact[context_key] = artifact_body
    artifact_key = f"{item_class}_artifact_{context_key}"
    webserver.put_secret(secret_name=artifact_key, secret_value=artifact, as_pkg=True)


def retrieve_artifact(item_class: str, **kwargs):
    context_key = build_context_key(**kwargs)
    _artifact = webserver.retrieve_secret(
        secret_name=f"{item_class}_artifact_{context_key}",
        as_pkg=True,
        use_cache=True,
    )
    artifact = _artifact[0] if isinstance(_artifact, list) else None
    return artifact, context_key


def update_artifact(item_class: str, update: dict, **kwargs):
    artifact, context_key = retrieve_artifact(item_class=item_class, **kwargs)
    updated_artifact = (
        patch_data(item=artifact[context_key], patch=update)
        if artifact is not None
        else update
    )
    build_artifact(item_class=item_class, artifact_body=updated_artifact, **kwargs)


def read_artifact(contexts: Union[List[str], str], item_class: str, **kwargs):
    global webserver

    contexts = contexts if isinstance(contexts, list) else [contexts]

    artifact, context_key = retrieve_artifact(item_class=item_class, **kwargs)

    context_data = artifact[context_key] if isinstance(artifact, dict) else {}

    return (
        [context_data.get(context) for context in contexts]
        if contexts != ["*"]
        else context_data
    )


def read_telemetry_artifact(
    contexts: Union[List[str], str], item_class: str, experiment_date: str = get_today()
) -> Any:
    return read_artifact(
        contexts=contexts,
        item_class=f"telemetry::{item_class}",
        **dict(factor_list=["experiment_date"], experiment_date=experiment_date),
    )


def from_artifact(
    contexts: List[str],
    _id: ObjectId,
    item_class: str,
    _type: str,
    **kwargs,
):
    context_data = read_artifact(contexts=contexts, item_class=item_class, **kwargs)
    default = 0 if _type == "items_count" else []
    results = [
        (_data or {}).get(ObjectId(_id), {}).get(_type, default)
        for _data in context_data
    ]
    return results if len(results) > 1 else results[0]


def get_creator_posts(
    creator_batch_ids: List[str] = None,
    **kwargs,
) -> List[dict]:
    global webserver

    if creator_batch_ids:
        creator_posts = [get_user_posts(user_id=creator_batch_ids, **kwargs)]
    else:
        creator_posts = read_artifact(
            contexts=["_USER_POSTS"], item_class="user", **kwargs
        )

    return creator_posts


@push_signal(
    telemetry_type="create_user_artifact",
    signal_name=f"user_artifact_{gen_unique_id()}",
)
def create_user_artifacts(users: List[dict], **kwargs) -> List[str]:

    global webserver

    _USER_IDS = [user["_id"] for user in users]

    artifact_body = {
        "_USER_IDS": _USER_IDS,
        "_USER_COMMENTS": get_user_comments(user_id=_USER_IDS, **kwargs),
        "_USER_FOLLOWERS": get_user_followers(user_id=_USER_IDS, **kwargs),
        "_USER_POSTS": get_user_posts(user_id=_USER_IDS, **kwargs),
        "_USER_AMBASSADORS": get_user_ambassadors(user_id=_USER_IDS, **kwargs),
        "_USER_COMPLETE_PROFILE": get_user_complete_profile(
            user_id=_USER_IDS, **kwargs
        ),
    }

    build_artifact(item_class="user", artifact_body=artifact_body, **kwargs)

    return artifact_body


def delete_data_dir():
    shutil.rmtree(path=_DATA_DIR_, ignore_errors=True)


def build_artifacts(user_inputs, post_inputs, **kwargs):
    """generate model artifacts"""
    global webserver

    create_user_artifacts(user_inputs, **kwargs)
    create_post_artifacts(post_inputs, **kwargs)


@push_signal(
    telemetry_type="discovery_phase", signal_name=f"model_inputs_{gen_unique_id()}"
)
def create_initial_buckets(props: tuple, **kwargs) -> tuple:

    global webserver

    # find similar creators and posts based on shared properties

    similar_creators, similar_posts = create_shared_property_pool(
        use_property="preferences",
        props=props,
        include_posts=kwargs.get("post_context"),
        **kwargs,
    )

    return similar_creators, similar_posts


@push_signal(
    telemetry_type="training_process", signal_name=f"training_outputs_{gen_unique_id()}"
)
def training_process(user_inputs, post_inputs, **kwargs) -> dict:

    global webserver

    user_outputs = training_flow(item_class="user", items=user_inputs, **kwargs)
    post_outputs = training_flow(item_class="post", items=post_inputs, **kwargs)

    return {"user": user_outputs, "post": post_outputs}


def discovery_phase(args) -> tuple:
    """Source base data for model training"""

    preferences = args.preferences.split(",")
    country = args.country

    kwargs = _force_dict(args)
    props = (preferences, country)
    prop_fields = ("preferences", "country")

    for field in prop_fields:
        kwargs.pop(field)

    return create_initial_buckets(props=props, **kwargs)


def orchestrator_flow(kwargs):
    global webserver
    args = ArgsWrapper()
    args.build_from_dict(source=kwargs)
    user_inputs, post_inputs = discovery_phase(args)
    build_artifacts(user_inputs, post_inputs, **kwargs)
    return training_process(user_inputs, post_inputs, **kwargs)


def training_controller(
    username, country, preferences, post_context: bool = False, **kwargs
):
    preferences = clean_preference_list(preferences)

    # clear previous training data
    delete_data_dir()

    env = kwargs.get("env") or kwargs.get("environment")
    op_switches = {
        "env": env,
        "environment": env,
        "docker_mongo_ip": kwargs.get("local_mongo_ip"),
        "preferences": ",".join(preferences),
        "country": country,
        "username": username,
        "from_mongo": True,
        "post_context": False,
        "no_likes": False,
        "local_training_run": False,
        "secrets": SecretFileManager(),
        "post_limit": ModelParameters.POST_STAGING_SAMPLE_SIZE.value,
        "profile_limit": ModelParameters.STAGING_SAMPLE_SIZE.value,
        "live_run": True,
        "number_of_users": ModelParameters.NOMINAL_SHARED_INTEREST_POOL_SIZE.value,
    }
    if post_context:
        op_switches.update(
            {
                "post_context": True,
                "post_status": "published",
            }
        )
    no_likes = kwargs.get("no_likes")
    if no_likes:
        op_switches.update(
            {
                "no_likes": True,
            }
        )
    local_training_run = kwargs.get("local_training_run")
    if local_training_run:
        op_switches.update(
            {
                "local_training_run": True,
            }
        )
    return orchestrator_flow(op_switches)


def send_to_slack(text):
    webhook_url = "https://hooks.slack.com/services/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"
    headers = {"Content-Type": "application/json"}
    data = {"text": text}
    response = http.post(webhook_url, headers=headers, data=json.dumps(data))
    return response.status_code == 200
