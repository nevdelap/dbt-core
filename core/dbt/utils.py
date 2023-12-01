import collections
import datetime
import decimal
import functools
import itertools
import jinja2
import json
import os
import requests
from tarfile import ReadError
import time
from pathlib import PosixPath, WindowsPath

from dbt.common.utils import md5
from dbt.common.events.types import RetryExternalCall, RecordRetryException
from dbt.common.exceptions import (
    DbtInternalError,
    RecursionError,
)
from dbt.exceptions import ConnectionError, DuplicateAliasError
from dbt.common.helper_types import WarnErrorOptions
from dbt import flags
from enum import Enum
from typing import (
    Tuple,
    Type,
    Any,
    Optional,
    Dict,
    List,
    Iterator,
    Mapping,
    Iterable,
    AbstractSet,
    Set,
    Sequence,
)

DECIMALS: Tuple[Type[Any], ...]
try:
    import cdecimal  # typing: ignore
except ImportError:
    DECIMALS = (decimal.Decimal,)
else:
    DECIMALS = (decimal.Decimal, cdecimal.Decimal)


class ExitCodes(int, Enum):
    Success = 0
    ModelError = 1
    UnhandledError = 2


def coalesce(*args):
    for arg in args:
        if arg is not None:
            return arg
    return None


def get_profile_from_project(project):
    target_name = project.get("target", {})
    profile = project.get("outputs", {}).get(target_name, {})
    return profile


def get_model_name_or_none(model):
    if model is None:
        name = "<None>"

    elif isinstance(model, str):
        name = model
    elif isinstance(model, dict):
        name = model.get("alias", model.get("name"))
    elif hasattr(model, "alias"):
        name = model.alias
    elif hasattr(model, "name"):
        name = model.name
    else:
        name = str(model)
    return name


MACRO_PREFIX = "dbt_macro__"
DOCS_PREFIX = "dbt_docs__"


def get_dbt_macro_name(name):
    if name is None:
        raise DbtInternalError("Got None for a macro name!")
    return f"{MACRO_PREFIX}{name}"


def get_dbt_docs_name(name):
    if name is None:
        raise DbtInternalError("Got None for a doc name!")
    return f"{DOCS_PREFIX}{name}"


def get_materialization_macro_name(materialization_name, adapter_type=None, with_prefix=True):
    if adapter_type is None:
        adapter_type = "default"
    name = f"materialization_{materialization_name}_{adapter_type}"
    return get_dbt_macro_name(name) if with_prefix else name


def get_docs_macro_name(docs_name, with_prefix=True):
    return get_dbt_docs_name(docs_name) if with_prefix else docs_name


def get_test_macro_name(test_name, with_prefix=True):
    name = f"test_{test_name}"
    return get_dbt_macro_name(name) if with_prefix else name


def split_path(path):
    return path.split(os.sep)


def get_pseudo_test_path(node_name, source_path):
    "schema tests all come from schema.yml files. fake a source sql file"
    source_path_parts = split_path(source_path)
    source_path_parts.pop()  # ignore filename
    suffix = ["{}.sql".format(node_name)]
    pseudo_path_parts = source_path_parts + suffix
    return os.path.join(*pseudo_path_parts)


def get_pseudo_hook_path(hook_name):
    path_parts = ["hooks", "{}.sql".format(hook_name)]
    return os.path.join(*path_parts)


def get_hash(model):
    return md5(model.unique_id)


def get_hashed_contents(model):
    return md5(model.raw_code)


def flatten_nodes(dep_list):
    return list(itertools.chain.from_iterable(dep_list))


class memoized:
    """Decorator. Caches a function's return value each time it is called. If
    called later with the same arguments, the cached value is returned (not
    reevaluated).

    Taken from https://wiki.python.org/moin/PythonDecoratorLibrary#Memoize"""

    def __init__(self, func) -> None:
        self.func = func
        self.cache: Dict[Any, Any] = {}

    def __call__(self, *args):
        if not isinstance(args, collections.abc.Hashable):
            # uncacheable. a list, for instance.
            # better to not cache than blow up.
            return self.func(*args)
        if args in self.cache:
            return self.cache[args]
        value = self.func(*args)
        self.cache[args] = value
        return value

    def __repr__(self):
        """Return the function's docstring."""
        return self.func.__doc__

    def __get__(self, obj, objtype):
        """Support instance methods."""
        return functools.partial(self.__call__, obj)


def add_ephemeral_model_prefix(s: str) -> str:
    return "__dbt__cte__{}".format(s)


def timestring() -> str:
    """Get the current datetime as an RFC 3339-compliant string"""
    # isoformat doesn't include the mandatory trailing 'Z' for UTC.
    return datetime.datetime.utcnow().isoformat() + "Z"


def humanize_execution_time(execution_time: int) -> str:
    minutes, seconds = divmod(execution_time, 60)
    hours, minutes = divmod(minutes, 60)

    return f" in {int(hours)} hours {int(minutes)} minutes and {seconds:0.2f} seconds"


class JSONEncoder(json.JSONEncoder):
    """A 'custom' json encoder that does normal json encoder things, but also
    handles `Decimal`s and `Undefined`s. Decimals can lose precision because
    they get converted to floats. Undefined's are serialized to an empty string
    """

    def default(self, obj):
        if isinstance(obj, DECIMALS):
            return float(obj)
        elif isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        elif isinstance(obj, jinja2.Undefined):
            return ""
        elif isinstance(obj, Exception):
            return repr(obj)
        elif hasattr(obj, "to_dict"):
            # if we have a to_dict we should try to serialize the result of
            # that!
            return obj.to_dict(omit_none=True)
        else:
            return super().default(obj)


class Translator:
    def __init__(self, aliases: Mapping[str, str], recursive: bool = False) -> None:
        self.aliases = aliases
        self.recursive = recursive

    def translate_mapping(self, kwargs: Mapping[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        for key, value in kwargs.items():
            canonical_key = self.aliases.get(key, key)
            if canonical_key in result:
                raise DuplicateAliasError(kwargs, self.aliases, canonical_key)
            result[canonical_key] = self.translate_value(value)
        return result

    def translate_sequence(self, value: Sequence[Any]) -> List[Any]:
        return [self.translate_value(v) for v in value]

    def translate_value(self, value: Any) -> Any:
        if self.recursive:
            if isinstance(value, Mapping):
                return self.translate_mapping(value)
            elif isinstance(value, (list, tuple)):
                return self.translate_sequence(value)
        return value

    def translate(self, value: Mapping[str, Any]) -> Dict[str, Any]:
        try:
            return self.translate_mapping(value)
        except RuntimeError as exc:
            if "maximum recursion depth exceeded" in str(exc):
                raise RecursionError("Cycle detected in a value passed to translate!")
            raise


def translate_aliases(
    kwargs: Dict[str, Any],
    aliases: Dict[str, str],
    recurse: bool = False,
) -> Dict[str, Any]:
    """Given a dict of keyword arguments and a dict mapping aliases to their
    canonical values, canonicalize the keys in the kwargs dict.

    If recurse is True, perform this operation recursively.

    :returns: A dict containing all the values in kwargs referenced by their
        canonical key.
    :raises: `AliasError`, if a canonical key is defined more than once.
    """
    translator = Translator(aliases, recurse)
    return translator.translate(kwargs)


# Note that this only affects hologram json validation.
# It has no effect on mashumaro serialization.
# Q: Can this be removed?
def restrict_to(*restrictions):
    """Create the metadata for a restricted dataclass field"""
    return {"restrict": list(restrictions)}


def coerce_dict_str(value: Any) -> Optional[Dict[str, Any]]:
    """For annoying mypy reasons, this helper makes dealing with nested dicts
    easier. You get either `None` if it's not a Dict[str, Any], or the
    Dict[str, Any] you expected (to pass it to dbtClassMixin.from_dict(...)).
    """
    if isinstance(value, dict) and all(isinstance(k, str) for k in value):
        return value
    else:
        return None


def _coerce_decimal(value):
    if isinstance(value, DECIMALS):
        return float(value)
    return value


def fqn_search(root: Dict[str, Any], fqn: List[str]) -> Iterator[Dict[str, Any]]:
    """Iterate into a nested dictionary, looking for keys in the fqn as levels.
    Yield the level config.
    """
    yield root

    for level in fqn:
        level_config = root.get(level, None)
        if not isinstance(level_config, dict):
            break
        # This used to do a 'deepcopy',
        # but it didn't seem to be necessary
        yield level_config
        root = level_config


StringMap = Mapping[str, Any]
StringMapList = List[StringMap]
StringMapIter = Iterable[StringMap]


class MultiDict(Mapping[str, Any]):
    """Implement the mapping protocol using a list of mappings. The most
    recently added mapping "wins".
    """

    def __init__(self, sources: Optional[StringMapList] = None) -> None:
        super().__init__()
        self.sources: StringMapList

        if sources is None:
            self.sources = []
        else:
            self.sources = sources

    def add_from(self, sources: StringMapIter):
        self.sources.extend(sources)

    def add(self, source: StringMap):
        self.sources.append(source)

    def _keyset(self) -> AbstractSet[str]:
        # return the set of keys
        keys: Set[str] = set()
        for entry in self._itersource():
            keys.update(entry)
        return keys

    def _itersource(self) -> StringMapIter:
        return reversed(self.sources)

    def __iter__(self) -> Iterator[str]:
        # we need to avoid duplicate keys
        return iter(self._keyset())

    def __len__(self):
        return len(self._keyset())

    def __getitem__(self, name: str) -> Any:
        for entry in self._itersource():
            if name in entry:
                return entry[name]
        raise KeyError(name)

    def __contains__(self, name) -> bool:
        return any((name in entry for entry in self._itersource()))


def _connection_exception_retry(fn, max_attempts: int, attempt: int = 0):
    """Attempts to run a function that makes an external call, if the call fails
    on a Requests exception or decompression issue (ReadError), it will be tried
    up to 5 more times.  All exceptions that Requests explicitly raises inherit from
    requests.exceptions.RequestException.  See https://github.com/dbt-labs/dbt-core/issues/4579
    for context on this decompression issues specifically.
    """
    try:
        return fn()
    except (
        requests.exceptions.RequestException,
        ReadError,
        EOFError,
    ) as exc:
        if attempt <= max_attempts - 1:
            # This import needs to be inline to avoid circular dependency
            from dbt.common.events.functions import fire_event

            fire_event(RecordRetryException(exc=str(exc)))
            fire_event(RetryExternalCall(attempt=attempt, max=max_attempts))
            time.sleep(1)
            return _connection_exception_retry(fn, max_attempts, attempt + 1)
        else:
            raise ConnectionError("External connection exception occurred: " + str(exc))


# This is used to serialize the args in the run_results and in the logs.
# We do this separately because there are a few fields that don't serialize,
# i.e. PosixPath, WindowsPath, and types. It also includes args from both
# cli args and flags, which is more complete than just the cli args.
# If new args are added that are false by default (particularly in the
# global options) they should be added to the 'default_false_keys' list.
def args_to_dict(args):
    var_args = vars(args).copy()
    # update the args with the flags, which could also come from environment
    # variables or user_config
    flag_dict = flags.get_flag_dict()
    var_args.update(flag_dict)
    dict_args = {}
    # remove args keys that clutter up the dictionary
    for key in var_args:
        if key.lower() in var_args and key == key.upper():
            # skip all capped keys being introduced by Flags in dbt.cli.flags
            continue
        if key in ["cls", "mp_context"]:
            continue
        if var_args[key] is None:
            continue
        # TODO: add more default_false_keys
        default_false_keys = (
            "debug",
            "full_refresh",
            "fail_fast",
            "warn_error",
            "single_threaded",
            "log_cache_events",
            "store_failures",
            "use_experimental_parser",
        )
        default_empty_yaml_dict_keys = ("vars", "warn_error_options")
        if key in default_false_keys and var_args[key] is False:
            continue
        if key in default_empty_yaml_dict_keys and var_args[key] == "{}":
            continue
        # this was required for a test case
        if isinstance(var_args[key], PosixPath) or isinstance(var_args[key], WindowsPath):
            var_args[key] = str(var_args[key])
        if isinstance(var_args[key], WarnErrorOptions):
            var_args[key] = var_args[key].to_dict()

        dict_args[key] = var_args[key]
    return dict_args
