from dagster import check
from dagster.builtins import BuiltinEnum
from dagster.config.config_type import ConfigType
from dagster.core.errors import DagsterInvalidDefinitionError
from dagster.primitive_mapping import (
    is_supported_runtime_python_builtin,
    remap_python_builtin_for_runtime,
)
from dagster.utils.typing_api import is_typing_type

from .dagster_type import (
    Any,
    DagsterType,
    List,
    get_dagster_type_for_mapped_python_type,
    is_type_mapped,
)
from .python_dict import Dict, PythonDict
from .python_set import PythonSet, Set
from .python_tuple import PythonTuple, Tuple
from .transform_typing import transform_typing_type

DAGSTER_INVALID_TYPE_ERROR_MESSAGE = (
    'Invalid type: dagster_type must be a Python type, a type constructed using '
    'python.typing, a type imported from the dagster module, or a class annotated using '
    'as_dagster_type or @map_to_dagster_type: got {dagster_type}{additional_msg}'
)


def resolve_dagster_type(dagster_type):
    # circular dep

    check.invariant(
        not (isinstance(dagster_type, type) and issubclass(dagster_type, ConfigType)),
        'Cannot resolve a config type to a runtime type',
    )

    check.invariant(
        not (isinstance(dagster_type, type) and issubclass(dagster_type, DagsterType)),
        'Do not pass runtime type classes. Got {}'.format(dagster_type),
    )

    # First check to see if it part of python's typing library
    if is_typing_type(dagster_type):
        dagster_type = transform_typing_type(dagster_type)

    if isinstance(dagster_type, DagsterType):
        return dagster_type

    # Test for unhashable objects -- this is if, for instance, someone has passed us an instance of
    # a dict where they meant to pass dict or Dict, etc.
    try:
        hash(dagster_type)
    except TypeError:
        raise DagsterInvalidDefinitionError(
            DAGSTER_INVALID_TYPE_ERROR_MESSAGE.format(
                additional_msg=(
                    ', which isn\'t hashable. Did you pass an instance of a type instead of '
                    'the type?'
                ),
                dagster_type=repr(dagster_type),
            )
        )

    if is_supported_runtime_python_builtin(dagster_type):
        return remap_python_builtin_for_runtime(dagster_type)

    if dagster_type is None:
        return Any

    if is_type_mapped(dagster_type):
        return get_dagster_type_for_mapped_python_type(dagster_type)

    if dagster_type is Dict:
        return PythonDict
    if dagster_type is Tuple:
        return PythonTuple
    if dagster_type is Set:
        return PythonSet
    if dagster_type is List:
        return List(Any)
    if BuiltinEnum.contains(dagster_type):
        return DagsterType.from_builtin_enum(dagster_type)

    raise DagsterInvalidDefinitionError(
        DAGSTER_INVALID_TYPE_ERROR_MESSAGE.format(
            dagster_type=repr(dagster_type), additional_msg=''
        )
    )
