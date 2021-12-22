from pathlib import Path

import nestedtext as nt

import constants as c
from utils import AnonymousObj
from adts import *

STATE_TYPES = dict(
    index = IndexCommandParameters,
    query = QueryCommandParameters,
)

def get_state(command: str) -> AnonymousObj:
    type_ = STATE_TYPES[command]
    path_ = get_state_path(command)
    if path_.exists:
        state = nt.load(path_, top='dict')
        return type_(**state)
    return type_()        # Pick up default values from ADT definition


def save_state(command: str, state: AnonymousObj) -> bool:
    path_ = get_state_path(command)
    try:
        nt.dump(state.__dict__, path_)
    except nt.NestedTextError as err:
        err.terminate()
        return False
    return True


def get_state_path(command):
    return c.REPOMAN_PATH / Path(f"{c.STATE_ROOT}.{command}")
