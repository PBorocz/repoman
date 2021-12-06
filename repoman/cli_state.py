from pathlib import Path

import nestedtext as nt

import constants as c
from utils import AnonymousObj


def get_state(command: str) -> AnonymousObj:
    path_ = get_state_path(command)
    state = c.DEFAULTS[command]  # Default if file doesn't exist or is corrupt.
    if path_.exists():
        try:
            state = nt.load(path_, top='dict')
        except nt.NestedTextError as e:
            ...
    return AnonymousObj(**state)


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
