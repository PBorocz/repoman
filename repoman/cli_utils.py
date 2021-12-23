from pathlib import Path
from typing import Any

import nestedtext as nt
from prompt_toolkit.validation import Validator, ValidationError

import constants as c
from utils import AnonymousObj
from adts import SortOrderChoices, IndexCommandParameters, QueryCommandParameters

################################################################################
# CLI State Management
################################################################################
STATE_TYPES = dict(
    index = IndexCommandParameters,
    query = QueryCommandParameters,
)

def get_state(command: str) -> AnonymousObj:
    type_ = STATE_TYPES[command]
    path_ = _get_state_path(command)
    if path_.exists:
        state = nt.load(path_, top='dict')
        return type_(**state)
    return type_()        # Pick up default values from ADT definition


def save_state(command: str, state: AnonymousObj) -> bool:
    path_ = _get_state_path(command)
    try:
        nt.dump(state.__dict__, path_)
    except nt.NestedTextError as err:
        err.terminate()
        return False
    return True


def update_state(command: str, attribute: str, new_value: Any) -> None:
    """Wrapper to just update a single attribute within a CLI state"""
    state = get_state(command)
    setattr(state, attribute, new_value)
    save_state(command, state)


def _get_state_path(command):
    return c.REPOMAN_PATH / Path(f"{c.STATE_ROOT}.{command}")


################################################################################
# CLI interactive data validators
################################################################################
class PathValidator(Validator):
    def validate(self, document):
        path = Path(document.text)
        if not path.exists():
            raise ValidationError(message="Sorry, this path doesn't exist")
        if not path.is_dir():
            raise ValidationError(message="Sorry, this path isn't a directory")


class YesNoValidator(Validator):
    def validate(self, document):
        if document.text:
            text = document.text.lower()
            if not text.startswith('y') and not text.startswith('n'):
                raise ValidationError(message=f"Sorry, must be either y(es) or n(o)")


class SortOrderValidator(Validator):
    def validate(self, document):
        try:
            SortOrderChoices[document.text.lower().replace("-", "")]
        except KeyError:
            raise ValidationError(message=f"Sorry, only valid sort order entries are: {SORT_ORDER_CHOICES}")


class IntValidator(Validator):
    def validate(self, document):
        try:
            if document.text:   # We allow empty here...
                int(document.text)
        except ValueError:
            raise ValidationError(message=f"Sorry, Top-N must be an integer or empty")
