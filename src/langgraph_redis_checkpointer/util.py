
EMPTY_STRING_SENTINEL = "__empty__"
EMPTY_ID_SENTINEL = "00000000-0000-0000-0000-000000000000"


def to_safe_str(value: str) -> str:
    """
    Prepare a value for storage in Redis as a string.

    Convert an empty string to a sentinel value, otherwise return the
    value as a string.

    Args:
        value (str): The value to convert.

    Returns:
        str: The converted value.
    """
    if value == "":
        return EMPTY_STRING_SENTINEL
    else:
        return str(value)


def from_safe_str(value: str) -> str:
    """
    Convert a value from a sentinel value to an empty string if present,
    otherwise return the value unchanged.

    Args:
        value (str): The value to convert.

    Returns:
        str: The converted value.
    """
    if value == EMPTY_STRING_SENTINEL:
        return ""
    else:
        return value


def to_safe_id(value: str) -> str:
    """
    Prepare a value for storage in Redis as an ID.

    Convert an empty string to a sentinel value for empty ID strings, otherwise
    return the value as a string.

    Args:
        value (str): The value to convert.

    Returns:
        str: The converted value.
    """
    if value == "":
        return EMPTY_ID_SENTINEL
    else:
        return str(value)


def from_safe_id(value: str) -> str:
    """
    Convert a value from a sentinel value for empty ID strings to an empty
    ID string if present, otherwise return the value unchanged.

    Args:
        value (str): The value to convert.

    Returns:
        str: The converted value.
    """
    if value == EMPTY_ID_SENTINEL:
        return ""
    else:
        return value
