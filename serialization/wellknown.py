"""
wellknown.py

Well-known types that are not part of the core RPC types.
"""


from serialization.derived import create_struct_type, create_union_type, String
from typing import Final


# Void type -> 0 size, empty structure.
Void: Final = create_struct_type("Void", ())

# Union, Void or an error string. Useful for reporting errors.
VoidOrError: Final = create_union_type(
    "VoidOrError", (("void", Void), ("error", String))
)
