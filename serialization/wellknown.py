"""
wellknown.py

Well-known types that are not part of the core RPC types.
"""


from serialization.derived import create_struct_type
from typing import Final


# Void type -> 0 size, empty structure.
Void: Final = create_struct_type("Void", ())

