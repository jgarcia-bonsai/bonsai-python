"""
This script defines utility functions used by the bonsai sdk package.
"""
import uuid


def generate_guid():
    """
    Generates a GUID and returns it as a string appropriate for use in
    an ID
    Returns:
        A GUID cast to a string with dashes replaced with underscores.
    """
    id = uuid.uuid4()
    id_as_string = str(id).replace('-', '_')
    return id_as_string
