def contains(dictionary, key, value):
    """This function returns True if the 'key' is in 'dictionary' and the collection
    associated to such key contains also 'value', otherwise it returns False.

    Args:
        dictionary (dict): the target dictionary
        key (str): the key to retrieve
        value (any): the values to be checked

    Returns:
        bool: True if the 'key' is in 'dictionary' and the collection associated to such key contains
        also 'value', otherwise it returns False
    """
    field = None
    if dictionary:
        field = dictionary.get(key)
    return field and value in field
