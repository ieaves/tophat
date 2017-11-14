
def get_table_pk_name(table):
    """
    Returns a string representation of any given django models primary key name
    """
    return '_'.join([table._meta.model_name, table ._meta.pk.name])
