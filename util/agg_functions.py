from pyspark.sql.functions import last, sum, avg, count


def sums(col_keys : list[str]) -> list[sum]: 
    """Returns list of pyspawk.sql.functions.sum() for the given 
    column keys. Additionally sets the column's name to the col_key.
    Parameters
    ----------
    col_keys : list of column keys to return sum() of.
        Each element should be a column name (string) or an expression (:class:`Column`).
    Examples
    --------
    >>> *agf.sums(sums_keys)
    """
    return [sum(col_key).alias(col_key) for col_key in col_keys]

def lasts(col_keys : list[str]) -> list[last]:
    """Returns list of pyspawk.sql.functions.last() for the given 
    column keys. Additionally sets the column's name to the col_key.
    Parameters
    ----------
    col_keys : list of column keys to return last() of.
        Each element should be a column name (string) or an expression (:class:`Column`).
    Examples
    --------
    >>> *agf.lasts(lasts_keys)
    """
    return [last(col_key).alias(col_key) for col_key in col_keys]