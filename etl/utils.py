import numpy as np
import psycopg2

from psycopg2.extensions import register_adapter, AsIs

### type adapters for python types --> postgres
def adapt_numpy_float64(numpy_float64):
    if np.isnan(numpy_float64):
        return psycopg2.extensions.AsIs('NULL')
    return AsIs(numpy_float64)
def adapt_numpy_int64(numpy_int64):
    if np.isnan(numpy_int64):
        return psycopg2.extensions.AsIs('NULL')
    return AsIs(numpy_int64)
def adapt_numpy_bool(numpy_bool):
    if np.isnan(numpy_bool):
        return psycopg2.extensions.AsIs('NULL')
    if numpy_bool:
        return psycopg2.extensions.AsIs('True')
    return psycopg2.extensions.AsIs('False')

def adapt_set(s):
    if not len(s):
        return psycopg2.extensions.AsIs("'{}'")
    return psycopg2.extensions.QuotedString(str(s))

