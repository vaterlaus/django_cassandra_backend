#   Copyright 2010 BSN, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import time
from thrift.transport import TTransport
from django.db.utils import DatabaseError

def _cmp_to_key(comparison_function):
    """
    Convert a cmp= function into a key= function.
    This is built in to Python 2.7, but we define it ourselves
    to work with older versions of Python
    """
    class K(object):
        def __init__(self, obj, *args):
            self.obj = obj
        def __lt__(self, other):
            return comparison_function(self.obj, other.obj) < 0
        def __gt__(self, other):
            return comparison_function(self.obj, other.obj) > 0
        def __eq__(self, other):
            return comparison_function(self.obj, other.obj) == 0
        def __le__(self, other):
            return comparison_function(self.obj, other.obj) <= 0
        def __ge__(self, other):
            return comparison_function(self.obj, other.obj) >= 0
        def __ne__(self, other):
            return comparison_function(self.obj, other.obj) != 0
    return K

def _compare_rows(row1, row2, sort_spec_list):
    for sort_spec in sort_spec_list:
        column_name = sort_spec[0]
        reverse = sort_spec[1] if len(sort_spec) > 1 else False
        row1_value = row1.get(column_name, None)
        row2_value = row2.get(column_name, None)
        result = cmp(row1_value, row2_value)
        if result != 0:
            if reverse:
                result = -result
            break;
    else:
        result = 0
    return result

def sort_rows(rows, sort_spec):
    if sort_spec == None:
        return rows

    if (type(sort_spec) != list) and (type(sort_spec) != tuple):
        raise InvalidSortSpecException()
    
    # The sort spec can be either a single sort spec tuple or a list/tuple
    # of sort spec tuple. To simplify the code below we convert the case
    # where it's a single sort spec tuple to a 1-element tuple containing
    # the sort spec tuple here.
    if (type(sort_spec[0]) == list) or (type(sort_spec[0]) == tuple):
        sort_spec_list = sort_spec
    else:
        sort_spec_list = (sort_spec,)
    
    rows.sort(key=_cmp_to_key(lambda row1, row2: _compare_rows(row1, row2, sort_spec_list)))

COMBINE_INTERSECTION = 1
COMBINE_UNION = 2

def combine_rows(rows1, rows2, op, primary_key_column):
    # Handle cases where rows1 and/or rows2 are None or empty
    if not rows1:
        return list(rows2) if rows2 and (op == COMBINE_UNION) else []
    if not rows2:
        return list(rows1) if (op == COMBINE_UNION) else []
    
    # We're going to iterate over the lists in parallel and
    # compare the elements so we need both lists to be sorted
    # Note that this means that the input arguments will be modified.
    # We could optionally clone the rows first, but then we'd incur
    # the overhead of the copy. For now, we'll just always sort
    # in place, and if it turns out to be a problem we can add the
    # option to copy
    sort_rows(rows1,(primary_key_column,))
    sort_rows(rows2,(primary_key_column,))
    
    combined_rows = []
    iter1 = iter(rows1)
    iter2 = iter(rows2)
    update1 = update2 = True
    
    while True:
        # Get the next element from one or both of the lists
        if update1:
            try:
                row1 = iter1.next()
            except:
                row1 = None
            value1 = row1.get(primary_key_column, None) if row1 != None else None
        if update2:
            try:
                row2 = iter2.next()
            except:
                row2 = None
            value2 = row2.get(primary_key_column, None) if row2 != None else None
        
        if (op == COMBINE_INTERSECTION):
            # If we've reached the end of either list and we're doing an intersection,
            # then we're done
            if (row1 == None) or (row2 == None):
                break
        
            if value1 == value2:
                combined_rows.append(row1)
        elif (op == COMBINE_UNION):
            if row1 == None:
                if row2 == None:
                    break;
                combined_rows.append(row2)
            elif (row2 == None) or (value1 <= value2):
                combined_rows.append(row1)
            else:
                combined_rows.append(row2)
        else:
            raise InvalidCombineRowsOpException()
        
        update1 = (row2 == None) or (value1 <= value2)
        update2 = (row1 == None) or (value2 <= value1)
    
    return combined_rows

_last_time = None
_last_counter = None
    
def get_next_timestamp():
    # The timestamp is a 64-bit integer
    # We use the top 44 bits for the current time in milliseconds since the
    # epoch. The low 20 bits are a counter that is incremented if the current
    # time value from the top 44 bits is the same as the last
    # time value. This guarantees that a new timestamp is always
    # greater than the previous timestamp
    global _last_time, _last_counter
    current_time = int(time.time() * 1000)
    
    if (_last_time == None) or (current_time > _last_time):
        _last_time = current_time
        _last_counter = 0
    else:
        _last_counter += 1
    
    return _last_time * 0x100000 + _last_counter

def convert_string_to_list(s):
    # FIXME: Shouldn't use eval here, because of security considerations
    # (i.e. if someone could modify the data in Cassandra they could
    # insert arbitrary Python code that would then get evaluated on
    # the client machine. Should have code that parses the list string
    # to construct the list or else validate the string before calling eval.
    # But for now, during development, we'll just use the quick & dirty eval.
    return eval(s)

def convert_list_to_string(l):
    return unicode(l)


class CassandraConnectionError(DatabaseError):
    def __init__(self, message=None):
        msg = 'Error connecting to Cassandra database'
        if message:
            msg += '; ' + str(message)
        super(CassandraConnectionError,self).__init__(msg)


class CassandraAccessError(DatabaseError):
    def __init__(self, message=None):
        msg = 'Error accessing Cassandra database'
        if message:
            msg += '; ' + str(message)
        super(CassandraAccessError,self).__init__(msg)


def call_cassandra_with_reconnect(connection, fn, *args, **kwargs):
    try:
        try:
            results = fn(*args, **kwargs)
        except TTransport.TTransportException:
            connection.reopen()
            results = fn(*args, **kwargs)
    except TTransport.TTransportException, e:
        raise CassandraConnectionError(e)
    except Exception, e:
        raise CassandraAccessError(e)

    return results


