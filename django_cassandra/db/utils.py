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
from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from cassandra import Cassandra
#from cassandra.ttypes import *
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

_last_timestamp = None
    
def get_next_timestamp():
    # The timestamp is a 64-bit integer
    # We now use the standard Cassandra timestamp format of the
    # current system time in microseconds. We also keep track of the
    # last timestamp we returned and if the current time is less than
    # that, then we just advance the timestamp by 1 to make sure we
    # return monotonically increasing timestamps. Note that this isn't
    # guaranteed to handle the fairly common Django deployment model of
    # having multiple Django processes that are dispatched to from a
    # web server like Apache. In practice I don't think that case will be
    # a problem though (at least with current hardware) because I don't
    # think you could have two consecutive calls to Django from another
    # process that would be dispatched to two different Django processes
    # that would happen in the same microsecond.

    global _last_timestamp

    timestamp = int(time.time() * 1000000)
    
    if (_last_timestamp != None) and (timestamp <= _last_timestamp):
        timestamp = _last_timestamp + 1

    _last_timestamp = timestamp
    
    return timestamp

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


class CassandraConnection(object):
    def __init__(self, host, port, keyspace, user, password):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.user = user
        self.password = password
        self.transport = None
        self.client = None
        self.keyspace_set = False
        self.logged_in = False
        
    def commit(self):
        pass

    def set_keyspace(self):
        if not self.keyspace_set:
            try:
                if self.client:
                    self.client.set_keyspace(self.keyspace)
                    self.keyspace_set = True
            except Exception, e:
                # In this case we won't have set keyspace_set to true, so we'll throw the
                # exception below where it also handles the case that self.client
                # is not valid yet.
                pass
            if not self.keyspace_set:
                raise DatabaseError('Error setting keyspace: %s; %s' % (self.keyspace, str(e)))
    
    def login(self):
        # TODO: This user/password auth code hasn't been tested
        if not self.logged_in:
            if self.user:
                try:
                    if self.client:
                        credentials = {'username': self.user, 'password': self.password}
                        self.client.login(AuthenticationRequest(credentials))
                        self.logged_in = True
                except Exception, e:
                    # In this case we won't have set logged_in to true, so we'll throw the
                    # exception below where it also handles the case that self.client
                    # is not valid yet.
                    pass
                if not self.logged_in:
                    raise DatabaseError('Error logging in to keyspace: %s; %s' % (self.keyspace, str(e)))
            else:
                self.logged_in = True
            
    def open(self, set_keyspace=False, login=False):
        if self.transport == None:
            # Create the client connection to the Cassandra daemon
            socket = TSocket.TSocket(self.host, int(self.port))
            transport = TTransport.TFramedTransport(TTransport.TBufferedTransport(socket))
            protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
            transport.open()
            self.transport = transport
            self.client = Cassandra.Client(protocol)
            
        if login:
            self.login()
        
        if set_keyspace:
            self.set_keyspace()
                
    def close(self):
        if self.transport != None:
            try:
                self.transport.close()
            except Exception, e:
                pass
            self.transport = None
            self.client = None
            self.keyspace_set = False
            self.logged_in = False
            
    def is_connected(self):
        return self.transport != None
    
    def get_client(self):
        if self.client == None:
            self.open(True, True)
        return self.client
    
    def reopen(self):
        self.close()
        self.open(True, True)
            

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
            results = fn(connection.get_client(), *args, **kwargs)
        except TTransport.TTransportException:
            connection.reopen()
            results = fn(connection.get_client(), *args, **kwargs)
    except TTransport.TTransportException, e:
        raise CassandraConnectionError(e)
    except Exception, e:
        raise CassandraAccessError(e)

    return results


