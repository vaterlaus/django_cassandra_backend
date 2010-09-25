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

from djangotoolbox.db.base import NonrelDatabaseFeatures, \
    NonrelDatabaseOperations, NonrelDatabaseWrapper, NonrelDatabaseClient, \
    NonrelDatabaseValidation, NonrelDatabaseIntrospection, \
    NonrelDatabaseCreation

from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from cassandra import Cassandra
from cassandra.ttypes import *
import time
from .creation import DatabaseCreation
from .introspection import DatabaseIntrospection

class DatabaseFeatures(NonrelDatabaseFeatures):
    string_based_auto_field = True

class DatabaseOperations(NonrelDatabaseOperations):
    compiler_module = __name__.rsplit('.', 1)[0] + '.compiler'
    
    def sql_flush(self, style, tables, sequence_list):
        for table_name in tables:
            self.connection.creation.flush_table(table_name)
        return ""
    
class DatabaseClient(NonrelDatabaseClient):
    pass

class DatabaseValidation(NonrelDatabaseValidation):
    pass

# TODO: Maybe move this somewhere else? db.utils.py maybe?
class CassandraConnection(object):
    def __init__(self, client, transport):
        self.client = client
        self.transport = transport
    
    def commit(self):
        pass

    def open(self):
        if self.transport:
            self.transport.open()
            
    def close(self):
        if self.transport:
            self.transport.close()
        
class DatabaseWrapper(NonrelDatabaseWrapper):
    def __init__(self, *args, **kwds):
        super(DatabaseWrapper, self).__init__(*args, **kwds)
        
        # Set up the associated backend objects
        self.features = DatabaseFeatures(self)
        self.ops = DatabaseOperations(self)
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.validation = DatabaseValidation(self)
        self.introspection = DatabaseIntrospection(self)

        # Get the host and port specified in the database backend settings.
        # Default to the standard Cassandra settings.
        host = self.settings_dict.get('HOST')
        if not host or host == '':
            host = 'localhost'
        port = self.settings_dict.get('PORT')
        if not port or port == '':
            port = 9160
        
        # Create the client connection to the Cassandra daemon
        socket = TSocket.TSocket(host, port)
        transport = TTransport.TFramedTransport(TTransport.TBufferedTransport(socket))
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
        client = Cassandra.Client(protocol)
        
        # Create our connection wrapper
        self.db_connection = CassandraConnection(client, transport)
        self.db_connection.open()

        version = client.describe_version()
        # FIXME: Should do some version check here to make sure that we're
        # talking to a cassandra daemon that supports the operations we require
        
        # Set up the Cassandra keyspace
        keyspace_name = self.settings_dict.get('NAME')
        self.creation.init_keyspace(keyspace_name)

