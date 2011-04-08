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
            self.transport.close()
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

        self.read_consistency_level = self.settings_dict.get('CASSANDRA_READ_CONSISTENCY_LEVEL', ConsistencyLevel.ONE)
        self.write_consistency_level = self.settings_dict.get('CASSANDRA_WRITE_CONSISTENCY_LEVEL', ConsistencyLevel.ONE)
        self.max_key_count = self.settings_dict.get('CASSANDRA_MAX_KEY_COUNT', 10000)
        self.max_column_count = self.settings_dict.get('CASSANDRA_MAX_COLUMN_COUNT', 1000)
        self.column_family_def_defaults = self.settings_dict.get('CASSANDRA_COLUMN_FAMILY_DEF_DEFAULT_SETTINGS', {})

        self._db_connection = None

    def get_db_connection(self, set_keyspace=False, login=False):
        if not self._db_connection:
            # Get the host and port specified in the database backend settings.
            # Default to the standard Cassandra settings.
            host = self.settings_dict.get('HOST')
            if not host or host == '':
                host = 'localhost'
                
            port = self.settings_dict.get('PORT')
            if not port or port == '':
                port = 9160
                
            keyspace = self.settings_dict.get('NAME')
            if keyspace == None:
                keyspace = 'django'
                
            user = self.settings_dict.get('USER')
            password = self.settings_dict.get('PASSWORD')
            
            # Create our connection wrapper
            self._db_connection = CassandraConnection(host, port, keyspace, user, password)
        
        if not self._db_connection.is_connected():
            self._db_connection.open(False, False)
            
            #version = client.describe_version()
            # FIXME: Should do some version check here to make sure that we're
            # talking to a cassandra daemon that supports the operations we require
            
        if login:
            self._db_connection.login()
        
        if set_keyspace:
            try:
                self._db_connection.set_keyspace()
            except Exception, e:
                replication_factor = self.settings_dict.get('CASSANDRA_REPLICATION_FACTOR')
                if not replication_factor:
                    replication_factor = 1
                replication_strategy_class = self.settings_dict.get('CASSANDRA_REPLICATION_STRATEGY')
                if not replication_strategy_class:
                    replication_strategy_class = 'org.apache.cassandra.locator.SimpleStrategy'
                keyspace_def = KsDef(name=self._db_connection.keyspace,
                                     strategy_class=replication_strategy_class,
                                     replication_factor=replication_factor,
                                     cf_defs=[])
                self._db_connection.get_client().system_add_keyspace(keyspace_def)
                self._db_connection.set_keyspace()
    
        return self._db_connection
    
    @property
    def db_connection(self):
        return self.get_db_connection(True, True)
