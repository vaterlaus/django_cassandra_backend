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

from django.db.utils import DatabaseError

from djangotoolbox.db.base import NonrelDatabaseFeatures, \
    NonrelDatabaseOperations, NonrelDatabaseWrapper, NonrelDatabaseClient, \
    NonrelDatabaseValidation, NonrelDatabaseIntrospection, \
    NonrelDatabaseCreation

import re
import time
from .creation import DatabaseCreation
from .introspection import DatabaseIntrospection
from .utils import CassandraConnection, CassandraConnectionError, CassandraAccessError
from thrift.transport import TTransport
from cassandra.ttypes import *


class DatabaseFeatures(NonrelDatabaseFeatures):
    string_based_auto_field = True
    
    def __init__(self, connection):
        super(DatabaseFeatures, self).__init__(connection)
        self.supports_deleting_related_objects = connection.settings_dict.get('CASSANDRA_ENABLE_CASCADING_DELETES', False)


class DatabaseOperations(NonrelDatabaseOperations):
    compiler_module = __name__.rsplit('.', 1)[0] + '.compiler'
    
    def pk_default_value(self):
        """
        Use None as the value to indicate to the insert compiler that it needs
        to auto-generate a guid to use for the id. The case where this gets hit
        is when you create a model instance with no arguments. We override from
        the default implementation (which returns 'DEFAULT') because it's possible
        that someone would explicitly initialize the id field to be that value and
        we wouldn't want to override that. But None would never be a valid value
        for the id.
        """
        return None
    
    def sql_flush(self, style, tables, sequence_list):
        for table_name in tables:
            self.connection.creation.flush_table(table_name)
        return ""
    
class DatabaseClient(NonrelDatabaseClient):
    pass

class DatabaseValidation(NonrelDatabaseValidation):
    pass

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
        self.max_key_count = self.settings_dict.get('CASSANDRA_MAX_KEY_COUNT', 1000000)
        self.max_column_count = self.settings_dict.get('CASSANDRA_MAX_COLUMN_COUNT', 10000)
        self.column_family_def_defaults = self.settings_dict.get('CASSANDRA_COLUMN_FAMILY_DEF_DEFAULT_SETTINGS', {})

        self._db_connection = None
        self.determined_version = False
        
    def configure_connection(self, set_keyspace=False, login=False):
        
        if not self._db_connection.is_connected():
            self._db_connection.open(False, False)
            self.determined_version = False
            
        if not self.determined_version:
            # Determine which version of Cassandra we're connected to
            version_string = self._db_connection.get_client().describe_version()
            try:
                # FIXME: Should do some version check here to make sure that we're
                # talking to a cassandra daemon that supports the operations we require
                m = re.match('^([0-9]+)\.([0-9]+)\.([0-9]+)$', version_string)
                major_version = int(m.group(1))
                minor_version = int(m.group(2))
                patch_version = int(m.group(3))
                self.determined_version = True
            except Exception, e:
                raise DatabaseError('Invalid Thrift version string', e)
            
            # Determine supported features based on the API version
            self.supports_replication_factor_as_strategy_option = major_version >= 19 and minor_version >= 10
        
        if login:
            self._db_connection.login()
        
        if set_keyspace:
            try:
                self._db_connection.set_keyspace()
            except Exception, e:
                # Set up the default settings for the keyspace
                keyspace_def_settings = {
                    'name': self._db_connection.keyspace,
                    'strategy_class': 'org.apache.cassandra.locator.SimpleStrategy',
                    'strategy_options': {},
                    'cf_defs': []}
            
                # Apply any overrides for the keyspace settings
                custom_keyspace_def_settings = self.settings_dict.get('CASSANDRA_KEYSPACE_DEF_SETTINGS')
                if custom_keyspace_def_settings:
                    keyspace_def_settings.update(custom_keyspace_def_settings)
                
                # Apply any overrides for the replication strategy
                # Note: This could be done by the user using the 
                # CASSANDRA_KEYSPACE_DEF_SETTINGS, but the following customizations are
                # still supported for backwards compatibility with older versions of the backend
                strategy_class = self.settings_dict.get('CASSANDRA_REPLICATION_STRATEGY')
                if strategy_class:
                    keyspace_def_settings['strategy_class'] = strategy_class
                
                # Apply an override of the strategy options
                strategy_options = self.settings_dict.get('CASSANDRA_REPLICATION_STRATEGY_OPTIONS')
                if strategy_options:
                    if type(strategy_options) != dict:
                        raise DatabaseError('CASSANDRA_REPLICATION_STRATEGY_OPTIONS must be a dictionary')
                    keyspace_def_settings['strategy_options'].update(strategy_options)
                
                # Apply an override of the replication factor. Depending on the version of
                # Cassandra this may be applied to either the strategy options or the top-level
                # keyspace def settings
                replication_factor = self.settings_dict.get('CASSANDRA_REPLICATION_FACTOR')
                replication_factor_parent = keyspace_def_settings['strategy_options'] \
                    if self.supports_replication_factor_as_strategy_option else keyspace_def_settings
                if replication_factor:
                    replication_factor_parent['replication_factor'] = str(replication_factor)
                elif 'replication_factor' not in replication_factor_parent:
                    replication_factor_parent['replication_factor'] = '1'
                
                keyspace_def = KsDef(**keyspace_def_settings)
                self._db_connection.get_client().system_add_keyspace(keyspace_def)
                self._db_connection.set_keyspace()
                
    
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
            
        try:
            self.configure_connection(set_keyspace, login)
        except TTransport.TTransportException, e:
            raise CassandraConnectionError(e)
        except Exception, e:
            raise CassandraAccessError(e)
        
        return self._db_connection
    
    @property
    def db_connection(self):
        return self.get_db_connection(True, True)
