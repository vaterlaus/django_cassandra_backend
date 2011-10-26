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

from django.db.backends.creation import TEST_DATABASE_PREFIX
from django.db.utils import DatabaseError
from djangotoolbox.db.creation import NonrelDatabaseCreation
from cassandra import Cassandra
from cassandra.ttypes import *
from django.core.management import call_command
from .utils import get_next_timestamp

class DatabaseCreation(NonrelDatabaseCreation):

    data_types = {
        'AutoField':         'text',
        'BigIntegerField':   'long',
        'BooleanField':      'bool',
        'CharField':         'text',
        'CommaSeparatedIntegerField': 'text',
        'DateField':         'date',
        'DateTimeField':     'datetime',
        'DecimalField':      'decimal:%(max_digits)s,%(decimal_places)s',
        'EmailField':        'text',
        'FileField':         'text',
        'FilePathField':     'text',
        'FloatField':        'float',
        'ImageField':        'text',
        'IntegerField':      'int',
        'IPAddressField':    'text',
        'NullBooleanField':  'bool',
        'OneToOneField':     'integer',
        'PositiveIntegerField': 'int',
        'PositiveSmallIntegerField': 'int',
        'SlugField':         'text',
        'SmallIntegerField': 'integer',
        'TextField':         'text',
        'TimeField':         'time',
        'URLField':          'text',
        'XMLField':          'text',
        'GenericAutoField':  'id',
        'StringForeignKey':  'id',
        'AutoField':         'id',
        'RelatedAutoField':  'id',
    }
    
    def sql_create_model(self, model, style, known_models=set()):
        
        db_connection = self.connection.db_connection
        keyspace = self.connection.settings_dict['NAME']
        
        opts = model._meta
        column_metadata = []

        # Browsing through fields to find indexed fields
        for field in opts.local_fields:
            if field.db_index:
                column_name = str(field.db_column if field.db_column else field.column)
                column_def = ColumnDef(name=column_name, validation_class='BytesType',
                                       index_type=IndexType.KEYS)
                column_metadata.append(column_def)
        
        cfdef_settings = self.connection.column_family_def_defaults.copy()
        
        if hasattr(model, 'CassandraSettings') and \
            hasattr(model.CassandraSettings, 'COLUMN_FAMILY_DEF_SETTINGS'):
            cfdef_overrides = model.CassandraSettings.COLUMN_FAMILY_DEF_SETTINGS
            if type(cfdef_overrides) is not dict:
                raise DatabaseError('The value of COLUMN_FAMILY_DEF_SETTINGS in the '
                    'CassandraSettings class must be a dictionary of the optional '
                    'settings to use when creating the column family.')
            cfdef_settings.update(cfdef_overrides)

        cfdef_settings['keyspace'] = keyspace
        if not cfdef_settings.get('name'):
            cfdef_settings['name'] = opts.db_table
        if not cfdef_settings.get('comparator_type'):
            cfdef_settings['comparator_type'] = 'UTF8Type'
        cfdef_settings['column_metadata'] = column_metadata
        
        column_family_def = CfDef(**cfdef_settings)
        
        db_connection.get_client().system_add_column_family(column_family_def)
        
        return [], {}

    def drop_keyspace(self, keyspace_name, verbosity=1):
        """
        Drop the specified keyspace from the cluster.
        """
        
        db_connection = self.connection.get_db_connection(False, False)
        
        try:
            db_connection.get_client().system_drop_keyspace(keyspace_name)
        except Exception, e:
            # We want to succeed without complaining if the test db doesn't
            # exist yet, so we just assume that any exception that's raised
            # was for that reason and ignore it, except for printing a
            # message if verbose output is enabled
            # FIXME: Could probably be more specific about the Thrift
            # exception that we catch here.
            #if verbosity >= 1:
            #    print "Exception thrown while trying to drop the test database/keyspace: ", e
            pass
        
    def create_test_db(self, verbosity, autoclobber):
        """
        Create a new test database/keyspace.
        """
        
        if verbosity >= 1:
            print "Creating test database '%s'..." % self.connection.alias

        # Replace the NAME field in the database settings with the test keyspace name
        settings_dict = self.connection.settings_dict
        if settings_dict.get('TEST_NAME'):
            test_keyspace_name = settings_dict['TEST_NAME']
        else:
            test_keyspace_name = TEST_DATABASE_PREFIX + settings_dict['NAME']

        settings_dict['NAME'] = test_keyspace_name
        
        # First make sure we've destroyed an existing test keyspace
        # FIXME: Should probably do something with autoclobber here, but why
        # would you ever not want to autoclobber when running the tests?
        self.drop_keyspace(test_keyspace_name, verbosity)
        
        # Call syncdb to create the necessary tables/column families
        call_command('syncdb', verbosity=False, interactive=False, database=self.connection.alias)
    
        return test_keyspace_name
    
    def destroy_test_db(self, old_database_name, verbosity=1):
        """
        Destroy the test database/keyspace.
        """

        if verbosity >= 1:
            print "Destroying test database '%s'..." % self.connection.alias
            
        settings_dict = self.connection.settings_dict
        test_keyspace_name = settings_dict.get('NAME')
        settings_dict['NAME'] = old_database_name
        
        self.drop_keyspace(test_keyspace_name, verbosity)
        
    def flush_table(self, table_name):
        
        db_connection = self.connection.db_connection

        # FIXME: Calling truncate here seems to corrupt the secondary indexes,
        # so for now the truncate call has been replaced with removing the
        # row one by one. When the truncate bug has been fixed in Cassandra
        # this should be switched back to use truncate.
        # NOTE: This should be fixed as of the 0.7.0-rc2 build, so we should
        # try this out again to see if it works now.
        # UPDATE: Tried it with rc2 and it worked calling truncate but it was
        # slower than using remove (at least for the unit tests), so for now
        # I'm leaving it alone pending further investigation.
        #db_connection.get_client().truncate(table_name)
        
        column_parent = ColumnParent(column_family=table_name)
        slice_predicate = SlicePredicate(column_names=[])
        key_range = KeyRange(start_token = '0', end_token = '0', count = 1000)
        key_slice_list = db_connection.get_client().get_range_slices(column_parent, slice_predicate, key_range, ConsistencyLevel.ONE)
        column_path = ColumnPath(column_family=table_name)
        timestamp = get_next_timestamp()
        for key_slice in key_slice_list:
            db_connection.get_client().remove(key_slice.key, column_path, timestamp, ConsistencyLevel.ONE)

        
    def sql_indexes_for_model(self, model, style):
        """
        We already handle creating the indexes in sql_create_model (above) so
        we don't need to do anything more here.
        """
        return []
    
    def set_autocommit(self):
        pass
    
