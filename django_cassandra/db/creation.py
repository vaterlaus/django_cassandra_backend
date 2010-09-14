from django.db.backends.creation import TEST_DATABASE_PREFIX
from djangotoolbox.db.creation import NonrelDatabaseCreation
from cassandra import Cassandra
from cassandra.ttypes import *
from django.core.management import call_command

class DatabaseCreation(NonrelDatabaseCreation):
    
    def sql_create_model(self, model, style, known_models=set()):
        
        client = self.connection.db_connection.client
        keyspace_name = self.connection.settings_dict['NAME']
        
        opts = model._meta
        column_metadata = []

        # Browsing through fields to find references
        for field in opts.local_fields:
            if field.db_index:
                column_name = field.db_column if field.db_column else field.column
                column_def = ColumnDef(name=str(column_name), validation_class='BytesType', index_type=IndexType.KEYS, index_name=str(column_name))
                column_metadata.append(column_def)
                    
        column_family_def = CfDef(keyspace=keyspace_name,
                                  name = opts.db_table,
                                  column_metadata = column_metadata)
        client.system_add_column_family(column_family_def)
        return [], {}

    def _get_keyspace_name(self):
        """
        Construct the name to use for the test keyspace. This is the regular
        name of the keyspace (i.e. the one specified in the NAME field of the
        database settings) prepended with 'test_'.
        """
        keyspace_name = self.connection.settings_dict['NAME']
        #keyspace_name = 'test_' + base_keyspace_name
        return keyspace_name

    def init_keyspace(self, keyspace_name):
        """
        Initialize the keyspace, whic may be either the normal keyspace or the
        test keyspace. This involves creating the keyspace if it hasn't already
        been created, setting that keyspace for the Cassandra connection, and
        logging in to that keyspace if the USER/PASSWORD setting are specified.
        """
        
        client = self.connection.db_connection.client
        settings_dict = self.connection.settings_dict

        if not keyspace_name:
            # FIXME: throw an exception here
            pass

        try:
            # Try to set the keyspace. In the case where we're running for the
            # first time (i.e. syncdb) the keyspace will not exist yet, so if
            # this fails we try to create the keyspace and then retry the set.
            client.set_keyspace(keyspace_name)
        except Exception, e:
            # FIXME: This probably should be refactored so that we don't create the
            # keyspace here. It's not ideal, especially in the case where we're
            # running the unit tests against the test database/keyspace, because
            # we create the normal keyspace here even though we're not going to
            # use that keyspace.
            # FIXME: Should maybe be more specific in terms of the exact exception
            # type we catch here (i.e. figure out exactly what exception is thrown
            # by the Thrift API).
            keyspace_def = KsDef(name=keyspace_name,
                                 strategy_class='org.apache.cassandra.locator.SimpleStrategy',
                                 replication_factor=1,
                                 cf_defs=[])
            client.system_add_keyspace(keyspace_def)
            client.set_keyspace(keyspace_name)
            
        # TODO: This user/password auth code hasn't been tested
        user = settings_dict.get('USER')
        if user != None and user != '':
            password = settings_dict.get('PASSWORD')
            authentication_request = Cassandra.AuthenticationRequest({user: password})
            client.login(keyspace_name, authentication_request)
    
    def drop_keyspace(self, keyspace_name, verbosity=1):
        """
        Drop the specified keyspace from the cluster.
        """
        
        client = self.connection.db_connection.client
        
        try:
            client.system_drop_keyspace(keyspace_name)
        except Exception, e:
            # We want succeed without complaining if the test db doesn't
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
        
        # Now create the new test keyspace
        self.init_keyspace(test_keyspace_name)

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
        client = self.connection.db_connection.client
        #client.system_drop_column_family(table_name)
        # FIXME: this should really re-add the table here
        client.truncate(table_name)
        
    def sql_indexes_for_model(self, model, style):
        """
        We already handle creating the indexes in sql_create_model (above) so
        we don't need to do anything more here.
        """
        return []
    
    def set_autocommit(self):
        pass
    