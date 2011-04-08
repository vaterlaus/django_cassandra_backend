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

from djangotoolbox.db.base import NonrelDatabaseIntrospection
from django.db.backends import BaseDatabaseIntrospection

class DatabaseIntrospection(NonrelDatabaseIntrospection):
    def get_table_list(self, cursor):
        "Returns a list of names of all tables that exist in the database."
        db_connection = self.connection.db_connection
        ks_def = db_connection.get_client().describe_keyspace(db_connection.keyspace)
        result = [cf_def.name for cf_def in ks_def.cf_defs]
        return result
    
    def table_names(self):
        # NonrelDatabaseIntrospection has an implementation of this that returns
        # that all of the tables for the models already exist in the database,
        # so the DatabaseCreation code never gets called to create new tables,
        # which isn't how we want things to work for Cassandra, so we bypass the
        # nonrel implementation and go directly to the base introspection code.
        return BaseDatabaseIntrospection.table_names(self)

    def sequence_list(self):
        return []
    
# TODO: Implement these things eventually
#===============================================================================
#    def get_table_description(self, cursor, table_name):
#        "Returns a description of the table, with the DB-API cursor.description interface."
#        return ""
# 
#    def get_relations(self, cursor, table_name):
#        """
#        Returns a dictionary of {field_index: (field_index_other_table, other_table)}
#        representing all relationships to the given table. Indexes are 0-based.
#        """
#        relations = {}
#        return relations
#    
#    def get_indexes(self, cursor, table_name):
#        """
#        Returns a dictionary of fieldname -> infodict for the given table,
#        where each infodict is in the format:
#            {'primary_key': boolean representing whether it's the primary key,
#             'unique': boolean representing whether it's a unique index}
#        """
#        indexes = {}
#        return indexes
#===============================================================================
