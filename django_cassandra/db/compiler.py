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

import datetime
import sys
import traceback
import datetime
import decimal

from django.db.models import ForeignKey
from django.db.models.sql.where import AND, OR, WhereNode
from django.db.models.sql.constants import MULTI
from django.db.utils import DatabaseError

from functools import wraps

from djangotoolbox.db.basecompiler import NonrelQuery, NonrelCompiler, \
    NonrelInsertCompiler, NonrelUpdateCompiler, NonrelDeleteCompiler

from .utils import *
from .predicate import *

from uuid import uuid4
from cassandra import Cassandra
from cassandra.ttypes import *
from thrift.transport.TTransport import TTransportException

def safe_call(func):
    @wraps(func)
    def _func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception, e:
            raise DatabaseError, DatabaseError(*tuple(e)), sys.exc_info()[2]
    return _func

class CassandraQuery(NonrelQuery):
    
    # FIXME: How do we set this value? What's the maximum value it can be?
    #MAX_FETCH_COUNT = 0x7ffffff
    MAX_FETCH_COUNT = 10000
    
    def __init__(self, compiler, fields):
        super(CassandraQuery, self).__init__(compiler, fields)

        self.pk_column = self.query.get_meta().pk.column
        self.column_family = self.query.get_meta().db_table
        self.root_predicate = None
        self.ordering_spec = None
        self.cached_results = None
        
        self.indexed_columns = []
        self.field_name_to_column_name = {}
        for field in fields:
            column_name = field.db_column if field.db_column else field.column
            if field.db_index:
                self.indexed_columns.append(column_name)
            self.field_name_to_column_name[field.name] = column_name
                
    # This is needed for debugging
    def __repr__(self):
        # TODO: add some meaningful query string for debugging
        return '<CassandraQuery: ...>'

    def _convert_key_slice_to_rows(self, key_slice):
        rows = []
        for element in key_slice:
            if element.columns:
                row = self._convert_column_list_to_row(element.columns, self.pk_column, element.key)
                rows.append(row)
        return rows
    
    def _convert_column_list_to_row(self, column_list, pk_column_name, pk_value):
        row = {}
        # FIXME: When we add code to allow primary keys that also are indexed,
        # then we can change this to not set the primary key column in that case.
        # row[pk_column_name] = pk_value
        for column in column_list:
            row[column.column.name] = column.column.value
        return row


    def _get_rows_by_pk(self, range_predicate):

        db_connection = self.connection.db_connection
        column_parent = ColumnParent(column_family=self.column_family)
        slice_predicate = SlicePredicate(slice_range=SliceRange(start='',
            finish='', count=self.connection.max_column_count))
        
        if range_predicate._is_exact():
            column_list = call_cassandra_with_reconnect(db_connection,
               Cassandra.Client.get_slice, range_predicate.start,
                column_parent, slice_predicate, self.connection.read_consistency_level)
            if column_list:
                row = self._convert_column_list_to_row(column_list, self.pk_column, range_predicate.start)
                rows = [row]
            else:
                rows = []
        else:
            if range_predicate.start != None:
                key_start = range_predicate.start
                if not range_predicate.start_inclusive:
                    key_start = key_start + chr(1)
            else:
                key_start = ''
             
            if range_predicate.end != None:
                key_end = range_predicate.end
                if not range_predicate.end_inclusive:
                    key_end = key_end[:-1] + chr(ord(key_end[-1])-1) + (chr(126) * 16)
            else:
                key_end = ''
            
            key_range = KeyRange(start_key=key_start, end_key=key_end,
                count=self.connection.max_key_count)
            key_slice = call_cassandra_with_reconnect(db_connection,
                Cassandra.Client.get_range_slices, column_parent,
                slice_predicate, key_range, self.connection.read_consistency_level)
            
            rows = self._convert_key_slice_to_rows(key_slice)
                
        return rows
    
    def _get_rows_by_indexed_column(self, range_predicate):
        # Construct the index expression for the range predicate
        index_expressions = []
        if ((range_predicate.start != None) and
            (range_predicate.end == range_predicate.start) and
            range_predicate.start_inclusive and
            range_predicate.end_inclusive):
            index_expression = IndexExpression(range_predicate.column, IndexOperator.EQ, unicode(range_predicate.start))
            index_expressions.append(index_expression)
        else:
            # NOTE: These range queries don't work with the current version of cassandra
            # that I'm using (0.7 beta3)
            # It looks like there are cassandra tickets to add support for this, but it's
            # unclear how soon it will be supported. We shouldn't hit this code for now,
            # though, because can_evaluate_efficiently was changed to disable range queries
            # on indexed columns (they still can be performed, just inefficiently).
            if range_predicate.start:
                index_op = IndexOperator.GTE if range_predicate.start_inclusive else IndexOperator.GT
                index_expression = IndexExpression(unicode(range_predicate.column), index_op, unicode(range_predicate.start))
                index_expressions.append(index_expression)
            if range_predicate.end:
                index_op = IndexOperator.LTE if range_predicate.end_inclusive else IndexOperator.LT
                index_expression = IndexExpression(unicode(range_predicate.column), index_op, unicode(range_predicate.end))
                index_expressions.append(index_expression)
                
        assert(len(index_expressions) > 0)
               
        # Now make the call to cassandra to get the key slice
        db_connection = self.connection.db_connection
        column_parent = ColumnParent(column_family=self.column_family)
        index_clause = IndexClause(index_expressions, '', self.connection.max_key_count)
        slice_predicate = SlicePredicate(slice_range=SliceRange(start='', finish='', count=self.connection.max_column_count))
        
        key_slice = call_cassandra_with_reconnect(db_connection,
            Cassandra.Client.get_indexed_slices,
            column_parent, index_clause, slice_predicate,
            self.connection.read_consistency_level)
        rows = self._convert_key_slice_to_rows(key_slice)
        
        return rows
    
    def get_row_range(self, range_predicate):
        pk_column = self.query.get_meta().pk.column
        if range_predicate.column == pk_column:
            rows = self._get_rows_by_pk(range_predicate)
        else:
            assert(range_predicate.column in self.indexed_columns)
            rows = self._get_rows_by_indexed_column(range_predicate)
        return rows
    
    def get_all_rows(self):
        # TODO: Could factor this code better
        db_connection = self.connection.db_connection
        column_parent = ColumnParent(column_family=self.column_family)
        slice_predicate = SlicePredicate(slice_range=SliceRange(start='', finish='', count=self.connection.max_column_count))
        key_range = KeyRange(start_token = '0', end_token = '0', count=self.connection.max_key_count)
        #end_key = u'\U0010ffff'.encode('utf-8')
        #key_range = KeyRange(start_key='\x01', end_key=end_key, count=self.connection.max_key_count)
        
        key_slice = call_cassandra_with_reconnect(db_connection,
            Cassandra.Client.get_range_slices, column_parent,
            slice_predicate, key_range, self.connection.read_consistency_level)
        rows = self._convert_key_slice_to_rows(key_slice)
        
        return rows
    
    def _get_query_results(self):
        if self.cached_results == None:
            assert(self.root_predicate != None)
            self.cached_results = self.root_predicate.get_matching_rows(self)
            if self.ordering_spec:
                sort_rows(self.cached_results, self.ordering_spec)
        return self.cached_results
    
    @safe_call
    def fetch(self, low_mark, high_mark):
        
        if self.root_predicate == None:
            raise DatabaseError('No root query node')
        
        try:
            if high_mark is not None and high_mark <= low_mark:
                return
            
            results = self._get_query_results()
            if low_mark is not None or high_mark is not None:
                results = results[low_mark:high_mark]
        except Exception, e:
            # FIXME: Can get rid of this exception handling code eventually,
            # but it's useful for debugging for now.
            #traceback.print_exc()
            raise e
        
        for entity in results:
            yield entity

    @safe_call
    def count(self, limit=None):
        # TODO: This could be implemented more efficiently for simple predicates
        # where we could call the count method in the Cassandra Thrift API.
        # We can optimize for that later
        results = self._get_query_results()
        return len(results)
    
    @safe_call
    def delete(self):
        results = self._get_query_results()
        timestamp = get_next_timestamp()
        column_family = self.query.get_meta().db_table
        mutation_map = {}
        for item in results:
            mutation_map[item[self.pk_column]] = {column_family: [Mutation(deletion=Deletion(timestamp=timestamp))]}
        db_connection = self.connection.db_connection
        call_cassandra_with_reconnect(db_connection,
            Cassandra.Client.batch_mutate, mutation_map,
            self.connection.write_consistency_level)
        

    @safe_call
    def order_by(self, ordering):
        self.ordering_spec = []
        for order in ordering:
            if order.startswith('-'):
                field_name = order[1:]
                reversed = True
            else:
                field_name = order
                reversed = False
            column_name = self.field_name_to_column_name.get(field_name, field_name)
            #if column in self.foreign_key_columns:
            #    column = column + '_id'
            self.ordering_spec.append((column_name, reversed))
            
    def init_predicate(self, parent_predicate, node):
        if isinstance(node, WhereNode):
            if node.connector == OR:
                compound_op = COMPOUND_OP_OR
            elif node.connector == AND:
                compound_op = COMPOUND_OP_AND
            else:
                raise InvalidQueryOpException()
            predicate = CompoundPredicate(compound_op, node.negated)
            for child in node.children:
                child_predicate = self.init_predicate(predicate, child)
            if parent_predicate:
                parent_predicate.add_child(predicate)
        else:
            column, lookup_type, db_type, value = self._decode_child(node)
            db_value = self.convert_value_for_db(db_type, value)
            assert parent_predicate
            parent_predicate.add_filter(column, lookup_type, db_value)
            predicate = None
            
        return predicate
    
    # FIXME: This is bad. We're modifying the WhereNode object that's passed in to us
    # from the Django ORM. We should do the pruning as we build our predicates, not
    # munge the WhereNode.
    def remove_unnecessary_nodes(self, node, retain_root_node):
        if isinstance(node, WhereNode):
            child_count = len(node.children)
            for i in range(child_count):
                node.children[i] = self.remove_unnecessary_nodes(node.children[i], False)
            if (not retain_root_node) and (not node.negated) and (len(node.children) == 1):
                node = node.children[0]
        return node
        
    @safe_call
    def add_filters(self, filters):
        """
        Traverses the given Where tree and adds the filters to this query
        """
        
        #if filters.negated:
        #    raise InvalidQueryOpException('Exclude queries not implemented yet.')
        assert isinstance(filters,WhereNode)
        self.remove_unnecessary_nodes(filters, True)
        self.root_predicate = self.init_predicate(None, filters)
        
class SQLCompiler(NonrelCompiler):
    query_class = CassandraQuery

    SPECIAL_NONE_VALUE = "\b"

    # Override this method from NonrelCompiler to get around problem with
    # mixing the field default values with the field format as its stored
    # in the database (i.e. convert_value_from_db should only be passed
    # the database-specific storage format not the field default value.
    def _make_result(self, entity, fields):
        result = []
        for field in fields:
            value = entity.get(field.column)
            if value is not None:
                value = self.convert_value_from_db(
                    field.db_type(connection=self.connection), value)
            else:
                value = field.get_default()
            if not field.null and value is None:
                raise DatabaseError("Non-nullable field %s can't be None!" % field.name)
            result.append(value)
            
        return result
    
    # This gets called for each field type when you fetch() an entity.
    # db_type is the string that you used in the DatabaseCreation mapping
    def convert_value_from_db(self, db_type, value):
        
        if value == self.SPECIAL_NONE_VALUE or value is None:
            return None
        
        if  db_type.startswith('ListField:'):
            db_sub_type = db_type.split(':', 1)[1]
            value = convert_string_to_list(value)
            if isinstance(value, (list, tuple)) and len(value):
                value = [self.convert_value_from_db(db_sub_type, subvalue)
                         for subvalue in value]
        elif db_type == 'date':
            dt = datetime.datetime.strptime(value, '%Y-%m-%d')
            value = dt.date()
        elif db_type == 'datetime':
            value = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
        elif db_type == 'time':
            dt = datetime.datetime.strptime(value, '%H:%M:%S.%f')
            value = dt.time()
        elif db_type == 'bool':
            value = value.lower() == 'true'
        elif db_type == 'int':
            value = int(value)
        elif db_type == 'long':
            value = long(value)
        elif db_type == 'float':
            value = float(value)
        #elif db_type == 'id':
        #    value = unicode(value).decode('utf-8')
        elif db_type.startswith('decimal'):
            value = decimal.Decimal(value)
        elif isinstance(value, str):
            # always retrieve strings as unicode (it is possible that old datasets
            # contain non unicode strings, nevertheless work with unicode ones)
            value = value.decode('utf-8')
            
        return value

    # This gets called for each field type when you insert() an entity.
    # db_type is the string that you used in the DatabaseCreation mapping
    def convert_value_for_db(self, db_type, value):
        if value is None:
            return self.SPECIAL_NONE_VALUE
        
        if db_type.startswith('ListField:'):
            db_sub_type = db_type.split(':', 1)[1]
            if isinstance(value, (list, tuple)) and len(value):
                value = [self.convert_value_for_db(db_sub_type, subvalue) for subvalue in value]
            value = convert_list_to_string(value)
        elif type(value) is list:
            value = [self.convert_value_for_db(db_type, item) for item in value]
        elif db_type == 'datetime':
            value = value.strftime('%Y-%m-%d %H:%M:%S.%f')
        elif db_type == 'time':
            value = value.strftime('%H:%M:%S.%f')
        elif db_type == 'bool':
            value = str(value).lower()
        elif (db_type == 'int') or (db_type == 'long') or (db_type == 'float'):
            value = str(value)
        elif db_type == 'id':
            value = unicode(value)
        elif (type(value) is not unicode) and (type(value) is not str):
            value = unicode(value)
        
        # always store strings as utf-8
        if type(value) is unicode:
            value = value.encode('utf-8')
            
        return value

# This handles both inserts and updates of individual entities
class SQLInsertCompiler(NonrelInsertCompiler, SQLCompiler):
    
    @safe_call
    def insert(self, data, return_id=False):
        pk_column = self.query.get_meta().pk.column
        model = self.query.model
        compound_key_fields = None
        if hasattr(model, 'CassandraSettings'):
            if hasattr(model.CassandraSettings, 'ADJUSTED_COMPOUND_KEY_FIELDS'):
                compound_key_fields = model.CassandraSettings.ADJUSTED_COMPOUND_KEY_FIELDS
            elif hasattr(model.CassandraSettings, 'COMPOUND_KEY_FIELDS'):
                compound_key_fields = []
                for field_name in model.CassandraSettings.COMPOUND_KEY_FIELDS:
                    field_class = None
                    for lf in model._meta.local_fields:
                        if lf.name == field_name:
                            field_class = lf
                            break
                    if field_class is None:
                        raise DatabaseError('Invalid compound key field')
                    if type(field_class) is ForeignKey:
                        field_name += '_id'
                    compound_key_fields.append(field_name)
                model.CassandraSettings.ADJUSTED_COMPOUND_KEY_FIELDS = compound_key_fields
            separator = model.CassandraSettings.COMPOUND_KEY_SEPARATOR \
                if hasattr(model.CassandraSettings, 'COMPOUND_KEY_SEPARATOR') \
                else self.connection.settings_dict.get('CASSANDRA_COMPOUND_KEY_SEPARATOR', '|')
        # See if the data arguments contain a value for the primary key.
        # FIXME: For now we leave the key data as a column too. This is
        # suboptimal, since the data is duplicated, but there are a couple of cases
        # where you need to keep the column. First, if you have a model with only
        # a single field that's the primary key (admittedly a semi-pathological case,
        # but I can imagine valid use cases where you have this), then it doesn't
        # work if the column is removed, because then there are no columns and that's
        # interpreted as a deleted row (i.e. the usual Cassandra tombstone issue).
        # Second, if there's a secondary index configured for the primary key field
        # (not particularly useful with the current Cassandra, but would be valid when
        # you can do a range query on indexed column) then you'd want to keep the
        # column. So for now, we just leave the column in there so these cases work.
        # Eventually we can optimize this and remove the column where it makes sense.
        key = data.get(pk_column)
        if key:
            if compound_key_fields is not None:
                compound_key_values = key.split(separator)
                for field_name, compound_key_value in zip(compound_key_fields, compound_key_values):
                    if field_name in data and data[field_name] != compound_key_value:
                        raise DatabaseError("The value of the compound key doesn't match the values of the individual fields")
        else:
            if compound_key_fields is not None:
                try:
                    compound_key_values = [data.get(field_name) for field_name in compound_key_fields]
                    key = separator.join(compound_key_values)
                except Exception, e:
                    raise DatabaseError('The values of the fields used to form a compound key must be specified and cannot be null')
            else:
                key = str(uuid4())
            # Insert the key as column data too
            # FIXME. See the above comment. When the primary key handling is optimized,
            # then we would not always add the key to the data here.
            data[pk_column] = key
        
        timestamp = get_next_timestamp()
        
        mutation_list = []
        for name, value in data.items():
            # FIXME: Do we need this check here? Or is the name always already a str instead of unicode.
            if type(name) is unicode:
                name = name.decode('utf-8')
            mutation = Mutation(column_or_supercolumn=ColumnOrSuperColumn(column=Column(name=name, value=value, timestamp=timestamp)))
            mutation_list.append(mutation)
        
        db_connection = self.connection.db_connection
        column_family = self.query.get_meta().db_table
        call_cassandra_with_reconnect(db_connection,
            Cassandra.Client.batch_mutate, {key: {column_family: mutation_list}},
            self.connection.write_consistency_level)
        
        if return_id:
            return key

class SQLUpdateCompiler(NonrelUpdateCompiler, SQLCompiler):
    def __init__(self, *args, **kwargs):
        super(SQLUpdateCompiler, self).__init__(*args, **kwargs)
        
    def execute_sql(self, result_type=MULTI):
        data = {}
        for field, model, value in self.query.values:
            assert field is not None
            if not field.null and value is None:
                raise DatabaseError("You can't set %s (a non-nullable "
                                    "field) to None!" % field.name)
            db_type = field.db_type(connection=self.connection)
            value = self.convert_value_for_db(db_type, value)
            data[field.column] = value
        
        # TODO: Add compound key check here -- ensure that we're not updating
        # any of the fields that are components in the compound key.
        
        # TODO: This isn't super efficient because executing the query will
        # fetch all of the columns for each row even though all we really need
        # is the key for the row. Should be pretty straightforward to change
        # the CassandraQuery class to support custom slice predicates.
        
        #model = self.query.model
        pk_column = self.query.get_meta().pk.column
        
        pk_index = -1
        fields = self.get_fields()
        for index in range(len(fields)):
            if fields[index].column == pk_column:
                pk_index = index;
                break
        if pk_index == -1:
            raise DatabaseError('Invalid primary key column')
        
        row_count = 0
        column_family = self.query.get_meta().db_table
        timestamp = get_next_timestamp()
        batch_mutate_data = {}
        for result in self.results_iter():
            row_count += 1
            mutation_list = []
            key = result[pk_index]
            for name, value in data.items():
                # FIXME: Do we need this check here? Or is the name always already a str instead of unicode.
                if type(name) is unicode:
                    name = name.decode('utf-8')
                mutation = Mutation(column_or_supercolumn=ColumnOrSuperColumn(column=Column(name=name, value=value, timestamp=timestamp)))
                mutation_list.append(mutation)
            batch_mutate_data[key] = {column_family: mutation_list}
        
        db_connection = self.connection.db_connection
        call_cassandra_with_reconnect(db_connection,
            Cassandra.Client.batch_mutate, batch_mutate_data,
            self.connection.write_consistency_level)
        
        return row_count
    
class SQLDeleteCompiler(NonrelDeleteCompiler, SQLCompiler):
    pass
