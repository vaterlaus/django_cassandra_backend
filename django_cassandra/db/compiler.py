import datetime
import sys
import traceback
import datetime
import decimal

from django.db.models.sql.where import AND, OR, WhereNode
from django.db.utils import DatabaseError, IntegrityError

from functools import wraps

from djangotoolbox.db.basecompiler import NonrelQuery, NonrelCompiler, \
    NonrelInsertCompiler, NonrelUpdateCompiler, NonrelDeleteCompiler

from .utils import *
from .predicate import *

from uuid import uuid4
from cassandra import Cassandra
from cassandra.ttypes import *

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
        for field in fields:
            if field.db_index:
                column_name = field.db_column if field.db_column else field.column
                self.indexed_columns.append(column_name)
                
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
        row[pk_column_name] = pk_value
        for column in column_list:
            row[column.column.name] = column.column.value
        return row


    def _get_rows_by_pk(self, range_predicate):

        db_connection = self.connection.db_connection
        column_parent = ColumnParent(column_family=self.column_family)
        slice_predicate = SlicePredicate(slice_range=SliceRange(start='', finish='', count = CassandraQuery.MAX_FETCH_COUNT))
        
        if range_predicate._is_exact():
            column_list = db_connection.client.get_slice(range_predicate.start, column_parent, slice_predicate, ConsistencyLevel.ONE)
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
            
            key_range = KeyRange(start_key = key_start, end_key = key_end, count = CassandraQuery.MAX_FETCH_COUNT)
            try:
                key_slice = db_connection.client.get_range_slices(column_parent, slice_predicate, key_range, ConsistencyLevel.ONE)
            except Exception, e:
                raise e
            
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
            # that I'm using (0.7 beta1)
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
        index_clause = IndexClause(index_expressions, '')
        slice_predicate = SlicePredicate(slice_range=SliceRange(start='', finish='', count = CassandraQuery.MAX_FETCH_COUNT))
        key_slice = db_connection.client.get_indexed_slices(column_parent, index_clause, slice_predicate, ConsistencyLevel.ONE)
        
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
        slice_predicate = SlicePredicate(slice_range=SliceRange(start='', finish='', count = CassandraQuery.MAX_FETCH_COUNT))
        #key_range = KeyRange(start_token = ' ', end_token = ' ', count = CassandraQuery.MAX_FETCH_COUNT)
        key_range = KeyRange(start_token = '0', end_token = '0', count = 100)
        #key_range = KeyRange(start_key=chr(1), end_key=chr(255)*16, count=CassandraQuery.MAX_FETCH_COUNT)
        key_slice = db_connection.client.get_range_slices(column_parent, slice_predicate, key_range, ConsistencyLevel.ONE)
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
            traceback.print_exc()
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
            mutation_map[item[self.pk_column]] = {column_family: [Mutation(deletion=Deletion(clock=Clock(timestamp)))]}
        client = self.connection.db_connection.client
        client.batch_mutate(mutation_map, ConsistencyLevel.ONE)
        
    @safe_call
    def order_by(self, ordering):
       self.ordering_spec = []
       for order in ordering:
           if order.startswith('-'):
               column = order[1:]
               reversed = True
           else:
               column = order
               reversed = False
           self.ordering_spec.append((column, reversed))
            
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

    # This gets called for each field type when you fetch() an entity.
    # db_type is the string that you used in the DatabaseCreation mapping
    def convert_value_from_db(self, db_type, value):
        
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
        if db_type.startswith('ListField:'):
            db_sub_type = db_type.split(':', 1)[1]
            if isinstance(value, (list, tuple)) and len(value):
                value = [self.convert_value_for_db(db_sub_type, subvalue) for subvalue in value]
            value = convert_list_to_string(value)
        elif db_type == 'datetime':
            value = value.strftime('%Y-%m-%d %H:%M:%S.%f')
        elif db_type == 'time':
            value = value.strftime('%H:%M:%S.%f')
        elif type(value) is str:
            # always store unicode strings
            value = value.decode('utf-8')
        else:
            value = unicode(value)
        return value

# This handles both inserts and updates of individual entities
class SQLInsertCompiler(NonrelInsertCompiler, SQLCompiler):
    
    @safe_call
    def insert(self, data, return_id=False):
        pk_column = self.query.get_meta().pk.column
        if pk_column in data:
            key = data[pk_column]
            del data[pk_column]
        else:
            key = uuid4()
        
        key = unicode(key)
        
        timestamp = get_next_timestamp()
        
        mutation_list = []
        for name, value in data.items():
            mutation = Mutation(column_or_supercolumn=ColumnOrSuperColumn(column=Column(name=name, value=unicode(value), clock=Clock(timestamp))))
            mutation_list.append(mutation)
        
        client = self.connection.db_connection.client
        column_family = self.query.get_meta().db_table
        client.batch_mutate({key: {column_family: mutation_list}}, ConsistencyLevel.ONE)
        
        if return_id:
            return key

class SQLUpdateCompiler(NonrelUpdateCompiler, SQLCompiler):
    pass

class SQLDeleteCompiler(NonrelDeleteCompiler, SQLCompiler):
    pass
