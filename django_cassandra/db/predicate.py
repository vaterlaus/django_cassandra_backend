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

import re
from .utils import combine_rows

SECONDARY_INDEX_SUPPORT_ENABLED = True

class InvalidSortSpecException(Exception):
    def __init__(self):
        super(InvalidSortSpecException, self).__init__('The row sort spec must be a sort spec tuple/list or a tuple/list of sort specs')

class InvalidRowCombinationOpException(Exception):
    def __init__(self):
        super(InvalidRowCombinationOpException, self).__init__('Invalid row combination operation')

class InvalidPredicateOpException(Exception):
    def __init__(self):
        super(InvalidPredicateOpException, self).__init__('Invalid/unsupported query predicate operation')


COMPOUND_OP_AND = 1
COMPOUND_OP_OR = 2

class RangePredicate(object):
    
    def __init__(self, column, start=None, start_inclusive=True, end=None, end_inclusive=True):
        self.column = column
        self.start = start
        self.start_inclusive = start_inclusive
        self.end = end
        self.end_inclusive = end_inclusive
    
    def __repr__(self):
        s = '(RANGE: '
        if self.start:
            op = '<=' if self.start_inclusive else '<'
            s += (unicode(self.start) + op)
        s += self.column
        if self.end:
            op = '>=' if self.end_inclusive else '>'
            s += (op + unicode(self.end))
        s += ')'
        return s

    def _is_exact(self):
        return (self.start != None) and (self.start == self.end) and self.start_inclusive and self.end_inclusive
    
    def can_evaluate_efficiently(self, pk_column, indexed_columns):
        # FIXME: There's some problem with secondary index support currently.
        # I'm suspicious that this is a bug in Cassandra but I haven't really verified that yet.
        # Anyway disabling the secondary index support for now.
        return ((self.column == pk_column) or
                (SECONDARY_INDEX_SUPPORT_ENABLED and ((self.column in indexed_columns) and self._is_exact())))
    
    def incorporate_range_op(self, column, op, value, parent_compound_op):
        if column != self.column:
            return False
        
        # FIXME: The following logic could probably be tightened up a bit
        # (although perhaps at the expense of clarity?)
        if parent_compound_op == COMPOUND_OP_AND:
            if op == 'gt':
                if self.start == None or value >= self.start:
                    self.start = value
                    self.start_inclusive = False
                    return True
            elif op == 'gte':
                if self.start == None or value > self.start:
                    self.start = value
                    self.start_inclusive = True
                    return True
            elif op == 'lt':
                if self.end == None or value <= self.end:
                    self.end = value
                    self.end_inclusive = False
                    return True
            elif op == 'lte':
                if self.end == None or value < self.end:
                    self.end = value
                    self.end_inclusive = True
                    return True
            elif op == 'exact':
                if self._matches_value(value):
                    self.start = self.end = value
                    self.start_inclusive = self.end_inclusive = True
                    return True
            elif op == 'startswith':
                # For the end value we increment the ordinal value of the last character
                # in the start value and make the end value not inclusive
                end_value = value[:-1] + chr(ord(value[-1])+1)
                if (((self.start == None) or (value > self.start)) and
                    ((self.end == None) or (end_value <= self.end))):
                    self.start = value
                    self.end = end_value
                    self.start_inclusive = True
                    self.end_inclusive = False
                    return True
            else:
                raise InvalidPredicateOpException()
        elif parent_compound_op == COMPOUND_OP_OR:
            if op == 'gt':
                if self.start == None or value < self.start:
                    self.start = value
                    self.start_inclusive = False
                    return True
            elif op == 'gte':
                if self.start == None or value <= self.start:
                    self.start = value
                    self.start_inclusive = True
                    return True
            elif op == 'lt':
                if self.end == None or value > self.end:
                    self.end = value
                    self.end_inclusive = False
                    return True
            elif op == 'lte':
                if self.end == None or value >= self.end:
                    self.end = value
                    self.end_inclusive = True
                    return True
            elif op == 'exact':
                if self._matches_value(value):
                    return True
            elif op == 'startswith':
                # For the end value we increment the ordinal value of the last character
                # in the start value and make the end value not inclusive
                end_value = value[:-1] + chr(ord(value[-1])+1)
                if (((self.start == None) or (value <= self.start)) and
                    ((self.end == None) or (end_value > self.end))):
                    self.start = value
                    self.end = end_value
                    self.start_inclusive = True
                    self.end_inclusive = False
                    return True
        else:
            raise InvalidPredicateOpException()
    
        return False
    
    def _matches_value(self, value):
        if value == None:
            return False
        if self.start != None:
            if self.start_inclusive:
                if value < self.start:
                    return False
            elif value <= self.start:
                return False
        if self.end != None:
            if self.end_inclusive:
                if value > self.end:
                    return False
            elif value >= self.end:
                return False
        return True
    
    def row_matches(self, row):
        value = row.get(self.column, None)
        return self._matches_value(value)
    
    def get_matching_rows(self, query):
        rows = query.get_row_range(self)
        return rows
    
class OperationPredicate(object):
    def __init__(self, column, op, value=None):
        self.column = column
        self.op = op
        self.value = value
        if op == 'regex' or op == 'iregex':
            flags = re.I if op == 'iregex' else 0
            self.pattern = re.compile(value, flags)
    
    def __repr__(self):
        return '(OP: ' + self.op + ':' + unicode(self.value) + ')'
    
    def can_evaluate_efficiently(self, pk_column, indexed_columns):
        return False

    def row_matches(self, row):
        row_value = row.get(self.column, None)
        if self.op == 'isnull':
            return row_value == None
        # FIXME: Not sure if the following test is correct in all cases
        if (row_value == None) or (self.value == None):
            return False
        if self.op == 'in':
            return row_value in self.value
        if self.op == 'istartswith':
            return row_value.lower().startswith(self.value.lower())
        elif self.op == 'endswith':
            return row_value.endswith(self.value)
        elif self.op == 'iendswith':
            return row_value.lower().endswith(self.value.lower())
        elif self.op == 'iexact':
            return row_value.lower() == self.value.lower()
        elif self.op == 'contains':
            return row_value.find(self.value) >= 0
        elif self.op == 'icontains':
            return row_value.lower().find(self.value.lower()) >= 0
        elif self.op == 'regex' or self.op == 'iregex':
            return self.pattern.match(row_value) != None
        else:
            raise InvalidPredicateOpException()
    
    def incorporate_range_op(self, column, op, value, parent_compound_op):
        return False
    
    def get_matching_rows(self, query):
        # get_matching_rows should only be called for predicates that can
        # be evaluated efficiently, which is not the case for OperationPredicate's
        raise NotImplementedError('get_matching_rows() called for inefficient predicate')
    
class CompoundPredicate(object):
    def __init__(self, op, negated=False, children=None):
        self.op = op
        self.negated = negated
        self.children = children
        if self.children == None:
            self.children = []
    
    def __repr__(self):
        s = '('
        if self.negated:
            s += 'NOT '
        s += ('AND' if self.op == COMPOUND_OP_AND else 'OR')
        s += ': '
        first_time = True
        if self.children:
            for child_predicate in self.children:
                if first_time:
                    first_time = False
                else:
                    s += ','
                s += unicode(child_predicate)
        s += ')'
        return s
    
    def can_evaluate_efficiently(self, pk_column, indexed_columns):
        if self.negated:
            return False
        if self.op == COMPOUND_OP_AND:
            for child in self.children:
                if child.can_evaluate_efficiently(pk_column, indexed_columns):
                    return True
            else:
                return False
        elif self.op == COMPOUND_OP_OR:
            for child in self.children:
                if not child.can_evaluate_efficiently(pk_column, indexed_columns):
                    return False
            else:
                return True
        else:
            raise InvalidPredicateOpException()

    def row_matches_subset(self, row, subset):
        if self.op == COMPOUND_OP_AND:
            for predicate in subset:
                if not predicate.row_matches(row):
                    matches = False
                    break
            else:
                matches = True
        elif self.op == COMPOUND_OP_OR:
            for predicate in subset:
                if predicate.row_matches(row):
                    matches =  True
                    break
            else:
                matches = False
        else:
            raise InvalidPredicateOpException()
        
        if self.negated:
            matches = not matches
            
        return matches
        
    def row_matches(self, row):
        return self.row_matches_subset(row, self.children)
    
    def incorporate_range_op(self, column, op, value, parent_predicate):
        return False
    
    def add_filter(self, column, op, value):
        if op in ('lt', 'lte', 'gt', 'gte', 'exact', 'startswith'):
            for child in self.children:
                if child.incorporate_range_op(column, op, value, self.op):
                    return
            else:
                child = RangePredicate(column)
                incorporated = child.incorporate_range_op(column, op, value, COMPOUND_OP_AND)
                assert incorporated
                self.children.append(child)
        else:
            child = OperationPredicate(column, op, value)
            self.children.append(child)
    
    def add_child(self, child_query_node):
        self.children.append(child_query_node)
    
    def get_matching_rows(self, query):
        pk_column = query.query.get_meta().pk.column
        #indexed_columns = query.indexed_columns
        
        # In the first pass we handle the query nodes that can be processed
        # efficiently. Hopefully, in most cases, this will result in a
        # subset of the rows that is much smaller than the overall number
        # of rows so we only have to run the inefficient query predicates
        # over this smaller number of rows.
        if self.can_evaluate_efficiently(pk_column, query.indexed_columns):
            inefficient_predicates = []
            result = None
            for predicate in self.children:
                if predicate.can_evaluate_efficiently(pk_column, query.indexed_columns):
                    rows = predicate.get_matching_rows(query)
                            
                    if result == None:
                        result = rows
                    else:
                        result = combine_rows(result, rows, self.op, pk_column)
                else:
                    inefficient_predicates.append(predicate)
        else:
            inefficient_predicates = self.children
            result = query.get_all_rows()
        
        if result == None:
            result = []
            
        # Now 
        if len(inefficient_predicates) > 0:
            result = [row for row in result if self.row_matches_subset(row, inefficient_predicates)]
            
        return result

