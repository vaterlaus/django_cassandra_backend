from django.db import models

print "Entering tests.models"

# Create your models here.

class Slice(models.Model):
    name = models.CharField(max_length=64)
    
    class Meta:
        db_table = 'Slice'
        ordering = ['id']
        
class Host(models.Model):
    mac = models.CharField(max_length=20, db_index=True)
    ip = models.CharField(max_length=20, db_index = True)
    slice = models.ForeignKey(Slice, db_index=True)
    
    class Meta:
        db_table = 'Host'
        ordering = ['id']
        
class Tag(models.Model):
    name = models.CharField(max_length=64)
    value = models.CharField(max_length=256)
    host = models.ForeignKey(Host, db_index=True)
    
    class Meta:
        ordering = ['id']

class Test(models.Model):
    test_date = models.DateField(null=True)
    test_datetime = models.DateTimeField(null=True)
    test_time = models.TimeField(null=True)
    test_decimal = models.DecimalField(null=True, max_digits=10, decimal_places=3)
    
def add_host(id, ip='foobar', mac="hello", test='django'):
    h = Host(id=id, ip=ip, mac=mac)
    h.save()
    
def test_save():
    h = Host(id=1, ip='foo', mac='bar', test='hello world')
    h.save()

def test_query():
    host_query_set = Host.objects.filter(id=1)
    #host_query_set = host_query_set.filter(id=1)
    for host in host_query_set:
        print "Host: ip = " + host.ip + "; mac = " + host.mac

def ph(host):
    slice_name = host.slice.name if host.slice else 'None'
    print "id:" + host.id + "; ip = " + host.ip + "; mac = " + host.mac + "; slice = " + slice_name

def phqs(qs):
    for host in qs:
        ph(host)

def pa():
    all = Host.objects.all()
    phqs(all)
    
from django_cassandra.db.compiler import *

row1 = {'id':1, 'first': 'John', 'last': 'Smith', 'age': 25}
row2 = {'id':2, 'first': 'David', 'last': 'Jones', 'age':18}
row3 = {'id':3, 'first': 'Prince', 'age':40}
row4 = {'id':4, 'first': 'Steve', 'last': 'Baker', 'age':40}
row5 = {'id':5, 'first': 'Bill', 'last':'White', 'age':7}
row6 = {'id':6, 'first': 'David', 'last': 'Smith', 'age':20}

rows1 = [row1, row2, row3, row4, row5, row6]
rows2 = [row1]
rows3 = [row3, row1, row2, row4]
rows4 = []
rows5 = [row1, row3, row6]

row_spec1 = (('last', False))
row_spec2 = (('last',), ('first',))
row_spec3 = (('age', SORT_DESCENDING), ('last', SORT_ASCENDING), ('first', SORT_ASCENDING))

def print_rows(rows):
    for row in rows:
        print row
