from django.db import models
from djangotoolbox.fields import ListField

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
        db_table = 'Tag'
        ordering = ['id']

class Test(models.Model):
    test_date = models.DateField(null=True)
    test_datetime = models.DateTimeField(null=True)
    test_time = models.TimeField(null=True)
    test_decimal = models.DecimalField(null=True, max_digits=10, decimal_places=3)
    test_text = models.TextField(null=True)
    #test_list = ListField(models.CharField(max_length=500))
    
    class Meta:
        db_table = 'Test'
        ordering = ['id']



class CompoundKeyModel(models.Model):
    name = models.CharField(max_length=64)
    index = models.IntegerField()
    extra = models.CharField(max_length=32, default='test')
    
    class CassandraSettings:
        COMPOUND_KEY_FIELDS = ('name', 'index')


class CompoundKeyModel2(models.Model):
    slice = models.ForeignKey(Slice)
    name = models.CharField(max_length=64)
    index = models.IntegerField()
    extra = models.CharField(max_length=32)
    
    class CassandraSettings:
        COMPOUND_KEY_FIELDS = ('slice', 'name', 'index')
        COMPOUND_KEY_SEPARATOR = '#'

class CompoundKeyModel3(models.Model):
    name = models.CharField(max_length=32)

    class CassandraSettings:
        COMPOUND_KEY_FIELDS = ('name')
