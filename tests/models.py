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
