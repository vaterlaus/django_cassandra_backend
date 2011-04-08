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

from django.test import TestCase
from .models import *
import datetime
import decimal
from django.db.models.query import Q

class FieldsTest(TestCase):
    
    TEST_DATE = datetime.date(2007,3,5)
    TEST_DATETIME = datetime.datetime(2010,5,4,9,34,25)
    TEST_DATETIME2 = datetime.datetime(2010, 6, 6, 6, 20)
    TEST_TIME = datetime.time(10,14,29)
    TEST_DECIMAL = decimal.Decimal('33.55')
    TEST_TEXT = "Practice? We're talking about practice?"
    TEST_TEXT2 = "I'm a man. I'm 40."
    #TEST_LIST = [u'aaa',u'bbb',u'foobar',u'snafu',u'hello',u'goodbye']
    
    def setUp(self):
        self.test = Test(id='key1',
                          test_date=self.TEST_DATE,
                          test_datetime=self.TEST_DATETIME,
                          test_time=self.TEST_TIME,
                          test_decimal=self.TEST_DECIMAL,
                          test_text=self.TEST_TEXT
                          #,test_list=self.TEST_LIST
			  )
        self.test.save()
    
    def test_fields(self):
        test1 = Test.objects.get(id='key1')
        self.assertEqual(test1.test_date, self.TEST_DATE)
        self.assertEqual(test1.test_datetime, self.TEST_DATETIME)
        self.assertEqual(test1.test_time, self.TEST_TIME)
        self.assertEqual(test1.test_decimal, self.TEST_DECIMAL)
        self.assertEqual(test1.test_text, self.TEST_TEXT)
        #self.assertEqual(test1.test_list, self.TEST_LIST)
        
        test1.test_datetime = self.TEST_DATETIME2
        test1.test_text = self.TEST_TEXT2
        test1.save()
        
        test1 = Test.objects.get(id='key1')
        self.assertEqual(test1.test_datetime, self.TEST_DATETIME2)
        self.assertEqual(test1.test_text, self.TEST_TEXT2)
        
class BasicFunctionalityTest(TestCase):
    
    HOST_COUNT = 5
    
    def get_host_params_for_index(self, index):
        decimal_index = str(index)
        hex_index = hex(index)[2:]
        if len(hex_index) == 1:
            hex_index = '0' + hex_index
        id = 'key'+decimal_index
        mac = '00:01:02:03:04:'+hex_index
        ip = '10.0.0.'+decimal_index
        slice = self.s0 if index % 2 else self.s1
        
        return id, mac, ip, slice
    
    def setUp(self):
        # Create a couple slices
        self.s0 = Slice(id='key0',name='slice0')
        self.s0.save()
        self.s1 = Slice(id='key1',name='slice1')
        self.s1.save()
        
        # Create some hosts
        for i in range(self.HOST_COUNT):
            id, mac, ip, slice = self.get_host_params_for_index(i)
            h = Host(id=id, mac=mac,ip=ip,slice=slice)
            h.save()
    
            
    def test_create(self):
        """
        Tests that we correctly created the model instances
        """
        
        # Test that we have the slices we expect
        slice_query_set = Slice.objects.all()
        index = 0
        for slice in slice_query_set:
            self.assertEqual(slice.id, 'key' + str(index))
            self.assertEqual(slice.name, 'slice' + str(index))
            index += 1

        # There should have been exactly 2 slices created
        self.assertEqual(index, 2)
        
        host_query_set = Host.objects.all()
        index = 0
        for host in host_query_set:
            id, mac, ip, slice = self.get_host_params_for_index(index)
            index += 1

        # There should have been exactly 2 slices created
        self.assertEqual(index, self.HOST_COUNT)

    def test_update(self):
        s = Slice.objects.get(id='key0')
        s.name = 'foobar'
        s.save()
        #import time
        #time.sleep(5)
        s1 = Slice.objects.get(id='key0')
        #s2 = Slice.objects.get(id='key0')
        self.assertEqual(s1.name, 'foobar')
        #self.assertEqual(s2.name, 'foobar')
    
    def test_delete(self):
        host = Host.objects.get(id='key1')
        host.delete()
        hqs = Host.objects.filter(id='key1')
        count = hqs.count()
        self.assertEqual(count,0)
        
    def test_default_id(self):
        s = Slice(name='slice2')
        s.save()
        s2 = Slice.objects.get(name='slice2')
        self.assertEqual(s2.name, 'slice2')
        
SLICE_DATA_1 = ('key1', 'PCI')
SLICE_DATA_2 = ('key2', 'Eng1')
SLICE_DATA_3 = ('key3', 'Finance')
SLICE_DATA_4 = ('key4', 'blue')
SLICE_DATA_5 = ('key5', 'bluf')
SLICE_DATA_6 = ('key6', 'BLTSE')
SLICE_DATA_7 = ('key7', 'ZNCE')
SLICE_DATA_8 = ('key8', 'UNCLE')
SLICE_DATA_9 = ('key9', 'increment')

HOST_DATA_1 = ('key1', '00:01:02:03:04:05', '10.0.0.1', 'key1', (('foo3', 'bar3'), ('foo1','hello'), ('aaa', 'bbb')))
HOST_DATA_2 = ('key2', 'ff:fc:02:33:04:05', '192.168.0.55', 'key2', None)
HOST_DATA_3 = ('key3', 'ff:fc:02:03:04:01', '192.168.0.1', 'key2', (('cfoo3', 'bar3'), ('cfoo1','hello'), ('ddd', 'bbb')))
HOST_DATA_4 = ('key4', '55:44:33:03:04:05', '10.0.0.6', 'key1',None)
HOST_DATA_5 = ('key5', '10:01:02:03:04:05', '10.0.0.2', 'key1', None)
HOST_DATA_6 = ('key6', '33:44:55:03:04:05', '10.0.0.7', 'key3',None)
HOST_DATA_7 = ('key7', '10:01:02:03:04:05', '192.168.0.44', 'key1', None)

def create_slices(slice_data_list):
    for sd in slice_data_list:
        id, name = sd
        s = Slice(id=id,name=name)
        s.save()

def create_hosts(host_data_list):
    for hd in host_data_list:
        id,mac,ip,slice_id,tag_list = hd
        slice = Slice.objects.get(id=slice_id)
        h = Host(id=id,mac=mac,ip=ip,slice=slice)
        h.save()
        if tag_list != None:
            for tag in tag_list:
                name, value = tag
                t = Tag(name=name,value=value,host=h)
                t.save()
    
class QueryTest(TestCase):

    def setUp(self):
        create_slices((SLICE_DATA_1, SLICE_DATA_2, SLICE_DATA_3))
        create_hosts((HOST_DATA_1, HOST_DATA_6, HOST_DATA_5, HOST_DATA_7, HOST_DATA_3, HOST_DATA_2, HOST_DATA_4))
        
    def check_host_data(self, host, data):
        expected_id, expected_mac, expected_ip, expected_slice, expected_tag_list = data
        self.assertEqual(host.id, expected_id)
        self.assertEqual(host.mac, expected_mac)
        self.assertEqual(host.ip, expected_ip)
        self.assertEqual(host.slice.id, expected_slice)
        # TODO: For now we don't check the tag list
        
    def test_pk_query(self):
        h = Host.objects.get(id='key3')
        self.check_host_data(h, HOST_DATA_3)
    
        hqs = Host.objects.filter(id='key6')
        count = hqs.count()
        self.assertEqual(count, 1)
        h6 = hqs[0]
        self.check_host_data(h6, HOST_DATA_6)
    
        hqs = Host.objects.filter(id__gt='key4')
        count = hqs.count()
        self.assertEqual(count, 3)
        h5, h6, h7 = hqs[:]
        self.check_host_data(h5, HOST_DATA_5)
        self.check_host_data(h6, HOST_DATA_6)
        self.check_host_data(h7, HOST_DATA_7)
        
        hqs = Host.objects.filter(id__lte='key3')
        count = hqs.count()
        self.assertEqual(count, 3)
        h1, h2, h3 = hqs[:]
        self.check_host_data(h1, HOST_DATA_1)
        self.check_host_data(h2, HOST_DATA_2)
        self.check_host_data(h3, HOST_DATA_3)
        
        hqs = Host.objects.filter(id__gte='key3', id__lt='key7')
        count = hqs.count()
        self.assertEqual(count, 4)
        h3, h4, h5, h6 = hqs[:]
        self.check_host_data(h3, HOST_DATA_3)
        self.check_host_data(h4, HOST_DATA_4)
        self.check_host_data(h5, HOST_DATA_5)
        self.check_host_data(h6, HOST_DATA_6)
        
    def test_indexed_query(self):
        h = Host.objects.get(ip='10.0.0.7')
        self.check_host_data(h, HOST_DATA_6)
        
        hqs = Host.objects.filter(ip='192.168.0.1')
        h = hqs[0]
        self.check_host_data(h, HOST_DATA_3)
    
    def test_complex_query(self):
        hqs = Host.objects.filter(Q(id='key1') | Q(id='key3') | Q(id='key4')).order_by('id')
        count = hqs.count()
        self.assertEqual(count, 3)
        h1, h3, h4 = hqs[:]
        self.check_host_data(h1, HOST_DATA_1)
        self.check_host_data(h3, HOST_DATA_3)
        self.check_host_data(h4, HOST_DATA_4)

        s1 = Slice.objects.get(id='key1')
        
        hqs = Host.objects.filter(ip__startswith='10.', slice=s1)
        count = hqs.count()
        self.assertEqual(count, 3)
        h1, h4, h5 = hqs[:]
        self.check_host_data(h1, HOST_DATA_1)
        self.check_host_data(h4, HOST_DATA_4)
        self.check_host_data(h5, HOST_DATA_5)

        hqs = Host.objects.filter(ip='10.0.0.6', slice=s1)
        count = hqs.count()
        self.assertEqual(count, 1)
        h4 = hqs[0]
        self.check_host_data(h4, HOST_DATA_4)

        tqs = Tag.objects.filter(name='foo3', value='bar3')
        self.assertEqual(tqs.count(), 1)
        t = tqs[0]
        self.assertEqual(t.name, 'foo3')
        self.assertEqual(t.value, 'bar3')
        self.assertEqual(t.host_id, 'key1')
        
        hqs = Host.objects.filter((Q(ip__startswith='10.0') & Q(slice=s1)) | Q(mac__startswith='ff')).order_by('id')
        count = hqs.count()
        self.assertEqual(count, 5)
        h1, h2, h3, h4, h5 = hqs[:]
        self.check_host_data(h1, HOST_DATA_1)
        self.check_host_data(h2, HOST_DATA_2)
        self.check_host_data(h3, HOST_DATA_3)
        self.check_host_data(h4, HOST_DATA_4)
        self.check_host_data(h5, HOST_DATA_5)

    def test_exclude_query(self):
        hqs = Host.objects.exclude(ip__startswith="10")
        count = hqs.count()
        self.assertEqual(count,3)
        h2, h3, h7 = hqs[:]
        self.check_host_data(h2, HOST_DATA_2)
        self.check_host_data(h3, HOST_DATA_3)
        self.check_host_data(h7, HOST_DATA_7)

    def test_count(self):
        
        count = Host.objects.count()
        self.assertEqual(count, 7)
        
        count = Host.objects.all().count()
        self.assertEqual(count, 7)
        
        slice1 = Slice.objects.get(id='key1')
        qs = Host.objects.filter(slice=slice1)
        count = qs.count()
        #if count == 4:
        #    h1,h4,h5,h7 = qs[:]
        #else:
        #    h1,h4,h5,h7,h = qs[:]
        self.assertEqual(count, 4)

        qs = Slice.objects.filter(name__startswith='P')
        count = qs.count()
        self.assertEqual(count, 1)
        
        qs = Host.objects.filter(ip__startswith='10').order_by('slice_id')
        count = qs.count()
        self.assertEqual(count, 4)
    
    def test_query_set_slice(self):
        hqs = Host.objects.all()[2:6]
        count = hqs.count()
        h3, h4, h5, h6 = hqs[:]
        self.assertEqual(h3.id, 'key3')
        self.assertEqual(h4.id, 'key4')
        self.assertEqual(h5.id, 'key5')
        self.assertEqual(h6.id, 'key6')
        
    def test_order_by(self):
        # Test ascending order of all of the hosts
        qs = Host.objects.all().order_by('ip')
        h1, h2, h3, h4, h5, h6, h7 = qs[:]
        self.assertEqual(h1.id, 'key1')
        self.assertEqual(h2.id, 'key5')
        self.assertEqual(h3.id, 'key4')
        self.assertEqual(h4.id, 'key6')
        self.assertEqual(h5.id, 'key3')
        self.assertEqual(h6.id, 'key7')
        self.assertEqual(h7.id, 'key2')
        
        # Test descending order of all of the hosts
        qs = Host.objects.all().order_by('-ip')
        h1, h2, h3, h4, h5, h6, h7 = qs[:]
        self.assertEqual(h1.id, 'key2')
        self.assertEqual(h2.id, 'key7')
        self.assertEqual(h3.id, 'key3')
        self.assertEqual(h4.id, 'key6')
        self.assertEqual(h5.id, 'key4')
        self.assertEqual(h6.id, 'key5')
        self.assertEqual(h7.id, 'key1')

        # Test multiple ordering criteria
        qs = Host.objects.all().order_by('slice_id', 'ip')
        h1, h2, h3, h4, h5, h6, h7 = qs[:]
        self.assertEqual(h1.id, 'key1')
        self.assertEqual(h2.id, 'key5')
        self.assertEqual(h3.id, 'key4')
        self.assertEqual(h4.id, 'key7')
        self.assertEqual(h5.id, 'key3')
        self.assertEqual(h6.id, 'key2')
        self.assertEqual(h7.id, 'key6')

        # Test multiple ordering criteria
        qs = Host.objects.all().order_by('-slice_id', 'ip')
        h1, h2, h3, h4, h5, h6, h7 = qs[:]
        self.assertEqual(h1.id, 'key6')
        self.assertEqual(h2.id, 'key3')
        self.assertEqual(h3.id, 'key2')
        self.assertEqual(h4.id, 'key1')
        self.assertEqual(h5.id, 'key5')
        self.assertEqual(h6.id, 'key4')
        self.assertEqual(h7.id, 'key7')

        # Currently the nonrel code doesn't handle ordering that spans tables/column families
        #=======================================================================
        # qs = Host.objects.all().order_by('slice__name', 'id')
        # h2, h3, h6, h1, h5, h4, h7 = qs[:]
        # self.assertEqual(h2.id, 'key2')
        # self.assertEqual(h3.id, 'key3')
        # self.assertEqual(h6.id, 'key6')
        # self.assertEqual(h1.id, 'key1')
        # self.assertEqual(h5.id, 'key5')
        # self.assertEqual(h4.id, 'key4')
        # self.assertEqual(h7.id, 'key7')
        #=======================================================================


class OperationTest(TestCase):

    def setUp(self):
        create_slices((SLICE_DATA_1, SLICE_DATA_2, SLICE_DATA_3, SLICE_DATA_4, SLICE_DATA_5,
                       SLICE_DATA_6, SLICE_DATA_7, SLICE_DATA_8, SLICE_DATA_9))
    
    def test_range_ops(self):
        qs = Slice.objects.filter(name__gt='PCI')
        count = qs.count()
        self.assertEqual(count, 5)
        s4,s5,s7,s8,s9 = qs[:]
        self.assertEqual(s4.id,'key4')
        self.assertEqual(s5.id,'key5')
        self.assertEqual(s7.id,'key7')
        self.assertEqual(s8.id,'key8')
        self.assertEqual(s9.id,'key9')
        
        qs = Slice.objects.filter(name__gte='bluf',name__lte='bluf')
        count = qs.count()
        self.assertEqual(count, 1)
        s5 = qs[0]
        self.assertEqual(s5.id, 'key5')
        
        qs = Slice.objects.filter(name__gt='blue', name__lte='bluf')
        count = qs.count()
        self.assertEqual(count, 1)
        s5 = qs[0]
        self.assertEqual(s5.id, 'key5')
        
        qs = Slice.objects.filter(name__exact='blue')
        count = qs.count()
        self.assertEqual(count, 1)
        s4 = qs[0]
        self.assertEqual(s4.id, 'key4')

    def test_other_ops(self):
        
        qs = Slice.objects.filter(id__in=['key1','key4','key6','key9'])
        count = qs.count()
        self.assertEqual(count, 4)
        s1,s4,s6,s9 = qs[:]
        self.assertEqual(s1.id,'key1')
        self.assertEqual(s4.id,'key4')
        self.assertEqual(s6.id,'key6')
        self.assertEqual(s9.id,'key9')
        
        qs = Slice.objects.filter(name__startswith='bl')
        count = qs.count()
        self.assertEqual(count, 2)
        s4,s5 = qs[:]
        self.assertEqual(s4.id,'key4')
        self.assertEqual(s5.id,'key5')
        
        qs = Slice.objects.filter(name__endswith='E')
        count = qs.count()
        self.assertEqual(count, 3)
        s6,s7,s8 = qs[:]
        self.assertEqual(s6.id,'key6')
        self.assertEqual(s7.id,'key7')
        self.assertEqual(s8.id,'key8')
        
        qs = Slice.objects.filter(name__contains='NC')
        count = qs.count()
        self.assertEqual(count, 2)
        s7,s8 = qs[:]
        self.assertEqual(s7.id,'key7')
        self.assertEqual(s8.id,'key8')

        qs = Slice.objects.filter(name__istartswith='b')
        count = qs.count()
        self.assertEqual(count, 3)
        s4,s5,s6 = qs[:]
        self.assertEqual(s4.id,'key4')
        self.assertEqual(s5.id,'key5')
        self.assertEqual(s6.id,'key6')

        qs = Slice.objects.filter(name__istartswith='B')
        count = qs.count()
        self.assertEqual(count, 3)
        s4,s5,s6 = qs[:]
        self.assertEqual(s4.id,'key4')
        self.assertEqual(s5.id,'key5')
        self.assertEqual(s6.id,'key6')

        qs = Slice.objects.filter(name__iendswith='e')
        count = qs.count()
        self.assertEqual(count, 5)
        s3,s4,s6,s7,s8 = qs[:]
        self.assertEqual(s3.id,'key3')
        self.assertEqual(s4.id,'key4')
        self.assertEqual(s6.id,'key6')
        self.assertEqual(s7.id,'key7')
        self.assertEqual(s8.id,'key8')

        qs = Slice.objects.filter(name__icontains='nc')
        count = qs.count()
        self.assertEqual(count, 4)
        s3,s7,s8,s9 = qs[:]
        self.assertEqual(s3.id,'key3')
        self.assertEqual(s7.id,'key7')
        self.assertEqual(s8.id,'key8')
        self.assertEqual(s9.id,'key9')

        qs = Slice.objects.filter(name__regex='[PEZ].*')
        count = qs.count()
        self.assertEqual(count, 3)
        s1,s2,s7 = qs[:]
        self.assertEqual(s1.id,'key1')
        self.assertEqual(s2.id,'key2')
        self.assertEqual(s7.id,'key7')

        qs = Slice.objects.filter(name__iregex='bl.*e')
        count = qs.count()
        self.assertEqual(count, 2)
        s4,s6 = qs[:]
        self.assertEqual(s4.id,'key4')
        self.assertEqual(s6.id,'key6')
