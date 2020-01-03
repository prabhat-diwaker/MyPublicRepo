import random
import os
os.chdir('/Users/p0d00mp/Downloads')

from random import randrange
from datetime import timedelta

def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

from datetime import datetime

d1 = datetime.strptime('10/11/2019 1:30 PM', '%m/%d/%Y %I:%M %p')
d2 = datetime.strptime('10/15/2019 4:50 AM', '%m/%d/%Y %I:%M %p')


a={}


dates = ['2019-08-19','2019-08-20','2019-08-21','2019-08-22']
for i  in xrange(1,50000):
    id = random.randint(12345,999999)
    amount = round(random.uniform(10.0,1235.0),3)


    if id not in a:
        a[id]=amount


sales_id_amount_obj = open('sales_amount.txt','w')
for i in a:
    date = random_date(d1, d2)
    line = str(i)+","+ str(a[i])+","+str(date)
    sales_id_amount_obj.write(line+'\n')

ids = a.keys()


item_obj =  open('item.csv','r')
all_items = item_obj.readlines()
items = [i.replace('\n','').strip() for i in all_items]

store_obj =  open('store.csv','r')
all_store = store_obj.readlines()
stores = [i.replace('\n','').strip()  for i in all_store]

sales_obj = open('sales.txt','w')
for i in xrange(1,45000):
        item_id = random.choice(items)
        store_id = random.choice(stores)
        rand_number = random.randint(1,10)
        sales_ids =[]
        for i in xrange(0,rand_number):
            id=random.choice(ids)
            sales_ids.append(str(id))
            ids.remove(id)
        sales_id_str = ','.join(sales_ids)
        line= str(item_id) + "|" + str(store_id) + "|" + str(sales_id_str)
        sales_obj.write(line+'\n')

print len(a)
print len(a)

