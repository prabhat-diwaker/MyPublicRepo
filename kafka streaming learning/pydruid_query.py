from pydruid import *
from pydruid.client import *
from pylab import plt
from pydruid.query import QueryBuilder
from pydruid.utils.postaggregator import *
from pydruid.utils.aggregators import *
from pydruid.utils.filters import *

from pydruid.client import *
from pylab import plt
query = PyDruid("http://localhost:8888", 'druid/v2')
ts = query.timeseries(
datasource='store-label-schema_test',
granularity='hour',
intervals='2019-07-11/2019-08-11',
aggregations={'sellPrice': doublesum('sellPrice'), 'wasPrice': doublesum('wasPrice')},
post_aggregations={'ratio': (Field('sellPrice') / Field('wasPrice'))},
filter=Dimension('action') == 'PENDING'
)

df = query.export_pandas()
print df

df['timestamp'] = df['timestamp'].map(lambda x: x.split('T')[0])
df.plot(x='sellPrice', y='wasPrice',
        title='Sochi 2014')
plt.ylabel('avg tweet length (chars)')
plt.show()