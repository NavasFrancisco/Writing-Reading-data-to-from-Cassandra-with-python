import pandas as pd
from cassandra.cluster import Cluster

## Connect to cassandra cluster
cluster = Cluster()
session = cluster.connect('usecasedb') # Connection to a specific database or keyspace
rowsEnedis = session.execute('SELECT * FROM enedis_table') #Select all the data from the table using cqlsh


dfEnedis = pd.DataFrame(rowsEnedis) #Data is translated to pandas DataFrame

session.shutdown() #Closing Cassandra connection



print(dfEnedis.head()) #Print DataFrame head

    
