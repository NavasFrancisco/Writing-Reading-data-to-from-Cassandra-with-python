# Writing-Reading-data-to-from-Cassandra-with-python

Data used as example in the code can be downloaded from: https://data.enedis.fr/explore/dataset/conso-inf36/information/?disjunctive.profil&disjunctive.plage_de_puissance_souscrite&refine.profil=ENT1+(%2B+ENT2)&sort=horodate

Cassandra Docker image (https://hub.docker.com/_/cassandra) is used in a local computer. Configuration parameters could be modified in the code to connect to any cassandra cluster. 

My cassandra cluster is running before using the following codes:

docker run --name akka-cassandra -v C:\Users\Administrateur\Desktop\UserCaseEnedis\cassandra:/var/lib/cassandra -p 9042:9042 -d --rm cassandra

This repository contains two different code: 

Keyspace is equivalent to the database name.
Each keyspace can contain x tables or column family. Table metadata needs to be defined before entering data into the table.

writing_data_to_cassandra.py for writing data into the cassandra cluster. 
read_data_from_cassandra.py for reading data from the cassandra cluster.
