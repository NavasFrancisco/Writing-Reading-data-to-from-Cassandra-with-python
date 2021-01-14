## Import cassandra cluster python library
from cassandra.cluster import Cluster

# Local folder where  you are getting the data from

data_local_address = "C:/Users/Administrateur/Desktop/UserCaseEnedis/donneesEnedis.csv"


cluster = Cluster()
session = cluster.connect() #Connexion with Cassandra

# Use session.execute to use Cqlsh

session.execute("""CREATE KEYSPACE IF NOT EXISTS usecasedb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}""")
session.execute("use usecasedb")
print("KEYSPACE successfully created")

# Create metadata for table

session.execute("""CREATE COLUMNFAMILY IF NOT EXISTS enedis_table(
             "Horodate" varchar PRIMARY KEY,
             "Region" varchar,
             "Code_region" varchar,
             "Profil" varchar,
             "Plage_de_puissance_souscrite" varchar,
             "Nb_points_soutirage" varchar,
             "Total_energie_soutiree" varchar,
             "Courbe_Moyenne_1" varchar,
             "Indice_representativite_Courbe_1" varchar,
             "Courbe_Moyenne_2" varchar,
             "Indice_representativite_Courbe_2" varchar,
             "Courbe_Moyenne_1__2"  varchar,
             "Indice_representativite_Courbe_1__2" varchar,
             "Jour_max_du_mois" varchar,
             "Semaine_max_du_mois" varchar)""")

print("enedis_table successfully created") 

## Then load the table with data from data_local_address

prepared = session.prepare("""INSERT INTO enedis_table(
    "Horodate",
    "Region",
    "Code_region",
    "Profil",
    "Plage_de_puissance_souscrite",
    "Nb_points_soutirage",
    "Total_energie_soutiree",
    "Courbe_Moyenne_1",
    "Indice_representativite_Courbe_1",
    "Courbe_Moyenne_2",
    "Indice_representativite_Courbe_2",
    "Courbe_Moyenne_1__2" ,
    "Indice_representativite_Courbe_1__2",
    "Jour_max_du_mois",
    "Semaine_max_du_mois")  
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """)


with open(data_local_address,	"r") as rows:
    i = 0
    for row in rows:
        if i == 1:

            columns = row.split(";")
            Horodate = columns[0]
            Region  = columns[1]
            Code_region  = columns[2]
            Profil  = columns[3]
            Plage_de_puissance_souscrite  = columns[4]
            Nb_points_soutirage  = columns[5]
            Total_energie_soutiree  = columns[6]
            Courbe_Moyenne_1  = columns[7]
            Indice_representativite_Courbe_1  = columns[8]
            Courbe_Moyenne_2  = columns[9]
            Indice_representativite_Courbe_2  = columns[10]
            Courbe_Moyenne_1__2  = columns[11]
            Indice_representativite_Courbe_1__2  = columns[12]
            Jour_max_du_mois  = columns[13]
            Semaine_max_du_mois  = columns[14]
            session.execute(prepared, [Horodate,Region,Code_region,Profil,Plage_de_puissance_souscrite,Nb_points_soutirage,Total_energie_soutiree,Courbe_Moyenne_1,Indice_representativite_Courbe_1,Courbe_Moyenne_2,Indice_representativite_Courbe_2,Courbe_Moyenne_1__2,Indice_representativite_Courbe_1__2,Jour_max_du_mois,Semaine_max_du_mois])
            
        i = 1
        
print("implementation of enedis_table done with success")

#closing Cassandra connection
session.shutdown()

		