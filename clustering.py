import time

from neo4j.v1 import GraphDatabase, basic_auth
driver = GraphDatabase.driver("bolt://localhost", 	auth=basic_auth("neo4j", "1234"))
with driver.session() as session:



    ########################## CALL NETSCAN #############################
	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 1, 4, 1)') #eps, minPnts, radius


	
	session.close()
