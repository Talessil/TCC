import time

from neo4j.v1 import GraphDatabase, basic_auth
driver = GraphDatabase.driver("bolt://localhost", 	auth=basic_auth("neo4j", "1234"))
with driver.session() as session:
	start = time.time()
	############## primeira particao #######################
	session.run('Load csv with headers from "file:/export.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa1)')
	session.run('Load csv with headers from "file:/export.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa2)')
	session.run('Load csv with headers from "file:/export.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	########################## CALL NETSCAN #############################
	#*DBLP*#	

	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 100, 5, 1)')
	#karate	
	#session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 0.05, 14, 1)')
	#protein e artificial
	#session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 0.5, 5, 1)')
	#200data
 	#session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total","OUTGOING",false, 5, 2, 1)')
	
	session.close()

	end = time.time()	
	print(end - start)


