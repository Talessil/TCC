from neo4j.v1 import GraphDatabase, basic_auth
driver = GraphDatabase.driver("bolt://localhost", 	auth=basic_auth("neo4j", "1234"))
with driver.session() as session:

	##################### RESET #######################
	session.run('Match (a)-[b]->(c) DELETE a,b,c')
	session.run('Match (a) DELETE a')

	session.close()

