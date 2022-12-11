from neo4j.v1 import GraphDatabase, basic_auth
driver = GraphDatabase.driver("bolt://localhost", 	auth=basic_auth("neo4j", "1234"))
with driver.session() as session:

	##################### RESET #######################
	session.run('MATCH (a:Cluster)-[r]->(b:Pessoa) DELETE r')
	session.run('MATCH(a:Cluster) DELETE a')
	session.run('MATCH (a:Pessoa) REMOVE a.noise,a.expanded,a.core')

	session.close()

