import time

from neo4j.v1 import GraphDatabase, basic_auth
driver = GraphDatabase.driver("bolt://localhost", 	auth=basic_auth("neo4j", "1234"))
with driver.session() as session:
	start = time.time()

	############## primeira particao #######################
	session.run('Load csv with headers from "file:/particao1.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa1)')
	session.run('Load csv with headers from "file:/particao1.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa2)')
	session.run('Load csv with headers from "file:/particao1.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	########################## CALL NETSCAN #############################	
	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 100, 5, 1)')

	############## Salvar valores ##########################
		#salva nos pessoas e atributos#
	result = session.run("MATCH (p:Pessoa) RETURN p.idpessoa AS idpessoa, p.noise AS noise, p.expanded AS expanded, p.core AS core")
		#salva nos grupos e atributos
	grupos = session.run("MATCH (c:Cluster) RETURN c.id AS id, c.firstcore AS firstcore, c.tag AS tag")
		#salva relacao dos grupos e pessoas
	agrupados = session.run('MATCH (n:Cluster)-[r:CONTAINS]-(p:Pessoa) RETURN n.id AS idgrupo,p.idpessoa AS idpessoa')

	


	data = result.data()  #pode dar exception se o resultado for None
	for record in data:
		if record["core"] is None:
			record["core"] = 'null'
		if record["expanded"] is None:
			record["expanded"] = 'null'
		if record["noise"] is None:
			record["noise"] = 'null'
	
	data2 = grupos.data()
	data3 = agrupados.data()


	##################### RESET #######################
	session.run('Match (a)-[b]->(c) DELETE a,b,c')
	session.run('Match (a) DELETE a')

	session.close()

	end = time.time()
	print(end - start)
	

with driver.session() as session:
	
	start2 = time.time()
	############## segunda particao #######################
	session.run('Load csv with headers from "file:/particao2.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa1)')
	session.run('Load csv with headers from "file:/particao2.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa2)')
	session.run('Load csv with headers from "file:/particao2.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	########################## CALL NETSCAN #############################
	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 100, 5, 1)')

	session.close()

	end2 = time.time()
	print(end2 - start2)


with driver.session() as session:
	
	start3 = time.time()
	############## Jogar os nos da particao 1 em conjunto com a particao 2 ##############	
	for record in data:
		session.run('MERGE (u:Pessoa {idpessoa:'+str(record["idpessoa"])+'}) ON CREATE SET u.idpessoa ='+str(record["idpessoa"])+', u.expanded ='+str(record["expanded"])+', u.core = '+str(record["core"])+', u.noise = '+str(record["noise"])+'')

	for record in data2:
		session.run('CREATE (n:Cluster {id:'+str(record["id"])+',firstcore:'+str(record["firstcore"])+'})')
	
	############### Inserir arestas dos nos da particao 1 ################################
	session.run('Load csv with headers from "file:/particao1.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	
	############### Inserir relacao de grupos e pessoas da particao 1 ####################
	for record in data3:
		session.run('MATCH (a:Pessoa {idpessoa:'+str(record["idpessoa"])+'}),(b:Cluster{id:'+str(record["idgrupo"])+'}) create (b)-[r:CONTAINS]->(a)')
	
	########### Remover atributos dos nos presentes no arquivo arestas cortadas ##########
	session.run('Load csv with headers from "file:/arestas_cortadas.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) REMOVE a.expanded,a.core,a.noise,b.expanded,b.core,b.noise')
		########### Remover ligacao do cluster para esses nos - nao ta funfando ############
	
	################ Inserir arestas presentes no arquivo arestas cortadas ###############
	session.run('Load csv with headers from "file:/arestas_cortadas.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')

	########################## CALL NETSCAN #############################
	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 100, 5, 1)')


	session.close()

	end3 = time.time()
	print(end3 - start3)

