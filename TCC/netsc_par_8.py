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
	

#--------------------------------------------------------------------------------------------------------------------------------------------------------------------#


with driver.session() as session:
	
	start = time.time()
	############## segunda particao #######################
	session.run('Load csv with headers from "file:/particao2.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa1)')
	session.run('Load csv with headers from "file:/particao2.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa2)')
	session.run('Load csv with headers from "file:/particao2.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	########################## CALL NETSCAN #############################
	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 100, 5, 1)')

	session.close()

	end = time.time()
	print(end - start)



#--------------------------------------------------------------------------------------------------------------------------------------------------------------------#


with driver.session() as session:
	
	start = time.time()
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
	session.run('Load csv with headers from "file:/arestas_cortadas_1.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) REMOVE a.expanded,a.core,a.noise,b.expanded,b.core,b.noise')
		########### Remover ligacao do cluster para esses nos - nao ta funfando ############
	
	################ Inserir arestas presentes no arquivo arestas cortadas ###############
	session.run('Load csv with headers from "file:/arestas_cortadas_1.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')

	########################## CALL NETSCAN #############################
	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 100, 5, 1)')

	############## Salvar valores ##########################
		#salva nos pessoas e atributos#
	result = session.run("MATCH (p:Pessoa) RETURN p.idpessoa AS idpessoa, p.noise AS noise, p.expanded AS expanded, p.core AS core")
		#salva nos grupos e atributos
	grupos = session.run("MATCH (c:Cluster) RETURN c.id AS id, c.firstcore AS firstcore, c.tag AS tag")
		#salva relacao dos grupos e pessoas
	agrupados = session.run('MATCH (n:Cluster)-[r:CONTAINS]-(p:Pessoa) RETURN n.id AS idgrupo,p.idpessoa AS idpessoa')


	data7 = result.data()  #pode dar exception se o resultado for None
	for record in data7:
		if record["core"] is None:
			record["core"] = 'null'
		if record["expanded"] is None:
			record["expanded"] = 'null'
		if record["noise"] is None:
			record["noise"] = 'null'
	
	data8 = grupos.data()
	data9 = agrupados.data()

	##################### RESET #######################
	session.run('Match (a)-[b]->(c) DELETE a,b,c')
	session.run('Match (a) DELETE a')

	session.close()

	end = time.time()
	print(end - start)

#----------------------------------------------------------****************** NOVO ******************-------------------------------------------------------------------#


with driver.session() as session:
	start = time.time()

	############## primeira particao #######################
	session.run('Load csv with headers from "file:/particao3.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa1)')
	session.run('Load csv with headers from "file:/particao3.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa2)')
	session.run('Load csv with headers from "file:/particao3.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	
	########################## CALL NETSCAN #############################	
	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 100, 5, 1)')

	############## Salvar valores ##########################
		#salva nos pessoas e atributos#
	result = session.run("MATCH (p:Pessoa) RETURN p.idpessoa AS idpessoa, p.noise AS noise, p.expanded AS expanded, p.core AS core")
		#salva nos grupos e atributos
	grupos = session.run("MATCH (c:Cluster) RETURN c.id AS id, c.firstcore AS firstcore, c.tag AS tag")
		#salva relacao dos grupos e pessoas
	agrupados = session.run('MATCH (n:Cluster)-[r:CONTAINS]-(p:Pessoa) RETURN n.id AS idgrupo,p.idpessoa AS idpessoa')


	data4 = result.data()  #pode dar exception se o resultado for None
	for record in data4:
		if record["core"] is None:
			record["core"] = 'null'
		if record["expanded"] is None:
			record["expanded"] = 'null'
		if record["noise"] is None:
			record["noise"] = 'null'
	
	data5 = grupos.data()
	data6 = agrupados.data()


	##################### RESET #######################
	session.run('Match (a)-[b]->(c) DELETE a,b,c')
	session.run('Match (a) DELETE a')

	session.close()

	end = time.time()
	print(end - start)
	

#--------------------------------------------------------------------------------------------------------------------------------------------------------------------#


with driver.session() as session:
	
	start = time.time()
	############## segunda particao #######################
	session.run('Load csv with headers from "file:/particao4.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa1)')
	session.run('Load csv with headers from "file:/particao4.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa2)')
	session.run('Load csv with headers from "file:/particao4.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	########################## CALL NETSCAN #############################
	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 100, 5, 1)')

	session.close()

	end = time.time()
	print(end - start)



#--------------------------------------------------------------------------------------------------------------------------------------------------------------------#


with driver.session() as session:
	
	start = time.time()
	############## Jogar os nos da particao 3 em conjunto com a particao 4 ##############	
	for record in data4:
		session.run('MERGE (u:Pessoa {idpessoa:'+str(record["idpessoa"])+'}) ON CREATE SET u.idpessoa ='+str(record["idpessoa"])+', u.expanded ='+str(record["expanded"])+', u.core = '+str(record["core"])+', u.noise = '+str(record["noise"])+'')

	for record in data5:
		session.run('CREATE (n:Cluster {id:'+str(record["id"])+',firstcore:'+str(record["firstcore"])+'})')
	
	############### Inserir arestas dos nos da particao 3 ################################
	session.run('Load csv with headers from "file:/particao3.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	
	############### Inserir relacao de grupos e pessoas da particao 3 ####################
	for record in data6:
		session.run('MATCH (a:Pessoa {idpessoa:'+str(record["idpessoa"])+'}),(b:Cluster{id:'+str(record["idgrupo"])+'}) create (b)-[r:CONTAINS]->(a)')
	
	########### Remover atributos dos nos presentes no arquivo arestas cortadas ##########
	session.run('Load csv with headers from "file:/arestas_cortadas_2.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) REMOVE a.expanded,a.core,a.noise,b.expanded,b.core,b.noise')
		########### Remover ligacao do cluster para esses nos - nao ta funfando ############
	
	################ Inserir arestas presentes no arquivo arestas cortadas ###############
	session.run('Load csv with headers from "file:/arestas_cortadas_2.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')

	########################## CALL NETSCAN #############################
	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 100, 5, 1)')

	

	session.close()

	end = time.time()
	print(end - start)








#--------------------------------------------------------------------- FINAL --------------------------------------------------------------------------------#


with driver.session() as session:
	
	start = time.time()
	############## Jogar os nos da particao A em conjunto com a particao B ##############	
	for record in data7:
		session.run('MERGE (u:Pessoa {idpessoa:'+str(record["idpessoa"])+'}) ON CREATE SET u.idpessoa ='+str(record["idpessoa"])+', u.expanded ='+str(record["expanded"])+', u.core = '+str(record["core"])+', u.noise = '+str(record["noise"])+'')

	for record in data8:
		session.run('CREATE (n:Cluster {id:'+str(record["id"])+',firstcore:'+str(record["firstcore"])+'})')
	
	############### Inserir arestas dos nos da particao 1 e 2 ################################
	session.run('Load csv with headers from "file:/particao1.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	session.run('Load csv with headers from "file:/particao2.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	################ Inserir arestas presentes no arquivo arestas cortadas 1 ###############
	session.run('Load csv with headers from "file:/arestas_cortadas_1.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')

	
	############### Inserir relacao de grupos e pessoas da particao 3 ####################
	for record in data9:
		session.run('MATCH (a:Pessoa {idpessoa:'+str(record["idpessoa"])+'}),(b:Cluster{id:'+str(record["idgrupo"])+'}) create (b)-[r:CONTAINS]->(a)')
	
	########### Remover atributos dos nos presentes no arquivo arestas cortadas ##########
	session.run('Load csv with headers from "file:/arestas_cortadas_11.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) REMOVE a.expanded,a.core,a.noise,b.expanded,b.core,b.noise')
		########### Remover ligacao do cluster para esses nos - nao ta funfando ############
	
	################ Inserir arestas presentes no arquivo arestas cortadas ###############
	session.run('Load csv with headers from "file:/arestas_cortadas_11.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')

	########################## CALL NETSCAN #############################
	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 100, 5, 1)')

	############## Salvar valores ##########################
		#salva nos pessoas e atributos#
	result = session.run("MATCH (p:Pessoa) RETURN p.idpessoa AS idpessoa, p.noise AS noise, p.expanded AS expanded, p.core AS core")
		#salva nos grupos e atributos
	grupos = session.run("MATCH (c:Cluster) RETURN c.id AS id, c.firstcore AS firstcore, c.tag AS tag")
		#salva relacao dos grupos e pessoas
	agrupados = session.run('MATCH (n:Cluster)-[r:CONTAINS]-(p:Pessoa) RETURN n.id AS idgrupo,p.idpessoa AS idpessoa')


	data10 = result.data()  #pode dar exception se o resultado for None
	for record in data10:
		if record["core"] is None:
			record["core"] = 'null'
		if record["expanded"] is None:
			record["expanded"] = 'null'
		if record["noise"] is None:
			record["noise"] = 'null'
	
	data11 = grupos.data()
	data12 = agrupados.data()

	##################### RESET #######################
	session.run('Match (a)-[b]->(c) DELETE a,b,c')
	session.run('Match (a) DELETE a')
	

	session.close()

	end = time.time()
	print(end - start)



#####################################################################################################################################################################
#####################################################################################################################################################################
#####################################################################################################################################################################
###########################################################  8 particoes  ###########################################################################################
#####################################################################################################################################################################
#####################################################################################################################################################################
#####################################################################################################################################################################


with driver.session() as session:
	start = time.time()

	############## primeira particao #######################
	session.run('Load csv with headers from "file:/particao5.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa1)')
	session.run('Load csv with headers from "file:/particao5.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa2)')
	session.run('Load csv with headers from "file:/particao5.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	
	########################## CALL NETSCAN #############################	
	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 100, 5, 1)')

	############## Salvar valores ##########################
		#salva nos pessoas e atributos#
	result = session.run("MATCH (p:Pessoa) RETURN p.idpessoa AS idpessoa, p.noise AS noise, p.expanded AS expanded, p.core AS core")
		#salva nos grupos e atributos
	grupos = session.run("MATCH (c:Cluster) RETURN c.id AS id, c.firstcore AS firstcore, c.tag AS tag")
		#salva relacao dos grupos e pessoas
	agrupados = session.run('MATCH (n:Cluster)-[r:CONTAINS]-(p:Pessoa) RETURN n.id AS idgrupo,p.idpessoa AS idpessoa')


	data13 = result.data()  #pode dar exception se o resultado for None
	for record in data13:
		if record["core"] is None:
			record["core"] = 'null'
		if record["expanded"] is None:
			record["expanded"] = 'null'
		if record["noise"] is None:
			record["noise"] = 'null'
	
	data14 = grupos.data()
	data15 = agrupados.data()


	##################### RESET #######################
	session.run('Match (a)-[b]->(c) DELETE a,b,c')
	session.run('Match (a) DELETE a')

	session.close()

	end = time.time()
	print(end - start)
	

#--------------------------------------------------------------------------------------------------------------------------------------------------------------------#


with driver.session() as session:
	
	start = time.time()
	############## segunda particao #######################
	session.run('Load csv with headers from "file:/particao6.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa1)')
	session.run('Load csv with headers from "file:/particao6.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa2)')
	session.run('Load csv with headers from "file:/particao6.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	########################## CALL NETSCAN #############################
	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 100, 5, 1)')

	session.close()

	end = time.time()
	print(end - start)



#--------------------------------------------------------------------------------------------------------------------------------------------------------------------#


with driver.session() as session:
	
	start = time.time()
	############## Jogar os nos da particao 5 em conjunto com a particao 6 ##############	
	for record in data13:
		session.run('MERGE (u:Pessoa {idpessoa:'+str(record["idpessoa"])+'}) ON CREATE SET u.idpessoa ='+str(record["idpessoa"])+', u.expanded ='+str(record["expanded"])+', u.core = '+str(record["core"])+', u.noise = '+str(record["noise"])+'')

	for record in data14:
		session.run('CREATE (n:Cluster {id:'+str(record["id"])+',firstcore:'+str(record["firstcore"])+'})')
	
	############### Inserir arestas dos nos da particao 5 ################################
	session.run('Load csv with headers from "file:/particao5.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	
	############### Inserir relacao de grupos e pessoas da particao 5 ####################
	for record in data15:
		session.run('MATCH (a:Pessoa {idpessoa:'+str(record["idpessoa"])+'}),(b:Cluster{id:'+str(record["idgrupo"])+'}) create (b)-[r:CONTAINS]->(a)')
	
	########### Remover atributos dos nos presentes no arquivo arestas cortadas ##########
	session.run('Load csv with headers from "file:/arestas_cortadas_3.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) REMOVE a.expanded,a.core,a.noise,b.expanded,b.core,b.noise')
		########### Remover ligacao do cluster para esses nos - nao ta funfando ############
	
	################ Inserir arestas presentes no arquivo arestas cortadas ###############
	session.run('Load csv with headers from "file:/arestas_cortadas_3.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')

	########################## CALL NETSCAN #############################
	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 100, 5, 1)')

	############## Salvar valores ##########################
		#salva nos pessoas e atributos#
	result = session.run("MATCH (p:Pessoa) RETURN p.idpessoa AS idpessoa, p.noise AS noise, p.expanded AS expanded, p.core AS core")
		#salva nos grupos e atributos
	grupos = session.run("MATCH (c:Cluster) RETURN c.id AS id, c.firstcore AS firstcore, c.tag AS tag")
		#salva relacao dos grupos e pessoas
	agrupados = session.run('MATCH (n:Cluster)-[r:CONTAINS]-(p:Pessoa) RETURN n.id AS idgrupo,p.idpessoa AS idpessoa')


	data16 = result.data()  #pode dar exception se o resultado for None
	for record in data16:
		if record["core"] is None:
			record["core"] = 'null'
		if record["expanded"] is None:
			record["expanded"] = 'null'
		if record["noise"] is None:
			record["noise"] = 'null'
	
	data17 = grupos.data()
	data18 = agrupados.data()

	##################### RESET #######################
	session.run('Match (a)-[b]->(c) DELETE a,b,c')
	session.run('Match (a) DELETE a')

	session.close()

	end = time.time()
	print(end - start)

#----------------------------------------------------------****************** NOVO ******************-------------------------------------------------------------------#


with driver.session() as session:
	start = time.time()

	############## primeira particao #######################
	session.run('Load csv with headers from "file:/particao7.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa1)')
	session.run('Load csv with headers from "file:/particao7.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa2)')
	session.run('Load csv with headers from "file:/particao7.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	
	########################## CALL NETSCAN #############################	
	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 100, 5, 1)')

	############## Salvar valores ##########################
		#salva nos pessoas e atributos#
	result = session.run("MATCH (p:Pessoa) RETURN p.idpessoa AS idpessoa, p.noise AS noise, p.expanded AS expanded, p.core AS core")
		#salva nos grupos e atributos
	grupos = session.run("MATCH (c:Cluster) RETURN c.id AS id, c.firstcore AS firstcore, c.tag AS tag")
		#salva relacao dos grupos e pessoas
	agrupados = session.run('MATCH (n:Cluster)-[r:CONTAINS]-(p:Pessoa) RETURN n.id AS idgrupo,p.idpessoa AS idpessoa')


	data19 = result.data()  #pode dar exception se o resultado for None
	for record in data19:
		if record["core"] is None:
			record["core"] = 'null'
		if record["expanded"] is None:
			record["expanded"] = 'null'
		if record["noise"] is None:
			record["noise"] = 'null'
	
	data20 = grupos.data()
	data21 = agrupados.data()


	##################### RESET #######################
	session.run('Match (a)-[b]->(c) DELETE a,b,c')
	session.run('Match (a) DELETE a')

	session.close()

	end = time.time()
	print(end - start)
	

#--------------------------------------------------------------------------------------------------------------------------------------------------------------------#


with driver.session() as session:
	
	start = time.time()
	############## segunda particao #######################
	session.run('Load csv with headers from "file:/particao8.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa1)')
	session.run('Load csv with headers from "file:/particao8.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa2)')
	session.run('Load csv with headers from "file:/particao8.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	########################## CALL NETSCAN #############################
	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 100, 5, 1)')

	session.close()

	end = time.time()
	print(end - start)



#--------------------------------------------------------------------------------------------------------------------------------------------------------------------#


with driver.session() as session:
	
	start = time.time()
	############## Jogar os nos da particao 7 em conjunto com a particao 8 ##############	
	for record in data19:
		session.run('MERGE (u:Pessoa {idpessoa:'+str(record["idpessoa"])+'}) ON CREATE SET u.idpessoa ='+str(record["idpessoa"])+', u.expanded ='+str(record["expanded"])+', u.core = '+str(record["core"])+', u.noise = '+str(record["noise"])+'')

	for record in data20:
		session.run('CREATE (n:Cluster {id:'+str(record["id"])+',firstcore:'+str(record["firstcore"])+'})')
	
	############### Inserir arestas dos nos da particao 7 ################################
	session.run('Load csv with headers from "file:/particao7.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	
	############### Inserir relacao de grupos e pessoas da particao 7 ####################
	for record in data21:
		session.run('MATCH (a:Pessoa {idpessoa:'+str(record["idpessoa"])+'}),(b:Cluster{id:'+str(record["idgrupo"])+'}) create (b)-[r:CONTAINS]->(a)')
	
	########### Remover atributos dos nos presentes no arquivo arestas cortadas ##########
	session.run('Load csv with headers from "file:/arestas_cortadas_4.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) REMOVE a.expanded,a.core,a.noise,b.expanded,b.core,b.noise')
		########### Remover ligacao do cluster para esses nos - nao ta funfando ############
	
	################ Inserir arestas presentes no arquivo arestas cortadas ###############
	session.run('Load csv with headers from "file:/arestas_cortadas_4.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')

	########################## CALL NETSCAN #############################
	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 100, 5, 1)')

	

	session.close()

	end = time.time()
	print(end - start)








#--------------------------------------------------------------------- FINAL --------------------------------------------------------------------------------#


with driver.session() as session:
	
	start = time.time()
	############## Jogar os nos da particao C em conjunto com a particao D ##############	
	for record in data16:
		session.run('MERGE (u:Pessoa {idpessoa:'+str(record["idpessoa"])+'}) ON CREATE SET u.idpessoa ='+str(record["idpessoa"])+', u.expanded ='+str(record["expanded"])+', u.core = '+str(record["core"])+', u.noise = '+str(record["noise"])+'')

	for record in data17:
		session.run('CREATE (n:Cluster {id:'+str(record["id"])+',firstcore:'+str(record["firstcore"])+'})')
	
	############### Inserir arestas dos nos da particao 5 e 6 ################################
	session.run('Load csv with headers from "file:/particao5.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	session.run('Load csv with headers from "file:/particao6.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	################ Inserir arestas presentes no arquivo arestas cortadas 1 ###############
	session.run('Load csv with headers from "file:/arestas_cortadas_3.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')

	
	############### Inserir relacao de grupos e pessoas da particao C ####################
	for record in data18:
		session.run('MATCH (a:Pessoa {idpessoa:'+str(record["idpessoa"])+'}),(b:Cluster{id:'+str(record["idgrupo"])+'}) create (b)-[r:CONTAINS]->(a)')
	
	########### Remover atributos dos nos presentes no arquivo arestas cortadas ##########
	session.run('Load csv with headers from "file:/arestas_cortadas_22.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) REMOVE a.expanded,a.core,a.noise,b.expanded,b.core,b.noise')
		########### Remover ligacao do cluster para esses nos - nao ta funfando ############
	
	################ Inserir arestas presentes no arquivo arestas cortadas ###############
	session.run('Load csv with headers from "file:/arestas_cortadas_22.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')

	########################## CALL NETSCAN #############################
	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 100, 5, 1)')

	
	

	session.close()

	end = time.time()
	print(end - start)







#----------------------------------------**************************** MASTER FINAL *******************************--------------------------------------------------#


with driver.session() as session:
	
	start = time.time()
	############## Jogar os nos da particao AB em conjunto com a particao CD ##############	
	for record in data10:
		session.run('MERGE (u:Pessoa {idpessoa:'+str(record["idpessoa"])+'}) ON CREATE SET u.idpessoa ='+str(record["idpessoa"])+', u.expanded ='+str(record["expanded"])+', u.core = '+str(record["core"])+', u.noise = '+str(record["noise"])+'')

	for record in data11:
		session.run('CREATE (n:Cluster {id:'+str(record["id"])+',firstcore:'+str(record["firstcore"])+'})')
	
	############### Inserir arestas dos nos da particao 1, 2, 3 e 4 ################################
	session.run('Load csv with headers from "file:/particao1.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	session.run('Load csv with headers from "file:/particao2.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	session.run('Load csv with headers from "file:/particao3.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	session.run('Load csv with headers from "file:/particao4.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	################ Inserir arestas presentes no arquivo arestas cortadas 1 ###############
	session.run('Load csv with headers from "file:/arestas_cortadas_1.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')
	session.run('Load csv with headers from "file:/arestas_cortadas_2.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')

	
	############### Inserir relacao de grupos e pessoas da particao AB ####################
	for record in data12:
		session.run('MATCH (a:Pessoa {idpessoa:'+str(record["idpessoa"])+'}),(b:Cluster{id:'+str(record["idgrupo"])+'}) create (b)-[r:CONTAINS]->(a)')
	
	########### Remover atributos dos nos presentes no arquivo arestas cortadas ##########
	session.run('Load csv with headers from "file:/arestas_cortadas_111.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) REMOVE a.expanded,a.core,a.noise,b.expanded,b.core,b.noise')
		########### Remover ligacao do cluster para esses nos - nao ta funfando ############
	
	################ Inserir arestas presentes no arquivo arestas cortadas ###############
	session.run('Load csv with headers from "file:/arestas_cortadas_111.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')

	########################## CALL NETSCAN #############################
	session.run('CALL netscan.find_communities("Pessoa","Publicou","idpessoa","total", 100, 5, 1)')

	
	

	session.close()

	end = time.time()
	print(end - start)
