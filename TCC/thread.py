import time
import threading
from neo4j.v1 import GraphDatabase, basic_auth
driver = GraphDatabase.driver("bolt://localhost", 	auth=basic_auth("neo4j", "1234"))


class myThread (threading.Thread):
   def __init__(self, val, signal):
      threading.Thread.__init__(self)
      self.val = val
   def run(self):
	if self.val==1:
		print "Starting 1"
		with driver.session() as session:
			start = time.time()
			session.run('Load csv with headers from "file:/particao1.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa1)')
		session.close()
		end = time.time()	
		print(end - start)
		self.signal = signal  + 1
		print "Exiting 1"
   	else:
		print "Starting 2"
		with driver.session() as session:
			start = time.time()
			session.run('Load csv with headers from "file:/particao2.csv" as csvline MERGE (u:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) ON CREATE SET u.idpessoa = toInteger(csvline.idpessoa2)')
		session.close()
		end = time.time()	
		print(end - start)
		self.signal = signal + 1
		print "Exiting 2"


signal = 0
threadLock = threading.Lock()
threads = []
	
# Create new threads
thread1 = myThread(1,signal)
thread2 = myThread(2,signal)

# Start new Threads
thread1.start()
thread2.start()

# Add threads to thread list
threads.append(thread1)
threads.append(thread2)

for t in threads:
    t.join()
print "Exiting Main Thread"

#session.run('Load csv with headers from "file:/particao1.csv" as csvline MATCH (a:Pessoa {idpessoa: toInteger(csvline.idpessoa1)}),(b:Pessoa {idpessoa: toInteger(csvline.idpessoa2)}) create (a)-[r:Publicou{total:toFloat(csvline.total)}]->(b)')



