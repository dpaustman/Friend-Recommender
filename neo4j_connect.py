from neo4jrestclient.client import GraphDatabase
import csv
import friends_recommender	

url = "http://localhost:7474/db/data/"
gdb = GraphDatabase(url, username="neo4j", password="pronalt0212")

# cria label Person	
person = gdb.labels.create("Person")

people = []
friends = []
# le .csv de dados
with open('friends.csv', newline='') as csvfile:
	reader = csv.reader(csvfile, delimiter=';')
	for row in reader:
		# cria o no da pessoa
		people.append(row[0])

		for i in range(0,len(people)):
			node_query = "MERGE(p:Person {name: "+"'"+people[i]+"'"+"})"
			result_node = gdb.query(node_query)

		for i in range(1,len(row)):
			# cria relacionamento
			friends.append(row[i])
		for j in range(0, len(friends)):
			rel_query = str("match(p:Person), (f:Person)"+ " where p.name = "+"'"+str(people[0])+"'"+" and f.name = "+"'"+str(friends[j])+"'"+" merge(p)-[:knows]->(f)")
			result = gdb.query(q=rel_query)
		friends = []
		people = []

friends_recommender.FriendsRecommender.run()