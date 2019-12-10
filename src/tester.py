import argparse
import xmlrpc.client

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="SurfStore client")
	parser.add_argument('hostport', help='host:port of the server')
	args = parser.parse_args()

	hostport = args.hostport

	try:
		client = []
		client.append(xmlrpc.client.ServerProxy('http://localhost:8080'))
		client.append(xmlrpc.client.ServerProxy('http://localhost:8081'))
		client.append(xmlrpc.client.ServerProxy('http://localhost:8082'))
		client.append(xmlrpc.client.ServerProxy('http://localhost:8083'))
		client.append(xmlrpc.client.ServerProxy('http://localhost:8084'))
		# Test ping
		'''
		for i in client:

			i.surfstore.ping()
			print("Ping() successful")
			print("Leader ", i.surfstore.isLeader())
			i.surfstore.crash()
		'''
		for i in client:
			if not i.surfstore.isLeader():
				i.surfstore.restore()
				print("Restore:",i)
		
		leader = client[0]
		#client[1].surfstore.crash()
		for i in client:

			if i.surfstore.isLeader():
				leader = i
				#i.surfstore.crash()
				print("Leader",leader)
		for i in client:
			print(i,"Leader:",i.surfstore.isLeader(),"Crashed:",i.surfstore.isCrashed())
		
		for i in client:
			print(type(i.surfstore.tester_getversion("Test.txt")))
			print(i,"Getfileversion:",i.surfstore.tester_getversion("AAA.txt"))

		print("start")
		client[0].surfstore.updatefile("Test.txt", 3, [1,2,3])
		print("end")
		# print("IsLeader should be True: ", client.surfstore.isLeader())
		# client.surfstore.updatefile("Test.txt", 3, [1,2,3])
		# client.surfstore.updatefile("Test.txt", 4, [1,2,3])
		# print("version: ", client.surfstore.tester_getversion("Test.txt"))
		# print("IsCrashed should be False: ", client.surfstore.isCrashed())
		# client.surfstore.crash()
		# client.surfstore.updatefile("Test.txt", 5, [1,2,3])
		# print("version: ", client.surfstore.tester_getversion("Test.txt"))
		# print("IsCrashed should be True: ", client.surfstore.isCrashed())
		# client.surfstore.restore()
		# print("IsCrashed should be False: ", client.surfstore.isCrashed())
	except Exception as e:
		print("Client: " + str(e))
