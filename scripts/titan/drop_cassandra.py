from cassandra.cluster import Cluster
import time

cluster = Cluster()
session = cluster.connect()

session.execute("drop keyspace titan;")

print("Successfully reset titan keyspace.")
