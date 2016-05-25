import psycopg2
import sys

assert (len(sys.argv) == 2)
dbname = sys.argv[1]

print ("Attempting to to connect...")
conn = psycopg2.connect(database=dbname, user="ground", host="localhost", password="metadata")

conn.autocommit = False
cursor = conn.cursor()
print ("Connection succeeded. Dropping tables...")

try:
    cursor.execute(open("drop_postgres.sql", 'r').read())
except:
    print "Dropping tables failed."

print "Creating tables..."

cursor.execute(open("postgres.sql", 'r').read())

conn.commit()
conn.close()
print "Table creation succeeded."
