# build the server
mvn clean package -DskipTests

# start the server and capture the pid
java -jar ground-core/target/ground-core-0.1-SNAPSHOT.jar server ground-core/conf/config.yml &
SERVER_PID=$!

# make sure it's still running after 10 seconds
sleep 10
kill -0 $SERVER_PID

