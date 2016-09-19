FROM java:alpine
VOLUME /tmp
COPY config.yml config.yml
COPY ground-core-0.1-SNAPSHOT.jar app.jar
RUN sh -c 'touch /app.jar'
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/app.jar","server", "config.yml"]
