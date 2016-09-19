FROM java:alpine
VOLUME /tmp
ADD ground-core-0.1-SNAPSHOT.jar app.jar
ADD config.yml config.yml
RUN sh -c 'touch /app.jar'
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/app.jar","server",]
