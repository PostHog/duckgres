FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /app
COPY clients/jdbc/pom.xml .
RUN mvn dependency:go-offline -q

COPY clients/jdbc/src ./src
RUN mvn package -q -DskipTests

FROM eclipse-temurin:17-jre-jammy

COPY --from=builder /app/target/jdbc-compat-test-1.0-SNAPSHOT.jar /app.jar
COPY --from=builder /app/target/lib/ /lib/

COPY entrypoint.sh /entrypoint.sh
COPY queries.yaml /queries.yaml

ENTRYPOINT ["/entrypoint.sh", "java", "-cp", "/app.jar:/lib/*", "com.duckgres.compat.JdbcCompatTest"]
