FROM maven:3-eclipse-temurin-25 AS builder
WORKDIR /build
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q -o package -DskipTests

FROM eclipse-temurin:25-jdk-noble
WORKDIR /app
COPY --from=builder /build/target/kbench-java.jar /app/kbench-java.jar
ENTRYPOINT ["java", "-Xms2g", "-Xmx2g", "-XX:+AlwaysPreTouch", "-XX:+UseG1GC", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/kbench-java.jar"]
