# Multi-stage build (SELF-BUILD only — no cloud image, no DockerHub push).
# Stage 1: resolve dependencies (cached) then build the Spring Boot fat jar.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml ./
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B -DskipTests package

# Stage 2: minimal JRE runtime image carrying just the fat jar.
FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app
COPY --from=build /app/target/app.jar /app/app.jar
EXPOSE 3000
# Default profile is emitter; receiver-mock sets SPRING_PROFILES_ACTIVE=receiver.
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
