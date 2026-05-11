# -- Stage 1: Build --
FROM maven:3.9-eclipse-temurin-17-alpine AS build
WORKDIR /build

COPY pom.xml .
RUN mvn dependency:go-offline -Dmaven.repo.local=/tmp/m2repo -q

COPY src src
RUN mvn package -DskipTests -Dmaven.repo.local=/tmp/m2repo -q

# -- Stage 2: Runtime --
FROM eclipse-temurin:17-jre-alpine AS runtime

RUN addgroup -S ds2api && adduser -S ds2api -G ds2api
WORKDIR /app

COPY --from=build /build/target/*.jar /app/ds2api.jar

USER ds2api

EXPOSE 5001

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD wget -qO- http://localhost:5001/actuator/health || exit 1

ENTRYPOINT ["java", \
  "-XX:+UseZGC", \
  "-XX:MaxRAMPercentage=75.0", \
  "-Djava.security.egd=file:/dev/./urandom", \
  "-Dds2api.config=/data/config.json", \
  "-jar", "/app/ds2api.jar"]
