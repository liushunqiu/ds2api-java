# Build stage
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests -B

# Run stage
FROM eclipse-temurin:17-jre-alpine
RUN apk add --no-cache wget \
    && addgroup -S ds2api && adduser -S ds2api -G ds2api
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
RUN mkdir -p /data && chown -R ds2api:ds2api /app /data
USER ds2api
ENV DS2API_CONFIG_PATH=/data/config.json
EXPOSE 5001
ENTRYPOINT ["java", "-jar", "app.jar"]
