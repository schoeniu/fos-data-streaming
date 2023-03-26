FROM maven:3.8.3-openjdk-17 AS build

WORKDIR /home/fos-report

COPY pom.xml .
RUN mvn verify -DskipTests --fail-never

COPY src ./src
RUN mvn verify

FROM azul/zulu-openjdk-alpine:17-jre-latest
ENTRYPOINT ["java", "-jar", "/app/fos-report.jar"]

WORKDIR /app
COPY --from=build /home/fos-report/target .