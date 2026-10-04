FROM sbtscala/scala-sbt:eclipse-temurin-21.0.5_11_1.10.7_3.3.4 AS build

WORKDIR /workspace

COPY build.sbt ./
COPY project ./project
COPY config ./config
COPY domain ./domain
COPY redis ./redis
COPY service ./service
COPY http ./http
COPY app ./app

RUN sbt app/stage

FROM eclipse-temurin:21-jre

RUN mkdir -p /workspace/conf
COPY --from=build --chown=10001:10001 /workspace/app/target/universal/stage /opt/zeus

USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["/opt/zeus/bin/app"]
