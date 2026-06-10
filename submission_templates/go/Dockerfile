# Sandbox template: Go submission.
# The orchestrator drops the contestant's source in as engine.go before building.
FROM golang:1.22 AS build
WORKDIR /src
ENV CGO_ENABLED=0
COPY go.mod .
COPY engine.go .
RUN go mod tidy && go build -ldflags="-s -w" -o /engine .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /engine /engine
EXPOSE 9000
ENTRYPOINT ["/engine"]
