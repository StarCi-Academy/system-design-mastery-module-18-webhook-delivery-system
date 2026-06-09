# Multi-stage build (SELF-BUILD only — no cloud image, no DockerHub push).
# Stage 1: compile the emitter and receiver static binaries.
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
# Resolve dependencies and generate go.sum, then build both binaries.
RUN go mod tidy
RUN CGO_ENABLED=0 go build -o /out/emitter ./cmd/emitter
RUN CGO_ENABLED=0 go build -o /out/receiver ./cmd/receiver

# Stage 2: minimal runtime image carrying just the binaries.
FROM alpine:3.20 AS runtime
WORKDIR /app
COPY --from=build /out/emitter /app/emitter
COPY --from=build /out/receiver /app/receiver
EXPOSE 3000
# Default command runs the emitter; receiver-mock overrides via compose `command`.
CMD ["/app/emitter"]
