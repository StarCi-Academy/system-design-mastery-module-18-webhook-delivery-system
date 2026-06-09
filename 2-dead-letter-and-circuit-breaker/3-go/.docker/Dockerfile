# syntax=docker/dockerfile:1

# ---- build stage ----
FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/emitter ./main.go

# ---- runtime stage ----
FROM alpine:3.20
WORKDIR /app
COPY --from=build /out/emitter ./emitter
EXPOSE 3000
CMD ["./emitter"]
