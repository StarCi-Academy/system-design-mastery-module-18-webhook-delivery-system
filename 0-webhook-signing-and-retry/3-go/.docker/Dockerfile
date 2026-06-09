# syntax=docker/dockerfile:1

# ---- build stage ----
FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /app/server .

# ---- runtime stage ----
FROM alpine:3.20
WORKDIR /app
COPY --from=build /app/server ./server
EXPOSE 3000
CMD ["./server"]
