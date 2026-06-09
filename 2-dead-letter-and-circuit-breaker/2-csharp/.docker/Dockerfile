# syntax=docker/dockerfile:1

# ---- build stage ----
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY WebhookDlqCircuit.csproj ./
RUN dotnet restore
COPY . .
RUN dotnet publish -c Release -o /app

# ---- runtime stage ----
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app ./
EXPOSE 3000
ENTRYPOINT ["dotnet", "WebhookDlqCircuit.dll"]
