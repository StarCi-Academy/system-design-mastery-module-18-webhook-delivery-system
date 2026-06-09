# Multi-stage build (SELF-BUILD only — no cloud image, no DockerHub push).
# Stage 1: restore + publish the ASP.NET Core app.
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY Outbox.csproj ./
RUN dotnet restore
COPY . ./
RUN dotnet publish -c Release -o /app --no-restore

# Stage 2: minimal ASP.NET runtime image.
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /app ./
ENV PORT=3000
EXPOSE 3000
# Default command runs the emitter; receiver-mock overrides via compose `command`.
ENTRYPOINT ["dotnet", "Outbox.dll"]
