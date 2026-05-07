FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY UniversalConnector.slnx .
COPY src/UniversalConnector.Core/UniversalConnector.Core.csproj src/UniversalConnector.Core/
COPY src/UniversalConnector.Nats/UniversalConnector.Nats.csproj src/UniversalConnector.Nats/
COPY src/UniversalConnector.Connectors/UniversalConnector.Connectors.csproj src/UniversalConnector.Connectors/
COPY src/UniversalConnector.Generic/UniversalConnector.Generic.csproj src/UniversalConnector.Generic/
COPY src/UniversalConnector.Host/UniversalConnector.Host.csproj src/UniversalConnector.Host/
RUN dotnet restore

COPY src/ src/
RUN dotnet publish src/UniversalConnector.Host/UniversalConnector.Host.csproj \
    -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
COPY connectors/ connectors/

ENTRYPOINT ["dotnet", "UniversalConnector.Host.dll"]
