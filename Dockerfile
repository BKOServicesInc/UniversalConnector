# ── Stage 1: restore (layer-cache friendly) ──────────────────────────────────
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS restore
WORKDIR /src

# Copy only .csproj files first so NuGet restore is cached independently of source changes
COPY src/CommonModel.Runtime.Core/CommonModel.Runtime.Core.csproj                         src/CommonModel.Runtime.Core/
COPY src/CommonModel.Runtime.Infrastructure/CommonModel.Runtime.Infrastructure.csproj     src/CommonModel.Runtime.Infrastructure/
COPY src/CommonModel.Runtime.Drivers.Generic/CommonModel.Runtime.Drivers.Generic.csproj   src/CommonModel.Runtime.Drivers.Generic/
COPY src/CommonModel.Runtime.Host/CommonModel.Runtime.Host.csproj                         src/CommonModel.Runtime.Host/

RUN dotnet restore src/CommonModel.Runtime.Host/CommonModel.Runtime.Host.csproj \
    --runtime linux-x64

# ── Stage 2: publish ──────────────────────────────────────────────────────────
FROM restore AS publish

COPY src/ src/

RUN dotnet publish src/CommonModel.Runtime.Host/CommonModel.Runtime.Host.csproj \
    -c Release \
    -r linux-x64 \
    --no-restore \
    --self-contained false \
    -o /app/publish

# ── Stage 3: runtime ──────────────────────────────────────────────────────────
FROM mcr.microsoft.com/dotnet/runtime:10.0 AS runtime

WORKDIR /app

# Non-root user for security
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

# Published binaries
COPY --from=publish /app/publish .

# Default connector descriptors embedded in the image.
# Override at runtime with:  -v ./connectors:/app/connectors:ro
COPY connectors/ connectors/

# Health check: verify the process is alive (no HTTP server — worker service)
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD pgrep -f CommonModel.Runtime.Host || exit 1

USER appuser

ENTRYPOINT ["dotnet", "CommonModel.Runtime.Host.dll"]
