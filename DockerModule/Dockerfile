FROM mcr.microsoft.com/dotnet/runtime:3.1 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:3.1 AS build
WORKDIR /src
COPY "IoTMetricsToLogAnalytics.csproj" .
RUN dotnet restore "IoTMetricsToLogAnalytics.csproj"
COPY . .
WORKDIR "/src"
RUN dotnet build "IoTMetricsToLogAnalytics.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "IoTMetricsToLogAnalytics.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "IoTMetricsToLogAnalytics.dll"]