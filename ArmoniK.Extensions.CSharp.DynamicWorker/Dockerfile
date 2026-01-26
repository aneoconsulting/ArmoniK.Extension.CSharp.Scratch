#See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["ArmoniK.Extensions.CSharp.DynamicWorker/ArmoniK.Extensions.CSharp.DynamicWorker.csproj", "ArmoniK.Extensions.CSharp.DynamicWorker/"]
RUN dotnet restore "./ArmoniK.Extensions.CSharp.DynamicWorker/ArmoniK.Extensions.CSharp.DynamicWorker.csproj"
COPY . .
WORKDIR "/src/ArmoniK.Extensions.CSharp.DynamicWorker"
RUN dotnet build "./ArmoniK.Extensions.CSharp.DynamicWorker.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./ArmoniK.Extensions.CSharp.DynamicWorker.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
RUN groupadd --gid 5000 armonikuser && useradd --home-dir /home/armonikuser --create-home --uid 5000 --gid 5000 --shell /bin/sh --skel /dev/null armonikuser
RUN mkdir /cache && chown armonikuser: /cache
USER armonikuser
EXPOSE 1080
ENTRYPOINT ["dotnet", "ArmoniK.Extensions.CSharp.DynamicWorker.dll"]