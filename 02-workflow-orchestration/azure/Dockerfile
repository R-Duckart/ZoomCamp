FROM kestra/kestra:latest
WORKDIR /app
USER root

# Install Azure CLI from official Microsoft signed apt repository
RUN apt-get update \
    && apt-get install -y curl ca-certificates gnupg lsb-release \
    && curl -sLS https://packages.microsoft.com/keys/microsoft.asc \
       | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] \
       https://packages.microsoft.com/repos/azure-cli/ $(lsb_release -cs) main" \
       > /etc/apt/sources.list.d/azure-cli.list \
    && apt-get update \
    && apt-get install -y azure-cli \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
