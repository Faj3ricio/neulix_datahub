FROM apache/airflow:2.10.4-python3.10

USER root
# 1. Instala dependências de SO e driver ODBC
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      curl \
      ca-certificates \
      apt-transport-https \
      unixodbc-dev \
 && curl -sSL https://packages.microsoft.com/config/debian/11/packages-microsoft-prod.deb \
      -o packages-microsoft-prod.deb \
 && dpkg -i packages-microsoft-prod.deb \
 && rm packages-microsoft-prod.deb \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
      msodbcsql17 \
      mssql-tools \
 && rm -rf /var/lib/apt/lists/*

# 2. Volta ao usuário airflow antes de instalar pacotes Python
USER airflow

# 3. Instala pyodbc e provider MSSQL do Airflow
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir \
      pyodbc \
      apache-airflow-providers-microsoft-mssql

# 4. Instala requirements.txt adicional (se houver)
COPY ../../requirements.txt /opt/airflow/
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# 5. Configura o diretório de trabalho
ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/daas_aav/painel_financeiro_aav/core/configs/sa_airflow.json
