# Neulix DataHub 🕸️

**Neulix DataHub** is a modular data collection and visualization platform built to run on a Raspberry Pi.  
It orchestrates web scrapers (spiders) through Airflow, stores cleaned data in Postgres, and provides real-time dashboards using Streamlit — all within Docker.

---

### 📌 Key Features

- Automated scraping pipelines (e.g., LinkedIn, Facebook)
- Modular ETL design (spiders, transformers, loaders)
- Real-time KPI dashboards (Streamlit)
- Accessible via public web interface (Nginx reverse proxy)
- Lightweight enough to run on Raspberry Pi 4
- Exposable with HTTPS using DDNS and Let’s Encrypt

---

### 🧱 Tech Stack

| Layer         | Tool                     |
|---------------|--------------------------|
| Orchestration | Apache Airflow           |
| Scraping      | BeautifulSoup + requests |
| ETL           | pandas, SQLAlchemy       |
| Storage       | PostgreSQL               |
| Visualization | Streamlit                |
| Proxy         | Nginx + Let’s Encrypt    |
| Container     | Docker + docker-compose  |

---

### 🗂 Directory Overview

```bash
neulix_datahub/
├── core/             # Airflow DAGs and helper scripts
├── neulix_dataflow/  # Spiders, Transformers, Loaders
├── neulix_interface/ # Streamlit dashboards
├── nginx/            # Nginx reverse proxy config
├── docker-compose.yml

🚀 Getting Started
bash
Copiar
Editar
git clone https://github.com/seu-usuario/neulix_datahub.git
cd neulix_datahub
cp .env.example .env
docker-compose up --build -d

Then access:
http://localhost/airflow – DAG management
http://localhost/dashboard – Streamlit dashboards

🔐 Exposing to the Web
Use DuckDNS for free domain

Enable HTTPS with Let’s Encrypt via Certbot

Configure Nginx for /airflow and /dashboard reverse proxy

👨‍💻 Dev Notes
All spiders are in neulix_dataflow/spiders/, each one can be triggered by a DAG.
Dashboards are built on demand from the Postgres DB.
