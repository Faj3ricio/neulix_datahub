# Neulix DataHub 🕸️

**Neulix DataHub** é uma plataforma modular para automação, coleta e visualização de dados, totalmente orquestrada via Airflow e Docker. Permite criar crawlers genéricos (scrapers e automações) facilmente extensíveis para qualquer site, com integração direta ao PostgreSQL e dashboards em tempo real via Streamlit.

---

### 📌 Principais Funcionalidades

- Pipelines de scraping e automação totalmente automatizadas (ex: LinkedIn, Wikipedia)
- Arquitetura modular e extensível (spiders, transformers, loaders)
- Dashboards em tempo real (Streamlit)
- Interface web pública (Nginx reverse proxy)
- Pronto para rodar em Raspberry Pi ou servidores
- HTTPS via DDNS e Let’s Encrypt

---

### 🧱 Stack Tecnológica

| Camada         | Ferramenta                       |
|---------------|-----------------------------------|
| Orquestração  | Apache Airflow                    |
| Scraping      | Selenium (Firefox), BeautifulSoup |
| ETL           | pandas, SQLAlchemy                |
| Storage       | PostgreSQL                        |
| Visualização  | Streamlit                         |
| Proxy         | Nginx + Let’s Encrypt             |
| Container     | Docker + docker-compose           |

---

### 🗂 Estrutura de Diretórios

```bash
neulix_datahub/
├── core/             # DAGs do Airflow
├── neulix_dataflow/  # Spiders, Transformers, Loaders
│   └── spiders/      # Spiders genéricos (Selenium)
├── neulix_interface/ # Dashboards Streamlit
├── nginx/            # Configuração do Nginx
├── docker-compose.yml
├── .env.example      # Variáveis de ambiente
```

---

### 🚀 Como Rodar

```bash
git clone https://github.com/seu-usuario/neulix_datahub.git
cd neulix_datahub
cp .env.example .env
docker-compose up --build -d
```

Acesse:
- http://localhost/airflow – Gerenciamento de DAGs
- http://localhost/dashboard – Dashboards Streamlit

---

### 🕸️ Exemplo de Spiders

* 

Todos os spiders herdam de `BaseSpider`, que utiliza Selenium (Firefox) para automação e scraping. Novos spiders podem ser criados facilmente herdando essa base.

---

### 🔄 Orquestração

- Cada spider sao executados por um DAG do Airflow (exemplos em `core/`).
- Resultados sao salvos no PostgreSQL e consumidos pelo Streamlit.

---

### 👨‍💻 Notas de Dev

- Spiders ficam em `neulix_dataflow/spiders/`, cada um pode ser disparado por um DAG.
- Dashboards são construídos sob demanda a partir do banco PostgreSQL.
- Arquitetura modular: fácil adicionar novos spiders, transformadores e loaders.
