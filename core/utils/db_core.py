"""
╔══════════════════════════════════╗
║       Module: DataBaseCore       ║
╚══════════════════════════════════╝
Database connections and operations integration.
"""

from typing import Any, Dict, List, Optional
import pandas as pd
import pandas_gbq
from google.cloud import bigquery, firestore
from google.protobuf.timestamp_pb2 import Timestamp as ProtoTimestamp
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
import os
import json
import google.auth

from painel_financeiro_aav.core.configs.logs import AgentLogger

class DatabaseCore:
    """
    Classe responsável por gerenciar conexões com diversos bancos de dados,
    abstraindo a extração e inserção de dados, como: SQL Server, BigQuery, MongoDB, Firestore, Google Sheets.
    """

    SHEETS_SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/cloud-platform'  # Adicionado escopo amplo
    ]

    def __init__(self) -> None:
        self.sql_engine = None
        self.tunnel = None
        self.log = AgentLogger()
        self.sheets_service = None
        self.drive_service = None

    def _sanitize_chunk(self,
                       chunk: pd.DataFrame) -> pd.DataFrame:
        """
        Sanitiza um DataFrame, preenchendo valores ausentes com 0 para numéricos.
        """
        for column in chunk.columns:
            if chunk[column].dtype.kind in 'iuf':
                chunk[column] = chunk[column].fillna(0)
            elif chunk[column].dtype.kind == 'O':
                chunk[column] = chunk[column].fillna('')
            elif chunk[column].dtype.kind == 'M':
                chunk[column] = pd.to_datetime(chunk[column], errors='coerce')
        return chunk

    def _sanitize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sanitiza os nomes das colunas para serem compatíveis com o BigQuery.
        """
        # Substitui caracteres não permitidos por underscore
        df.columns = df.columns.str.strip()
        df.columns = df.columns.str.replace('[^a-zA-Z0-9_]', '_', regex=True)
        
        # Remove underscores duplicados
        df.columns = df.columns.str.replace('_+', '_', regex=True)
        
        # Remove underscore do início e fim
        df.columns = df.columns.str.strip('_')
        
        # Garante que nomes não começam com números
        df.columns = [f"col_{col}" if col[0].isdigit() else col for col in df.columns]
        
        return df

    def insert_bq(self,
                  data: str,
                  destination_table: str,
                  loading_behavior: str) -> None:
        """
        Insere dados em uma tabela do BigQuery a partir de um arquivo CSV ou Parquet.
        """
        try:
            if data.endswith('.parquet'):
                dataframe = pd.read_parquet(data)
            elif data.endswith('.csv'):
                # Tenta diferentes delimitadores
                for delimiter in [',', ';', '\t']:
                    try:
                        dataframe = pd.read_csv(data, delimiter=delimiter, on_bad_lines='warn')
                        if len(dataframe.columns) > 1:  # Se tiver mais de uma coluna, encontramos o delimitador correto
                            self.log.inf(f"Arquivo CSV lido com sucesso usando delimitador: {delimiter}")
                            break
                    except:
                        continue
                else:
                    raise ValueError("Não foi possível determinar o delimitador correto do arquivo CSV")
            else:
                raise ValueError("Formato de arquivo não suportado. Use .csv ou .parquet.")

            if dataframe.empty:
                self.log.war("DataFrame vazio após leitura do arquivo.")
                return

            # Sanitiza os nomes das colunas
            dataframe = self._sanitize_column_names(dataframe)

            # Sanitiza os dados
            dataframe = self._sanitize_chunk(dataframe)

            pandas_gbq.to_gbq(
                dataframe,
                destination_table=destination_table,
                if_exists=loading_behavior
            )
            self.log.inf(f"Dados enviados para o BigQuery com sucesso. Tabela: {destination_table}")

        except (pd.errors.ParserError, ValueError, FileNotFoundError) as e:
            self.log.err(f"Erro na inserção de dados no BigQuery: {e}")

    def execute_query_bq(self,
                        query: str) -> Optional[pd.DataFrame]:
        """        
        Executa uma consulta SQL no BigQuery e retorna um DataFrame com os resultados.
        """
        try:
            result_df = pandas_gbq.read_gbq(query)
            return result_df
        except pandas_gbq.gbq.GenericGBQException as e:
            self.log.war(f"Erro ao executar consulta no BigQuery: {e}")
            return None
        except pandas_gbq.gbq.InvalidColumnOrder as e:
            self.log.war(f"Erro de ordem de coluna no BigQuery: {e}")
            return None
        except pandas_gbq.gbq.NotFoundException as e:
            self.log.war(f"Tabela ou recurso não encontrado no BigQuery: {e}")
            return None
            
    def upload_raw_files_to_bq(self, base_path: str) -> None:
        """
        Faz upload dos arquivos CSV para seus respectivos datasets no BigQuery.
        
        Args:
            base_path (str): Caminho base onde os arquivos CSV estão localizados
        """
        try:
            # Arquivos para raw_ego_datalake
            ego_files = {
                'entities.csv': 'raw_ego_datalake.entities',
                'instances.csv': 'raw_ego_datalake.instances'
            }
            
            # Arquivos para raw_sap_datalake
            sap_files = {
                'accounts_received.csv': 'raw_sap_datalake.accounts_received',
                'complete_reports.csv': 'raw_sap_datalake.complete_reports',
                'invoices.csv': 'raw_sap_datalake.invoices'
            }
            
            # Upload dos arquivos do EGO
            for file, table in ego_files.items():
                file_path = os.path.join(base_path, file)
                if os.path.exists(file_path):
                    self.log.inf(f"Iniciando upload do arquivo {file} para {table}")
                    self.insert_bq(
                        data=file_path,
                        destination_table=table,
                        loading_behavior='replace'
                    )
                else:
                    self.log.war(f"Arquivo {file} não encontrado em {base_path}")
            
            # Upload dos arquivos do SAP
            for file, table in sap_files.items():
                file_path = os.path.join(base_path, file)
                if os.path.exists(file_path):
                    self.log.inf(f"Iniciando upload do arquivo {file} para {table}")
                    self.insert_bq(
                        data=file_path,
                        destination_table=table,
                        loading_behavior='replace'
                    )
                else:
                    self.log.war(f"Arquivo {file} não encontrado em {base_path}")
        except Exception as e:
            self.log.war(f"Erro inesperado ao executar consulta no BigQuery: {e}")
            return None

    def execute_update_bq(self,
                         query: str) -> None:
        """
        Executa uma consulta de update no BigQuery.
        """
        try:
            client = bigquery.Client()
            query_job = client.query(query)
            query_job.result()
            self.log.inf("Consulta de atualização executada com sucesso.")
        except bigquery.exceptions.GoogleCloudError as e:
            self.log.err(f"Erro ao executar consulta de atualização no BigQuery: {e}")
        except Exception as e:
            self.log.err(f"Erro inesperado ao executar consulta de atualização no BigQuery: {e}")

    def connect_firestore(self,
                         firestore_project_id: str,
                         firestore_db: str):
        """
        Realiza a conexão com o Firestore, especificando o projeto e o banco de dados.
        """
        self.log.deb(f"Tentando conectar ao Firestore com project_id: '{firestore_project_id}' e database_id: '{firestore_db}'")
        cred_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        self.log.deb(f"GOOGLE_APPLICATION_CREDENTIALS: {cred_path}")
        # 1) Verifica se o caminho das credenciais foi definido e se o arquivo existe. Localmente ou no ambiente.
        if cred_path and os.path.exists(cred_path):
            with open(cred_path, encoding="utf-8") as f:
                cred_info = json.load(f)
                # 1.1) Confirma se a credencial pertence ao projeto correto.
                if cred_info.get('project_id') != firestore_project_id:
                    self.log.war(f"A credencial pertence ao projeto '{cred_info.get('project_id')}', mas o código está tentando usar o projeto '{firestore_project_id}'.")
        else:
            self.log.err("Caminho de credenciais não encontrado ou inválido.")

        try:
            client = firestore.Client(project=firestore_project_id, database=firestore_db)
            # 2) Log de informações do cliente Firestore
            self.log.deb(f"Firestore client criado: {client}")
            self.log.deb(f"Firestore project (client.project): {client.project}")
            self.log.inf("Conexão com Firestore realizada com sucesso.")
            return client
        except firestore.exceptions.GoogleAPICallError as e:
            self.log.err(f"Erro ao conectar ao Firestore: {e}")
            return None
        except firestore.exceptions.ClientError as e:
            self.log.err(f"Erro de cliente ao conectar ao Firestore: {e}")
            return None

    def get_firestore_documents(self,
                                project_id: str,
                                database_id: str,
                                collection_name: str,
                                filters: Optional[Dict[str, Any]] = None,
                                order_by: Optional[str] = None,
                                descending: bool = False,
                                limit: Optional[int] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Retorna lista de documentos de uma coleção do Firestore.
        """
        client = self.connect_firestore(project_id, database_id)
        if client is None:
            return None

        try:
            query = client.collection(collection_name)
            # Aplicar filtros simples (==)
            if filters:
                for field, value in filters.items():
                    query = query.where(field, '==', value)
            # Ordenação
            if order_by:
                direction = firestore.Query.DESCENDING if descending else firestore.Query.ASCENDING
                query = query.order_by(order_by, direction=direction)
            # Limite
            if limit:
                query = query.limit(limit)

            docs = query.stream()
            result = [doc.to_dict() for doc in docs]
            self.log.inf(f"{len(result)} docs carregados de {collection_name}")
            return result
        except Exception as e:
            self.log.err(f"Erro ao buscar docs Firestore: {e}")
            return None

    def upsert_dataframe_firestore(self,
                                  firestore_project_id: str,
                                  firestore_db_name: str,
                                  firestore_collection_name: str,
                                  dataframe: pd.DataFrame,
                                  id_column: str) -> bool:
        """
        Insere ou atualiza documentos no Firestore.

        Cada linha do DataFrame se torna um documento. O ID de cada documento é
        retirado da coluna especificada em 'id_column'.

        Args:
            firestore_project_id (str): ID do projeto no Google Cloud.
            firestore_db_id (str): ID do banco de dados Firestore.
            collection_name (str): Nome da coleção de destino.
            dataframe (pd.DataFrame): O DataFrame contendo os dados.
            id_column (str): O nome da coluna no DataFrame a ser usada como ID do documento.
        """

        # 1) Verifica se a coluna de ID especificada realmente existe no DataFrame.
        if id_column not in dataframe.columns:
            self.log.err(f"A coluna '{id_column}' não foi encontrada no DataFrame. Abortando.")
            return False

        client = self.connect_firestore(firestore_project_id, firestore_db_name)
        if client is None:
            self.log.deb("Firestore client não foi criado.")
            return False

        try:
            collection_ref = client.collection(firestore_collection_name)
            batch = client.batch()
            commit_count = 0

            # 2) Lógica de Inserção 
            for _, row in dataframe.iterrows():
                # 2.1) Pega o valor da coluna de ID e o converte para string.
                doc_id = str(row[id_column])
                doc_data = row.to_dict()

                self.log.deb(f"Preparando para inserir/atualizar documento com ID '{doc_id}'.")

                # 2.2) Cria uma referência ao documento com o ID customizado
                doc_ref = collection_ref.document(doc_id)
                # 2.3) Adiciona a operação de .set() ao batch
                batch.set(doc_ref, doc_data)

                commit_count += 1
                # 2.4) Para performance, envia o batch a cada 500 operações
                if commit_count % 500 == 0:
                    self.log.deb(f"Enviando batch de {commit_count} operações...")
                    batch.commit()
                    # 2.4.1) Reinicia o batch
                    batch = client.batch()

            # 3) Envia quaisquer operações restantes no batch final
            if commit_count > 0:
                self.log.deb(f"Enviando batch final com as operações restantes...")
                batch.commit()

            self.log.inf(f"{len(dataframe)} documentos inseridos/atualizados na coleção '{firestore_collection_name}'.")
            return True
        except Exception as e:
            self.log.err(f"Erro ao inserir documentos no Firestore: {e}")
            return False

    def insert_dataframe_bq(self,
                            dataframe: pd.DataFrame,
                            destination_table: str,
                            loading_behavior: str) -> None:
        """
        Insere um DataFrame do Pandas diretamente em uma tabela do BigQuery.

        Args:
            dataframe (pd.DataFrame): O DataFrame a ser inserido.
            destination_table (str): Tabela de destino no formato 'projeto.dataset.tabela' ou 'dataset.tabela'.
            loading_behavior (str): Como lidar com a tabela se ela já existir ('fail', 'replace', 'append').
        """
        if dataframe.empty:
            self.log.war(f"DataFrame para a tabela {destination_table} está vazio. Inserção no BigQuery ignorada.")
            return

        try:
            self.log.inf(f"Iniciando inserção de {len(dataframe)} registros via DataFrame para {destination_table}.")
            pandas_gbq.to_gbq(
                dataframe,
                destination_table=destination_table,
                if_exists=loading_behavior,
                progress_bar=False
            )
            self.log.inf(f"Dados inseridos com sucesso via DataFrame na tabela: {destination_table}")

        except Exception as e:
            self.log.err(f"Erro na inserção de DataFrame no BigQuery: {e}")
            raise