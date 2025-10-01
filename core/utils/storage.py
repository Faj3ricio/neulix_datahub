"""
╔════════════════════════════╗
║ Module: GoogleCloudStorage ║
╚════════════════════════════╝
Integration Google Cloud Storage.
"""
from google.cloud import storage
import io
import pandas as pd
from painel_financeiro_aav.core.configs.logs import AgentLogger

class CloudStorageManager:
    """
    Classe para gerenciar operações de storage utilizando o Google Cloud Storage.
    Essa classe contém métodos para upload, download, listagem, exclusão de arquivos e
    geração de URLs assinadas para acesso temporário.
    """
    
    def __init__(self, bucket_name: str = None):
        """
        Inicializa o cliente do Storage e obtém o bucket.

        :param service_account_path: Caminho para o arquivo JSON de credenciais.
        :param bucket_name: Nome do bucket no Google Cloud Storage.
        """
        self.log = AgentLogger()
        self.client = storage.Client()

        if bucket_name:
            self.bucket = self.client.get_bucket(bucket_name)
        else:
            self.bucket = None
    
    def upload_file(self, local_file_path, destination_blob_name):
        """
        Faz o upload de um arquivo para o bucket.
        
        :param local_file_path: Caminho local do arquivo.
        :param destination_blob_name: Nome do arquivo (blob) no bucket.
        """
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_file_path)
        self.log.inf(f"Arquivo '{local_file_path}' enviado para '{destination_blob_name}' no bucket.")
    
    def download_file(self, blob_name, destination_file_path):
        """
        Faz o download de um arquivo do bucket.
        
        :param blob_name: Nome do arquivo (blob) no bucket.
        :param destination_file_path: Caminho local onde o arquivo será salvo.
        """
        blob = self.bucket.blob(blob_name)
        blob.download_to_filename(destination_file_path)
        self.log.inf(f"Arquivo '{blob_name}' baixado para '{destination_file_path}'.")
    
    def delete_file(self, blob_name):
        """
        Exclui um arquivo do bucket.
        
        :param blob_name: Nome do arquivo (blob) a ser excluído.
        """
        blob = self.bucket.blob(blob_name)
        blob.delete()
        self.log.inf(f"Arquivo '{blob_name}' deletado do bucket.")
    
    def list_files(self):
        """
        Lista todos os arquivos presentes no bucket.
        
        :return: Lista com os nomes dos arquivos (blobs).
        """
        blobs = self.bucket.list_blobs()
        file_names = [blob.name for blob in blobs]
        self.log.inf("Arquivos no bucket:")
        for name in file_names:
            self.log.inf(f"- {name}")
        return file_names

    def list_buckets(self, project_id):
        client = storage.Client(project=project_id)
        buckets = list(client.list_buckets())
        print("Buckets encontrados:")
        for bucket in buckets:
            print(bucket.name)
    
    def generate_signed_url(self, blob_name, expiration=3600):
        """
        Gera uma URL assinada para acesso temporário a um arquivo.
        
        :param blob_name: Nome do arquivo (blob) para gerar a URL.
        :param expiration: Tempo de expiração da URL em segundos (padrão: 3600s = 1 hora).
        :return: URL assinada.
        """
        blob = self.bucket.blob(blob_name)
        url = blob.generate_signed_url(expiration=expiration)
        self.log.inf(f"URL assinada gerada para '{blob_name}': {url}")
        return url

    def save_dataframe_to_parquet(self, 
                               df: pd.DataFrame, 
                               gcs_path: str,
                               compression: str = 'snappy') -> bool:
        """
        Método genérico para salvar DataFrame como arquivo Parquet no Google Cloud Storage.
        
        Args:
            df (pd.DataFrame): DataFrame a ser salvo
            gcs_path (str): Caminho completo do arquivo no bucket (ex: 'data/ploomes/deals/2024/01/15/deals.parquet')
            compression (str): Tipo de compressão ('snappy', 'gzip', 'brotli', 'lz4')
        
        Returns:
            bool: True se o arquivo foi salvo com sucesso, False caso contrário
        """
        
        if df.empty:
            self.log.war(f"DataFrame vazio, nenhum arquivo será salvo em '{gcs_path}'")
            return False
        
        try:
            # 1) Converte DataFrame para Parquet em buffer de memória
            parquet_buffer = io.BytesIO()
            
            df.to_parquet(
                parquet_buffer,
                compression=compression,
                index=False
            )
            
            # 2) Volta para o início do buffer
            parquet_buffer.seek(0)
            
            # 3) Upload para o GCS
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
            
            # 4) Log de sucesso
            file_size_mb = len(parquet_buffer.getvalue()) / (1024 * 1024)
            self.log.inf(f"DataFrame salvo como Parquet: {gcs_path} - "
                        f"{len(df)} registros, {len(df.columns)} colunas, "
                        f"{file_size_mb:.2f}MB")
            
            return True
            
        except Exception as e:
            self.log.err(f"Erro ao salvar DataFrame como Parquet no GCS: {e}")
            return False
        
        finally:
            # 5) Limpa o buffer da memória
            if 'parquet_buffer' in locals():
                parquet_buffer.close()

    def read_parquet_from_gcs(self,
                              gcs_path: str) -> pd.DataFrame:
        """
        Método genérico para ler um arquivo Parquet do Google Cloud Storage e retornar como DataFrame.
        Args:
            gcs_path (str): Caminho completo do arquivo no bucket (ex: 'data/ploomes/deals/2024/01/15/deals.parquet')
        Returns:
            pd.DataFrame: DataFrame lido do arquivo Parquet
        """
        
        try:
            # 1) Faz o download do arquivo Parquet para um buffer de memória
            blob = self.bucket.blob(gcs_path)

            # 2) Verifica se o blob existe
            if not blob.exists():
                self.log.war(f"Arquivo Parquet não encontrado no GCS: {gcs_path}")
                return pd.DataFrame()

            # 3) Download do conteúdo para um buffer de memória
            parquet_buffer = io.BytesIO()
            blob.download_to_file(parquet_buffer)
            parquet_buffer.seek(0)

            # 4) Lê o Parquet do buffer para um DataFrame
            df = pd.read_parquet(parquet_buffer)

            # 5) Log de sucesso
            file_size_mb = len(parquet_buffer.getvalue()) / (1024 * 1024)
            self.log.inf(f"Arquivo Parquet lido do GCS: {gcs_path} - "
                        f"{len(df)} registros, {len(df.columns)} colunas, "
                        f"{file_size_mb:.2f}MB")

            return df
            
        except Exception as e:
            self.log.err(f"Erro ao ler Parquet do GCS: {e}")
            return pd.DataFrame()
        finally:
            # 6) Limpa o buffer da memória
            if 'parquet_buffer' in locals():
                parquet_buffer.close()