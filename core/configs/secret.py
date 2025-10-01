"""
╔═════════════════════════════╗
║ Module: GoogleSecretManager ║
╚═════════════════════════════╝
Integration Google Secret Manager.
"""
from google.cloud import secretmanager
from painel_financeiro_aav.core.configs.logs import AgentLogger
import google.auth

class GoogleSecretManager:
    """
    Esta classe encapsula operações básicas com o Google Secret Manager,
    como acessar, criar e adicionar versões a segredos.
    """

    def __init__(self):
        """
        Inicializa o cliente do Secret Manager.

        Parâmetros:
            project_id (str): ID do projeto no Google Cloud.
            credentials_path (str): Caminho para o arquivo de credenciais.
            secret (str): Nome do arquivo.
                Se fornecido, define a variável de ambiente GOOGLE_APPLICATION_CREDENTIALS.
        """
        # Configura as credenciais se o caminho for informado
        self.log = AgentLogger()
        try:
            # Usando Application Default Credentials
            creds, project_id = google.auth.default()
            self.project_id = project_id
            self.client = secretmanager.SecretManagerServiceClient(credentials=creds)
        except Exception as e:
            self.log.cri(f"Erro ao configurar o Secret Manager: {e}")

    def access_secret(self, secret_id: str, version_id: str = "latest") -> str:
        """
        Acessa a versão de um segredo.

        Parâmetros:
            secret_id (str): Identificador do segredo.
            version_id (str): Versão do segredo (padrão: "latest").

        Retorna:
            str: O conteúdo do segredo.
        """
        try:
            name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"
            response = self.client.access_secret_version(request={"name": name})
            secret_data = response.payload.data.decode("UTF-8")
            return secret_data
        except Exception as e:
            self.log.cri(f"Erro crítico ao consumir secrets:\n {e}")

    def create_secret(self, secret_id: str):
        """
        Cria um novo segredo.

        Parâmetros:
            secret_id (str): Identificador do novo segredo.

        Retorna:
            O objeto resposta da criação do segredo.
        """
        parent = f"projects/{self.project_id}"
        secret = {"replication": {"automatic": {}}}
        response = self.client.create_secret(
            request={"parent": parent, "secret_id": secret_id, "secret": secret}
        )
        return response

    def add_secret_version(self, secret_id: str, payload: str):
        """
        Adiciona uma nova versão para o segredo especificado.

        Parâmetros:
            secret_id (str): Identificador do segredo.
            payload (str): Dados a serem adicionados como nova versão.

        Retorna:
            O objeto resposta da adição da nova versão.
        """
        parent = f"projects/{self.project_id}/secrets/{secret_id}"
        response = self.client.add_secret_version(
            request={"parent": parent, "payload": {"data": payload.encode("UTF-8")}}
        )
        return response
