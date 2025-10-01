"""
╔══════════════════════════════════╗
║       Module: SheetsCore         ║
╚══════════════════════════════════╝

Classe reutilizável para integração com Google Sheets/Drive.
"""
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import os
import json
from datetime import datetime

from google.oauth2 import service_account
import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from painel_financeiro_aav.core.configs.logs import AgentLogger


class SheetsCore:
    """
    Classe responsável por encapsular integrações com Google Sheets e Drive.
    Utiliza Google API Client com service account ou default credentials.
    """

    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/cloud-platform'
    ]

    def __init__(self, 
                sheet_config: Optional[Dict[str, Any]] = None,
                logger: Optional[Any] = None):
        self.config: Dict[str, Any] = sheet_config or {}
        self.log = logger or AgentLogger()
        self.creds = None
        self.sheets_service = None
        self.drive_service = None
        self.sa_info: Dict[str, Any] = {}
        # Constrói serviços durante inicialização para falhar rápido em ambiente sem creds
        self._build_services()

    # ---------------------- Helpers padrão de DataFrame ----------------------
    @staticmethod
    def find_col(df: pd.DataFrame,
                target_name: Optional[str]) -> Optional[str]:
        if target_name is None:
            return None
        target = str(target_name).strip().lower()
        for c in df.columns:
            if str(c).strip().lower() == target:
                return c
        for c in df.columns:
            if target in str(c).strip().lower():
                return c
        return None

    @staticmethod
    def parse_flag(v) -> bool:
        try:
            if pd.isna(v):
                return False
        except Exception:
            pass
        if isinstance(v, bool):
            return bool(v)
        s = str(v).strip().lower()
        if s in ('1', 'true', 't', 'sim', 's', 'yes', 'y'):
            return True
        try:
            fv = float(s.replace(',', '.'))
            return int(fv) == 1
        except Exception:
            pass
        return False

    @staticmethod
    def df_convert_datetimes_to_iso(df: pd.DataFrame) -> pd.DataFrame:
        for col in df.select_dtypes(include=['datetime64']).columns:
            df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
        return df

    @staticmethod
    def create_flag_columns(df: pd.DataFrame,
                            control_names: Dict[str, str]) -> pd.DataFrame:
        df['_flag_sanity'] = df[control_names['sanity_check']].apply(SheetsCore.parse_flag)
        df['_flag_dup'] = df[control_names['is_duplicate']].apply(SheetsCore.parse_flag)
        df['_flag_quality'] = df[control_names['quality_check']].apply(SheetsCore.parse_flag)
        return df

    # ---------------------- Bootstrap Google Services ----------------------
    def _build_services(self) -> None:
        """
        Cria sheets_service e drive_service usando keyfile ou default credentials.
        """
        keyfile = self.config.get('service_account_keyfile') or os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        impersonate = self.config.get('impersonate')

        try:
            if keyfile and os.path.exists(keyfile):
                creds = service_account.Credentials.from_service_account_file(keyfile, scopes=self.SCOPES)
                try:
                    with open(keyfile, 'r', encoding='utf-8') as f:
                        jf = json.load(f)
                        self.sa_info['client_email'] = jf.get('client_email')
                        self.sa_info['project_id'] = jf.get('project_id')
                except Exception:
                    pass
                if impersonate:
                    creds = creds.with_subject(impersonate)
                    self.sa_info['impersonate'] = impersonate
            else:
                creds, _ = google.auth.default(scopes=self.SCOPES)
                self.log.inf("Using google.auth.default() credentials")

            self.creds = creds
            self.sheets_service = build('sheets', 'v4', credentials=creds)
            self.drive_service = build('drive', 'v3', credentials=creds)
        except Exception as e:
            self.log.err(f"Erro inicializando serviços Google (Sheets/Drive): {e}")
            # Relança para o chamador decidir (seguindo padrão do projeto: fail fast)
            raise

    # ---------------------- Operações com Spreadsheet ----------------------
    def open_or_create_spreadsheet(self) -> Tuple[str, Optional[int], Optional[str]]:
        """
        Abre planilha existente ou cria uma nova e retorna (spreadsheet_id, sheet_id, url).
        Se a planilha já existir, retorna seu ID e URL.
        """
        spreadsheet_id = self.config.get('spreadsheet_id')
        spreadsheet_url = None
        sheet_id = None
        try:
            if spreadsheet_id:
                meta = self.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id, fields="spreadsheetId,properties,sheets.properties").execute()
                spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
                sheet_id = meta['sheets'][0]['properties']['sheetId'] if meta.get('sheets') else None
            else:
                title = f"{self.config.get('sheet_title_prefix','Homologação')} - {datetime.utcnow().strftime('%Y-%m-%d')}"
                resp = self.sheets_service.spreadsheets().create(body={"properties": {"title": title}}, fields='spreadsheetId,spreadsheetUrl,sheets.properties').execute()
                spreadsheet_id = resp.get('spreadsheetId')
                spreadsheet_url = resp.get('spreadsheetUrl')
                sheet_id = resp.get('sheets')[0]['properties']['sheetId']
                self.log.inf(f"Spreadsheet created: {spreadsheet_id}")
            return spreadsheet_id, sheet_id, spreadsheet_url
        except HttpError as e:
            self.log.err(f"Sheets API error opening/creating spreadsheet: {e}")
            raise

    def read_values(self,
                    spreadsheet_id: str,
                    range: str = "A1:Z100000") -> List[List[str]]:
        try:
            resp = self.sheets_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range, majorDimension="ROWS").execute()
            return resp.get('values', [])
        except HttpError as e:
            self.log.err(f"Error reading sheet values: {e}")
            raise

    def dedupe_existing_ids_from_sheet(self,
                                       existing_vals: List[List[str]],
                                       id_column: str) -> set:
        if not existing_vals:
            return set()
        header = existing_vals[0]
        id_idx = None
        for idx, h in enumerate(header):
            if str(h).strip().lower() == str(id_column).strip().lower():
                id_idx = idx
                break
        if id_idx is None:
            raise ValueError('ID_COLUMN_NOT_IN_SHEET')
        existing_ids = { str(row[id_idx]).strip() for row in existing_vals[1:] if len(row) > id_idx and str(row[id_idx]).strip() != '' }
        return existing_ids

    def create_header_if_missing(self,
                                 spreadsheet_id: str,
                                 header: List[str]) -> None:
        try:
            self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range="A1",
                valueInputOption='RAW',
                body={'values': [header]}
            ).execute()
            self.log.inf('Header written to empty sheet.')
        except HttpError as e:
            self.log.err(f"Error writing header: {e}")
            raise

    def prepare_rows_to_append(self,
                               df_to_append: pd.DataFrame,
                               write_header: List[str]) -> List[List[str]]:
        rows = []
        for _, row in df_to_append.iterrows():
            line = []
            for col in write_header:
                val = row.get(col, "")
                if hasattr(val, 'isoformat'):
                    try:
                        val = val.isoformat()
                    except Exception:
                        val = str(val)
                line.append("") if pd.isna(val) else line.append(str(val))
            rows.append(line)
        return rows

    def append_rows(self,
                    spreadsheet_id: str,
                    rows: List[List[str]]) -> int:
        if not rows:
            return 0
        try:
            resp = self.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range="A1",
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body={'values': rows}
            ).execute()
            self.log.inf(f"{len(rows)} rows appended to spreadsheet {spreadsheet_id}")
            return len(rows)
        except HttpError as e:
            self.log.err(f"Error appending rows: {e}")
            raise

    def apply_validations_on_sheet(self,
                                   spreadsheet_id: str,
                                   sheet_id: Optional[int],
                                   write_header: List[str],
                                   appended_count: int) -> None:
        try:
            if sheet_id is None:
                meta = self.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id, fields='sheets.properties').execute()
                sheet_id = meta['sheets'][0]['properties']['sheetId']
        except Exception:
            sheet_id = None

        if sheet_id is None:
            return

        validation_requests = []
        if 'homologado' in write_header:
            col_index = write_header.index('homologado')
            validation_requests.append({
                "setDataValidation": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": max(1, appended_count+2),
                        "startColumnIndex": col_index,
                        "endColumnIndex": col_index+1
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [{"userEnteredValue":"SIM"}, {"userEnteredValue":"NAO"}]
                        },
                        "showCustomUi": True,
                        "strict": True
                    }
                }
            })

        if validation_requests:
            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": validation_requests}
            ).execute()
            self.log.inf('Validation rules applied.')

    def share_sheet_with_emails(self,
                                spreadsheet_id: str,
                                authorized_emails: List[str]) -> List[tuple]:
        errors: List[tuple] = []
        for email in authorized_emails:
            try:
                self.drive_service.permissions().create(
                    fileId=spreadsheet_id,
                    body={'type':'user','role':'writer','emailAddress':email},
                    sendNotificationEmail=False,
                    supportsAllDrives=True
                ).execute()
                self.log.inf(f"Shared (writer) with {email}")
            except HttpError as e:
                status = getattr(e.resp, 'status', None)
                self.log.err(f"Error sharing ({status}) with {email}: {e}")
                errors.append((email, status, str(e)))
        return errors

    def get_spreadsheet_url(self,
                            spreadsheet_id: str) -> str:
        return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"

