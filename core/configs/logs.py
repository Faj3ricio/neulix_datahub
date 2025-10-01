"""
╔═════════════════════════════╗
║ Module: Agent Logger        ║
╚═════════════════════════════╝
Handles structured logging with context awareness using Google Cloud Logging.
"""
from __future__ import annotations

import os
import logging
import ast
import inspect
import io
import csv
import json
import mimetypes
import smtplib
from typing import Any, Dict, List, Optional, Tuple
from email.message import EmailMessage

from google.cloud import logging as cloud_logging
from google.cloud.logging.handlers import CloudLoggingHandler

class AgentLogger:
    """
    AgentLogger é responsável por configurar e gerenciar o logging estruturado
    utilizando o Google Cloud Logging.
    """

    def __init__(self):
        """
        Inicializa o AgentLogger.

        Parâmetros:
            project (str): ID do projeto GCP.
            credentials_path (str): Caminho para o arquivo JSON de credenciais do Google Cloud.

        Se credentials_path for fornecido, a variável de ambiente
        GOOGLE_APPLICATION_CREDENTIALS será configurada.
        """

        self.client = cloud_logging.Client()
        self.logger = logging.getLogger('AgentLogger')
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        try:
            cloud_handler = CloudLoggingHandler(self.client)
            cloud_handler.setLevel(logging.INFO)
            if not any(isinstance(h, CloudLoggingHandler) for h in self.logger.handlers):
                self.logger.addHandler(cloud_handler)
        except Exception as e:
            print(f"[ERROR] Falha ao adicionar CloudLoggingHandler: {e}")

    def _get_caller_info(self):
        """
        Retorna uma string com o nome da classe, método e linha de onde o log foi chamado.
        Usa o módulo inspect para obter o frame de quem chamou o método de log.
        """
        stack = inspect.stack()
        if len(stack) > 3:
            frame_info = stack[3]
            function_name = frame_info.function
            line_number = frame_info.lineno
            caller_instance = frame_info.frame.f_locals.get('self', None)
            if caller_instance:
                class_name = caller_instance.__class__.__name__
                return f"{class_name}.{function_name} - Line: {line_number}"
            return f"{function_name} - Line: {line_number}"
        return "N/A"

    def _format_message(self, prefix: str, message: str):
        """
        Adiciona ao final da mensagem as informações do caller (classe e método).
        """
        caller_info = self._get_caller_info()
        return f"{prefix} {message} -> ({caller_info})"

    def inf(self, message):
        """Registra uma mensagem de nível INFO."""
        msg = self._format_message("[ oo INFO ]", message)
        self.logger.info(msg)
        print(f"\033[32m{msg}\033[0m")

    def war(self, message):
        """Registra uma mensagem de nível WARNING."""
        msg = self._format_message("[ <> WARNING ]", message)
        self.logger.warning(msg)
        print(f"\033[33m{msg}\033[0m")

    def err(self, message):
        """Registra uma mensagem de nível ERROR."""
        msg = self._format_message("[ ^^ ERROR ]", message)
        self.logger.error(msg)
        print(f"\033[1;31m{msg}\033[0m")

    def cri(self, message):
        """Registra uma mensagem de nível CRITICAL."""
        msg = self._format_message("[ xx CRITICAL ]", message)
        self.logger.critical(msg)
        print(f"\033[91m{msg}\033[0m")

    def deb(self, message):
        """Registra uma mensagem de nível DEBUG."""
        msg = self._format_message("[ ++ DEBUG ]", message)
        self.logger.debug(msg)
        print(f"\033[36m{msg}\033[0m")

    def build_smtp_transport(self,
                            config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Monta um dicionário de configuração SMTP a partir do `config` passado
        ou das variáveis de ambiente. Não cria conexão apenas normaliza os
        parâmetros necessários para envio.
        Retorna:
            dict com as chaves: host, port, user, password, starttls
        Help rápido:
            transport = build_smtp_transport()
        """
        cfg = config or {}
        host = cfg.get('smtp_host') or os.environ.get('SMTP_HOST', 'smtp.gmail.com')
        port = int(cfg.get('smtp_port') or os.environ.get('SMTP_PORT', 587))
        user = cfg.get('smtp_user') or os.environ.get('SMTP_USER')
        password = cfg.get('smtp_password') or os.environ.get('SMTP_PASSWORD')
        starttls = bool(cfg.get('smtp_starttls', True))
        return {'host': host, 'port': port, 'user': user, 'password': password, 'starttls': starttls}

    def build_message(self,
                     subject: str,
                     body: str, 
                     from_addr: str, 
                     to_addrs: List[str], 
                     from_name: Optional[str] = None) -> EmailMessage:
        """
        Constroi e retorna um objeto EmailMessage básico com o corpo em plain text.
        Parâmetros:
            subject: assunto do e-mail
            body: corpo em texto
            from_addr: endereço remetente
            to_addrs: lista de destinatários
            from_name: nome exibido no remetente (opcional)
        Uso:
            msg = build_message('Assunto','corpo','me@ex.com', ['dest@ex.com'])
        """
        msg = EmailMessage()
        sender = f"{from_name} <{from_addr}>" if from_name else from_addr
        msg['From'] = sender
        msg['To'] = ', '.join(to_addrs)
        msg['Subject'] = subject
        msg.set_content(body)
        return msg

    def attach_dataframe(self,
                         msg: EmailMessage,
                         df: Any,
                         csv_name: str = 'data.csv',
                         add_html_alternative: bool = False) -> None:
        """
        Anexa um DataFrame / lista de dicts / dict como arquivo.
        Importante: por padrão NÃO adiciona alternativa HTML (evita injetar tabela no body).
        Se add_html_alternative=True, então tentará adicionar HTML (use apenas se quiser ver tabela no corpo).
        """
        # 1) Gerar alternativa HTML apenas se explicitado
        if add_html_alternative:
            try:
                if hasattr(df, 'to_html'):
                    html_table = df.to_html(index=False, na_rep='')
                else:
                    html_table = '<pre>' + json.dumps(df, default=str, ensure_ascii=False) + '</pre>'
                html_body = f"<p>Segue os dados em anexo:</p>\n{html_table}"
                msg.add_alternative(html_body, subtype='html')
            except Exception as e:
                self.err(f"Falha ao gerar alternativa HTML para o DataFrame: {e}")
                # 1.1) fallback será anexar arquivo .html/CSV

        # 2) Anexa CSV ou JSON como attachment (sempre anexo)
        try:
            csv_buf = io.StringIO()
            # 2.1) Se for pandas DataFrame
            if hasattr(df, 'to_csv'):
                df.to_csv(csv_buf, index=False)
                csv_bytes = csv_buf.getvalue().encode('utf-8')
                msg.add_attachment(csv_bytes, maintype='text', subtype='csv', filename=csv_name)
            # 2.2) Se for lista de dicts > CSV
            elif isinstance(df, list) and df and isinstance(df[0], dict):
                header = list(df[0].keys())
                extra = [k for r in df for k in r.keys() if k not in header]
                for k in extra:
                    header.append(k)
                buf = io.StringIO()
                writer = csv.DictWriter(buf, fieldnames=header, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(df)
                msg.add_attachment(buf.getvalue().encode('utf-8'), maintype='text', subtype='csv', filename=csv_name)
            elif isinstance(df, dict):
                blob = json.dumps(df, default=str, ensure_ascii=False).encode('utf-8')
                msg.add_attachment(blob, maintype='application', subtype='json', filename='data.json')
            else:
                blob = json.dumps(df, default=str, ensure_ascii=False).encode('utf-8')
                msg.add_attachment(blob, maintype='application', subtype='json', filename='data.json')
        except Exception as e:
            self.err(f"Erro ao anexar DataFrame como CSV/JSON: {e}")

    def attach_bytes(self,
                    msg: EmailMessage,
                    content: bytes,
                    filename: str) -> None:
        """
        Anexa um arquivo binário ao EmailMessage. Detecta o mime-type com mimetypes.

        Uso:
            attach_bytes(msg, b'conteudo', 'arquivo.bin')
        """
        ctype, _ = mimetypes.guess_type(filename)
        if ctype:
            maintype, subtype = ctype.split('/', 1)
        else:
            maintype, subtype = 'application', 'octet-stream'
        msg.add_attachment(content, maintype=maintype, subtype=subtype, filename=filename)

    # ---------------------- Envio SMTP ----------------------
    def send_message_via_smtp(self,
                             msg: EmailMessage,
                             transport_config: Dict[str, Any]) -> None:
        """
        Envia um EmailMessage via SMTP usando as configurações informadas.
        transport_config deve conter: host, port, user, password, starttls
        Lança exceção em caso de erro para que o chamador trate/logue.
        """
        host = transport_config.get('host')
        port = int(transport_config.get('port', 587))
        user = transport_config.get('user')
        password = transport_config.get('password')
        starttls = bool(transport_config.get('starttls', True))

        with smtplib.SMTP(host, port, timeout=30) as smtp:
            smtp.ehlo()
            if starttls and port in (587,):
                smtp.starttls()
                smtp.ehlo()
            if user and password:
                smtp.login(user, password)
            smtp.send_message(msg)

    def _normalize_payload(self,
                          raw: Any) -> Any:
        """
        Tenta transformar payload (que pode ser dict, str(json), str(repr)) em objeto Python.
        """
        if raw is None:
            return None
        if isinstance(raw, dict) or isinstance(raw, list):
            return raw
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except Exception:
                pass
            try:
                return ast.literal_eval(raw)
            except Exception:
                pass
            return raw
        return raw


    # ---------------------- Wrappers e função consolidada ----------------------
    def send_email(self,
                   to_emails: List[str],
                   subject: str,
                   body_text: str,
                   data: Optional[Any] = None,
                   attachments: Optional[List[Tuple[str, bytes]]] = None,
                   from_name: Optional[str] = None,
                   config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Método principal:
        - Constrói a mensagem plain-text (body_text).
        - Se 'data' for passado, converte para anexo (CSV/JSON) e NÃO injeta no body.
        - 'attachments' também é suportado (lista de tuples: (filename, bytes)).
        """
        transport_cfg = self.build_smtp_transport(config)
        smtp_from = (config.get('smtp_from') if config else None) or os.environ.get('SMTP_FROM') or transport_cfg.get('user')
        if not smtp_from:
            err = 'smtp_from não configurado (smtp_from / SMTP_FROM)'
            self.err(err)
            return {'sent': False, 'error': err}

        try:
            msg = self.build_message(subject=subject, body=body_text, from_addr=smtp_from, to_addrs=to_emails, from_name=from_name)
            normalized = self._normalize_payload(data)

            if isinstance(normalized, dict) and 'appended_rows' in normalized:
                appended = normalized.get('appended_rows') or []
                if appended:
                    self.attach_dataframe(msg, appended, csv_name='homologated_rows.csv', add_html_alternative=False)
                if normalized.get('spreadsheet_url'):
                    url_blob = normalized.get('spreadsheet_url').encode('utf-8')
                    msg.add_attachment(url_blob, maintype='text', subtype='plain', filename='spreadsheet_url.txt')

            elif isinstance(normalized, list) or (hasattr(normalized, 'to_csv') if normalized is not None else False):
                self.attach_dataframe(msg, normalized, csv_name='data.csv', add_html_alternative=False)

            elif isinstance(normalized, dict):
                self.attach_dataframe(msg, normalized, csv_name='data.json', add_html_alternative=False)

            if attachments:
                for fname, bts in attachments:
                    self.attach_bytes(msg, bts, fname)

            self.send_message_via_smtp(msg, transport_cfg)
            self.inf(f"E-mail enviado: {to_emails} - subject: {subject}")
            return {'sent': True, 'recipients': to_emails}
        except Exception as exc:
            self.err(f"Erro enviando email: {exc}")
            return {'sent': False, 'error': str(exc)}

    def alert_email(self,
                    to_emails: List[str],
                    subject: str,
                    body: str,
                    data: Optional[Any] = None,
                    attachments: Optional[List[Tuple[str, bytes]]] = None,
                    from_name: Optional[str] = None,
                    config: Optional[Dict[str, Any]] = None, log: bool = True) -> Dict[str, Any]:
        """
        Wrapper que só registra e chama send_email.
        Observação: por padrão NÃO adiciona a tabela no body; ela vira anexo quando possível.
        """
        if log:
            self.err(f"Enviando alerta por e-mail para: {to_emails} - assunto: {subject}")
        return self.send_email(to_emails=to_emails, subject=subject, body_text=body,
                               data=data, attachments=attachments, from_name=from_name, config=config)
