"""
╔═════════════════════════════╗
║ Module: DataCore            ║
╚═════════════════════════════╝
Data operations and I/O integration.
"""
import os
import json
import shutil
import tempfile
from pathlib import Path
from typing import Optional, List, Dict, Any
import numpy as np
import pandas as pd

from painel_financeiro_aav.core.configs.logs import AgentLogger


# try:
#     from sdv.tabular import CTGAN
# except ImportError:
#     try:
#         from sdv.single_table import CTGANSynthesizer as CTGAN
#     except ImportError:
#         try:
#             from ctgan import CTGANSynthesizer as CTGAN
#         except ImportError:
#             raise ImportError(
#                 "CTGAN não encontrado. Instale o pacote 'sdv' ou 'ctgan' via 'pip install sdv ctgan'."
#             )
# try:
#     from sdv.metadata import SingleTableMetadata
# except ImportError:
#     SingleTableMetadata = None

class DataCore:
    """
    Classe responsável por operações comuns de manipulação e I/O de dados,
    incluindo operações com DataFrames e arquivos em diversos formatos (Parquet, CSV, JSON, TXT),
    além de geração de dados sintéticos.
    """

    def __init__(self, project_id: str = None):
        """
        Inicializa o DataCore com logger de agente.
        """
        self.log = AgentLogger()

    def dataframe_to_parquet(self, data: pd.DataFrame, name_parquet: str) -> str:
        base_path = os.path.abspath(os.path.dirname(__file__))
        parquet_file = os.path.abspath(os.path.join(base_path, '..', 'data', 'processed', name_parquet))
        os.makedirs(os.path.dirname(parquet_file), exist_ok=True)
        data.to_parquet(parquet_file, index=False)
        self.log.inf(f"DataFrame salvo em Parquet: {parquet_file}")
        return parquet_file

    def dataframe_to_csv(self, data: pd.DataFrame, name_csv: str, delimiter: str = ',') -> str:
        base_path = os.path.abspath(os.path.dirname(__file__))
        csv_file = os.path.abspath(os.path.join(base_path, '..', 'data', 'processed', name_csv))
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)
        data.to_csv(csv_file, sep=delimiter, index=False)
        self.log.inf(f"DataFrame salvo em CSV: {csv_file}")
        return csv_file

    def csv_to_dataframe(self, file_path: str, delimiter: str = ',') -> pd.DataFrame:
        if not os.path.exists(file_path):
            self.log.err(f"Arquivo CSV não encontrado: {file_path}")
            raise FileNotFoundError(f"Arquivo CSV não encontrado: {file_path}")
        df = pd.read_csv(file_path, sep=delimiter)
        self.log.inf(f"CSV lido com sucesso: {file_path}")
        return df

    def parquet_to_dataframe(self, file_path: str) -> pd.DataFrame:
        if not os.path.exists(file_path):
            self.log.err(f"Arquivo Parquet não encontrado: {file_path}")
            raise FileNotFoundError(f"Arquivo Parquet não encontrado: {file_path}")
        df = pd.read_parquet(file_path)
        self.log.inf(f"Parquet lido com sucesso: {file_path}")
        return df

    def dataframe_to_json(self, data: pd.DataFrame, name_json: str) -> str:
        base_path = os.path.abspath(os.path.dirname(__file__))
        json_file = os.path.abspath(os.path.join(base_path, '..', 'data', 'processed', name_json))
        os.makedirs(os.path.dirname(json_file), exist_ok=True)
        data.to_json(json_file, force_ascii=False)
        self.log.inf(f"DataFrame salvo em JSON: {json_file}")
        return json_file

    def read_text_file(self, file_path: str) -> str:
        if not os.path.exists(file_path):
            self.log.err(f"Arquivo de texto não encontrado: {file_path}")
            raise FileNotFoundError(f"Arquivo de texto não encontrado: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        self.log.inf(f"Arquivo de texto lido com sucesso: {file_path}")
        return content

    def save_text_file(self, content: str, file_name: str) -> str:
        base_path = os.path.abspath(os.path.dirname(__file__))
        txt_file = os.path.abspath(os.path.join(base_path, '..', 'data', 'processed', file_name))
        os.makedirs(os.path.dirname(txt_file), exist_ok=True)
        with open(txt_file, 'w', encoding='utf-8') as f:
            f.write(content)
        self.log.inf(f"Conteúdo salvo em arquivo de texto: {txt_file}")
        return txt_file

    def describe_dataframe(self, data: pd.DataFrame) -> None:
        self.log.inf(f"Descrição do DataFrame:\n {data.describe()}")

    def generate_synthetic_data(
        self,
        input_csv: str,
        output_csv: str,
        num_rows: int,
        mutable_columns: list,
        id_column: str
    ) -> str:
        """
        Gera dados sintéticos a partir de um CSV usando SDV (CTGAN).

        Args:
            input_csv (str): Caminho do arquivo CSV de entrada.
            output_csv (str): Caminho do arquivo CSV de saída.
            num_rows (int): Quantidade de linhas sintéticas a gerar.
            mutable_columns (list): Lista de colunas que podem ser alteradas.
            id_column (str): Nome da coluna de ID para sequência contínua.

        Returns:
            str: Caminho do arquivo CSV gerado.
        """
        # 1) Lê dados originais
        data = pd.read_csv(input_csv)
        self.log.inf(f"CSV original lido: {input_csv} ({len(data)} linhas)")

        # 2) Cria metadata se disponível
        metadata = None
        if SingleTableMetadata is not None:
            metadata = SingleTableMetadata()
            metadata.detect_from_dataframe(data)
            self.log.inf("Metadata detectada para SDV CTGAN.")

        # 3) Instancia modelo
        try:
            if metadata is not None:
                model = CTGAN(metadata)
            else:
                model = CTGAN()
        except TypeError:
            model = CTGAN()
        self.log.inf(f"Instância do modelo CTGAN criada: {type(model).__name__}")

        # 4) Treina modelo
        try:
            model.fit(data)
        except TypeError:
            model.fit(data, [])
        self.log.inf("Modelo CTGAN treinado com sucesso.")

        # 5) Amostra novos registros
        synthetic = model.sample(num_rows)
        self.log.inf(f"{num_rows} registros sintéticos gerados.")

        # 6) Mantém colunas não mutáveis iguais ao original
        for col in data.columns:
            if col not in mutable_columns + [id_column]:
                synthetic[col] = data[col].iloc[:len(synthetic)].reset_index(drop=True)

        # 7) Atualiza IDs
        max_id = data[id_column].max()
        synthetic[id_column] = range(max_id + 1, max_id + 1 + len(synthetic))
        self.log.inf(f"IDs ajustados a partir de {max_id + 1}.")

        # 8) Salva CSV de saída
        synthetic.to_csv(output_csv, index=False)
        self.log.inf(f"Dados sintéticos salvos em: {output_csv}")
        return output_csv

    def inspect_output(self,
                   dataframe: Optional[pd.DataFrame] = None,
                   extract_task_id: Optional[str] = None,
                   ti=None) -> pd.DataFrame:
        """
        Gera um relatório tabular simples por coluna: tipagem, contagem de nulos,
        porcentagem de nulos, cardinalidade (unique), amostras e top-10 valores.
        Retorna um DataFrame com essa informação (uma linha por coluna).

        Parâmetros principais:
        - dataframe: DataFrame já disponível para inspecionar (opcional).
        - extract_task_id: se informado e dataframe for None, puxa o DataFrame do XCom deste task_id.
        - ti: TaskInstance do Airflow (necessário se for usar extract_task_id).
        """
        # 0) Pegar df via XCom se necessário
        if dataframe is None and extract_task_id:
            if ti is None:
                self.log.err("inspect_output: 'ti' ausente ao tentar puxar XCom com extract_task_id.")
                return pd.DataFrame()
            try:
                dataframe = ti.xcom_pull(task_ids=extract_task_id)
            except Exception as e:
                self.log.err(f"inspect_output: falha ao puxar XCom {extract_task_id}: {e}")
                return pd.DataFrame()

        # 1) Validações básicas
        if dataframe is None:
            self.log.war("inspect_output: DataFrame ausente; nada a inspecionar.")
            return pd.DataFrame()

        if not isinstance(dataframe, pd.DataFrame):
            try:
                dataframe = pd.DataFrame(dataframe)
                self.log.deb("inspect_output: input convertido para DataFrame.")
            except Exception as e:
                self.log.err(f"inspect_output: input inválido e não foi possível converter: {e}")
                return pd.DataFrame()

        df = dataframe.copy()
        n_rows, n_cols = df.shape
        self.log.inf(f"🔍 inspect_output: iniciando inspeção — {n_rows} linhas x {n_cols} colunas")

        # 2) Montar relatório por coluna (lista de dicts)
        rows = []
        SAMPLE_N = 5  # quantos exemplos mostrar na coluna sample_values
        for col in df.columns:
            try:
                ser = df[col]
            except Exception as e:
                self.log.deb(f"inspect_output: falha ao acessar coluna '{col}': {e}")
                continue

            # tipagem
            try:
                dtype = str(ser.dtype)
            except Exception:
                dtype = "unknown"

            # nulos / unique
            try:
                null_count = int(ser.isna().sum())
                null_pct = round((null_count / n_rows) if n_rows else 0.0, 6)
            except Exception:
                null_count, null_pct = None, None

            try:
                unique_count = int(ser.nunique(dropna=True))
            except Exception:
                unique_count = None

            # amostras (valores não nulos primeiros N)
            try:
                sample_vals = ser.dropna().astype(str).head(SAMPLE_N).tolist()
            except Exception:
                sample_vals = []

            # top-10 valores (serializável: lista de {value, count})
            top10_serializable = []
            try:
                vc = ser.fillna("__NULL__").astype(str).value_counts(dropna=False)
                top = vc.head(10)
                for v, c in zip(top.index.tolist(), top.values.tolist()):
                    top10_serializable.append({"value": v, "count": int(c)})
            except Exception as e:
                self.log.deb(f"inspect_output: top10 falhou para '{col}': {e}")

            row = {
                "column": col,
                "dtype": dtype,
                "null_count": null_count,
                "null_pct": null_pct,
                "unique_count": unique_count,
                "sample_values": sample_vals,
                "top_10": top10_serializable
            }
            rows.append(row)

            # log resumido por coluna (útil para visualizar direto no log do Airflow)
            try:
                self.log.inf(f"[COL] {col} | dtype={dtype} | nulls={null_count} ({null_pct*100 if null_pct is not None else 'NA'}%) | unique={unique_count}")
                if sample_vals:
                    self.log.deb(f"   sample (up to {SAMPLE_N} non-null): {sample_vals}")
            except Exception:
                pass

        # 3) Construir DataFrame resultado
        try:
            result_df = pd.DataFrame(rows)
        except Exception as e:
            self.log.err(f"inspect_output: falha ao construir summary DataFrame: {e}")
            result_df = pd.DataFrame(rows) if isinstance(rows, list) else pd.DataFrame()

        # 4) Push para XCom (resumido e serializável) - key consistente
        if ti:
            try:
                ti.xcom_push(key="inspect_summary", value=result_df.to_dict(orient="records"))
                self.log.inf("inspect_output: summary empurrado para XCom key='inspect_summary'")
            except Exception as e:
                self.log.deb(f"inspect_output: falha ao xcom_push summary: {e}")

        self.log.inf("✅ inspect_output concluído (relatório tabular e tipagem).")
        return result_df

# core = DataCore()
# core.generate_synthetic_data(
#     input_csv='/home/fabriciolopes/repos/aav_datastream/painel_financeiro_aav/neulix_dataflow/data/raw/sample_venda_produtos.csv',
#     output_csv='/home/fabriciolopes/repos/aav_datastream/painel_financeiro_aav/neulix_dataflow/data/processed/sdv_sample_venda_produtos.csv',
#     num_rows=50000,
#     mutable_columns=['grupo', 'cnpj_grupo', 'grupo_anunciante', 'cnpj_anunciante', 'nome_participante', 'cnpj_cpf_participante', 'lojas_ativas', 'sap_ativos', 'id_documento_transferencia', 'placa', 'marca', 'modelo', 'km', 'data_arremate', 'data_ultima_modificacao', 'faturamento_produto', 'recebido_produto'],
#     id_column='id'
# )