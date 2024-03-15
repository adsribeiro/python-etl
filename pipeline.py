import duckdb
import os
import ast
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from pathlib import Path
from duckdb import DuckDBPyRelation, DuckDBPyConnection
from sqlalchemy import create_engine
from pandas import DataFrame
from dotenv import load_dotenv
from datetime import datetime


# path = Path("gdown")

# for f in path.glob("*"):
#     print(f.name, f.suffix)

load_dotenv()

token_info = ast.literal_eval(os.getenv("TOKEN"))

def connect_db() -> DuckDBPyConnection:
    """Conecta ao banco de dados DuckDB: cria o bando se não existir."""
    return duckdb.connect(database="duckdb.db", read_only=False)

def init_table(con: DuckDBPyConnection):
    """Cria a tabela se ela não existir"""
    con.execute("""
    CREATE TABLE IF NOT EXISTS historico_arquivos(
                nome_arquivo VARCHAR,
                horario_processamento TIMESTAMP
    )
""")
    
def insert_file(con:DuckDBPyConnection, filename:Path):
    """Registra um novo arquivo no banco de dados com horário atual"""
    con.execute("""
    INSERT INTO historico_arquivos(nome_arquivo, horario_processamento)
    VALUES (?, ?)
""",(str(filename.name), datetime.now()))

def processed_files(con:DuckDBPyConnection):
    """Retorna um set com os nomes dos arquivos já processados"""
    return set(row[0] for row in con.execute("SELECT nome_arquivo FROM historico_arquivos").fetchall())

def create_drive_service():
    """Função para autenticar e criar um serviço do Google Drive"""
    creds = Credentials.from_authorized_user_info(token_info) # Altere para o caminho do seu arquivo de credenciais
    service = build('drive', 'v3', credentials=creds)
    return service

# Função para exportar um arquivo do Google para XLSX e baixá-lo
def export_and_download_sheet(service, file_id, file_name, destination_folder):
    # Exportar para XLSX
    request = service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    file_path = destination_folder / f"{file_name}.xlsx"

    # Download do arquivo exportado
    with open(file_path, 'wb') as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()

    print(f'Arquivo exportado e baixado: {file_name}.xlsx')

# Função para exportar um arquivo do Google para PDF e baixá-lo
def export_and_download_file(service, file_id, file_name, destination_folder):
    # Exportar para PDF
    request = service.files().export_media(fileId=file_id, mimeType='application/pdf')
    file_path = destination_folder / f"{file_name}.pdf"

    # Download do arquivo exportado
    with open(file_path, 'wb') as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()

    print(f'Arquivo exportado e baixado: {file_name}.pdf')

# Função para baixar todos os arquivos de uma pasta
def download_files_from_folder(folder_id, destination_folder):
    service = create_drive_service()
    page_token = None

    # Loop para buscar os arquivos na pasta
    while True:
        response = service.files().list(q=f"'{folder_id}' in parents",
                                        spaces='drive',
                                        fields='nextPageToken, files(id, name, mimeType)',
                                        pageToken=page_token).execute()
        files = response.get('files', [])

        # Baixar cada arquivo na pasta
        for file in files:
            file_id = file.get('id')
            file_name = file.get('name')
            file_mime_type = file.get('mimeType', '')

            # Define o caminho do arquivo
            file_path = destination_folder / file_name

            if 'application/vnd.google-apps' in file_mime_type:
                # Arquivo do Google Docs, Google Sheets, etc.
                if 'spreadsheet' in file_mime_type:
                    export_and_download_sheet(service, file_id, file_name, destination_folder)
                else:
                    export_and_download_file(service, file_id, file_name, destination_folder)
            else:
                # Arquivo binário ou de texto
                request = service.files().get_media(fileId=file_id)
                fh = file_path.open('wb')  # Abre o arquivo local para escrita em modo binário

                # Download do arquivo
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()

                fh.close()  # Fecha o arquivo após o download
                print(f'Arquivo baixado: {file_name}')

        # Verificar se há mais páginas de resultados
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

def read_file(file_path:Path, file_type):
    """Lê o arquivo de acordo com a sua extensão e retorna um dataframe"""
    if file_type == ".csv":
        return duckdb.read_csv(file_path)
    elif file_type == ".json":
        return duckdb.read_json(str(file_path))
    elif file_type == ".parquet":
        return duckdb.read_parquet(str(file_path))
    else:
        raise ValueError(f"Tipo do arquivo não suportado: {file_type}")
def transnform(df) -> DataFrame:
    df_transf = duckdb.sql("SELECT *, quantidade * valor as total_vendas from df").df()
    return df_transf

def list_files(file_path) -> list[Path]:
    return list(file_path.glob("*"))

def save_to_postgres(df: DataFrame, table: str):
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL)
    #Salvar no PostgreSQL
    df.to_sql(table, con=engine, if_exists='append', index=False)

def pipeline():

    # Define a ID da pasta do Google Drive que você deseja baixar
    folder_id = '15WafqiRULmp-Iw38hLOBm44vXmQRJcKC'

    # Pasta de destino onde os arquivos serão salvos
    destination_folder = Path('gdown')

    # Criar a pasta de destino se não existir
    destination_folder.mkdir(parents=True, exist_ok=True)

    # Baixar os arquivos da pasta do Google Drive
    download_files_from_folder(folder_id, destination_folder)

    files = list_files(destination_folder)
    con =connect_db()
    init_table(con)
    p_files = processed_files(con)

    logs = []
    for file in  files:
        if file.name not in p_files:
            df_duckdb = read_file(file, file.suffix)
            pandas_df = transnform(df_duckdb)
            save_to_postgres(pandas_df, "vendas_calculado")
            insert_file(con, file)
            print(f"Arquivo {file} processado e salvo!")
            logs.append(f"Arquivo {file} processado e salvo!")
        else:
            print(f"Arquivo {file} já foi processado anteriormente!")
            logs.append(f"Arquivo {file} já foi processado anteriormente!")
    return logs

if __name__=="__main__":
    pipeline()