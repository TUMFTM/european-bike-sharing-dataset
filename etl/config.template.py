import socket
import pathlib

db_name = 'postgres'
db_username = 'upload_api'
db_password = 'secret'
db_host = 'my-database.cooldomain.com'
db_port = 5432

PAGE_SIZE = 1000

upload_host = socket.gethostname()
FOLDER_ZIP = pathlib.Path('data')
