import os
from dataclasses import dataclass
import subprocess

import paramiko
from dotenv import load_dotenv

import psycopg2
from scp import SCPClient


@dataclass
class PSQLData:
    host: str
    port: int
    user: str
    password: str
    dbname: str


class SSHConnection:
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.ssh = None

    def __enter__(self):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(self.host, self.port, self.user, self.password)
        return self.ssh

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.ssh:
            self.ssh.close()


class PSQLConnection:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None

    def __enter__(self):
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
        )
        self.conn.autocommit = True
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()


class TransferData:
    local_conn = None
    local_cur = None
    remote_conn = None
    remote_cur = None

    def get_ssh_tunnel_data(self):
        return {
            'host': os.getenv(f'SSH_HOST'),
            'port': int(os.getenv(f'SSH_PORT')),
            'user': os.getenv(f'SSH_USER'),
            'password': os.getenv(f'SSH_PASSWORD'),
        }

    def get_local_conn_data(self) -> PSQLData:
        return PSQLData(
            **{
                'host': os.getenv(f'LOCAL_DB_HOST'),
                'port': int(os.getenv(f'LOCAL_DB_PORT')),
                'dbname': os.getenv(f'LOCAL_DB_NAME'),
                'user': os.getenv(f'LOCAL_DB_USER'),
                'password': os.getenv(f'LOCAL_DB_PASSWORD'),
            }
        )

    def get_remote_conn_data(self) -> PSQLData:
        return PSQLData(
            **{
                'host': os.getenv(f'REMOTE_DB_HOST'),
                'port': int(os.getenv(f'LOCAL_DB_PORT')),
                'dbname': os.getenv(f'REMOTE_DB_NAME'),
                'user': os.getenv(f'REMOTE_DB_USER'),
                'password': os.getenv(f'REMOTE_DB_PASSWORD'),
            }
        )

    def dump_from_remote(self, ssh_connection):
        db_conn_data = self.get_remote_conn_data()
        pg_dump_cmd = (
            f"sudo "
            f"-u {db_conn_data.user} "
            f"pg_dump "
            f"{db_conn_data.dbname} "
            f"-F c > {os.getenv(f'REMOTE_DUMP_PATH')}"
        )
        stdin, stdout, stderr = ssh_connection.exec_command(pg_dump_cmd)
        stdout.channel.recv_exit_status()

    def restore_dump(self):
        db_conn_data = self.get_local_conn_data()
        restore_cmd = (
            f"sudo "
            f"-u {db_conn_data.user} "
            f"pg_restore "
            f"-d {db_conn_data.dbname} "
            f"-F c -c < {os.getenv(f'LOCAL_DUMP_PATH')}"
        )
        subprocess.call(restore_cmd, shell=True)

    def copy_dump_to_local(self, ssh_connection):
        with SCPClient(ssh_connection.get_transport()) as scp:
            scp.get(
                remote_path=os.getenv(f'REMOTE_DUMP_PATH'),
                local_path=os.getenv(f'LOCAL_DUMP_PATH'),
            )

    def clear_tables(self):
        db_conn_data = self.get_local_conn_data()
        with PSQLConnection(
            dbname=db_conn_data.dbname,
            user=db_conn_data.user,
            password=db_conn_data.password,
            host=db_conn_data.host,
            port=db_conn_data.port,
        ) as conn:
            cur = conn.cursor()
            # Очистка таблицы
            cur.execute("TRUNCATE common_file CASCADE;")
            cur.execute("TRUNCATE django_migrations CASCADE;")
            conn.commit()
        return

    def dump_data(self):
        with SSHConnection(**self.get_ssh_tunnel_data()) as ssh_connection:
            # Создание дампа удаленной базы данных
            self.dump_from_remote(ssh_connection=ssh_connection)

            # Восстановление дампа в локальной базе данных
            self.copy_dump_to_local(ssh_connection=ssh_connection)
        return

    def remove_old_files(self):
        os.remove(f"{os.getenv(f'LOCAL_DUMP_PATH')}")
        return

    def move_data(self):
        print('Dumping data and copy to local server...')
        self.dump_data()
        print('Dumping done.')
        print('Restoring dump...')
        self.restore_dump()
        print('Restoring done.')
        # print('Removing old files...')
        # self.remove_old_files()
        # print('Removing done.')
        print('Clearing tables in db...')
        self.clear_tables()
        print('Clearing dome...')
        print('Dont forget to execute command "python manage.py migrate --fake" in your project folder.')


if __name__ == '__main__':
    # Загрузка данных из .env файла
    load_dotenv()
    TransferData().move_data()

