import zmq
import msgpack
import sqlite3
import os
from datetime import datetime, timezone

SERVER_ENDPOINT = os.getenv("SERVER_ENDPOINT")
ENDERECO = os.getenv("SERVER_BIND", "tcp://*:5555")

PASTA = os.path.dirname(os.path.abspath(__file__))
BANCO = os.getenv("DB_PATH", os.path.join(PASTA, "servidor.db"))

def obter_timestamp():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

def criar_banco():
    conexao = sqlite3.connect(BANCO)
    cursor = conexao.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS logins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario TEXT NOT NULL,
            timestamp TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS canais (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL UNIQUE,
            usuario TEXT NOT NULL,
            timestamp TEXT NOT NULL
        )
    """)

    conexao.commit()
    conexao.close()

    print(f"Banco de dados inicializado: {BANCO}", flush=True)

def resposta(tipo, status, mensagem):
    return {
        "tipo": tipo,
        "status": status,
        "mensagem": mensagem,
        "dados": {},
        "timestamp": obter_timestamp()
    }

def processar_mensagem(requisicao):

    if not isinstance(requisicao, dict):
        return resposta(
            "desconhecido",
            "erro",
            "Mensagem invalida"
        )

    tipo = requisicao.get("tipo")
    dados = requisicao.get("dados", {})
    timestamp = requisicao.get("timestamp")

    if not isinstance(timestamp, str) or not timestamp.strip():
        return resposta(tipo, "erro", "Timestamp invalido")

    if not isinstance(dados, dict):
        return resposta(tipo, "erro", "Dados invalidos")

    conexao = sqlite3.connect(BANCO)
    cursor = conexao.cursor()

    if tipo == "login":

        usuario = dados.get("usuario", "")

        if not isinstance(usuario, str) or not usuario.strip():
            conexao.close()
            return resposta(tipo, "erro", "Nome de usuario invalido")

        usuario = usuario.strip()

        cursor.execute(
            "INSERT INTO logins (usuario, timestamp) VALUES (?, ?)",
            (usuario, timestamp)
        )

        conexao.commit()
        conexao.close()

        return resposta(
            tipo,
            "sucesso",
            "Login realizado com sucesso"
        )

    elif tipo == "criar_canal":

        nome = dados.get("nome", "")
        usuario = dados.get("usuario", "")

        if not isinstance(nome, str) or not isinstance(usuario, str):
            conexao.close()
            return resposta(tipo, "erro", "Dados do canal invalidos")

        nome = nome.strip()
        usuario = usuario.strip()

        if not nome or not usuario:
            conexao.close()
            return resposta(
                tipo,
                "erro",
                "Nome do canal ou usuario invalido"
            )

        try:
            cursor.execute(
                "INSERT INTO canais (nome, usuario, timestamp) VALUES (?, ?, ?)",
                (nome, usuario, timestamp)
            )

            conexao.commit()
            conexao.close()

            return resposta(
                tipo,
                "sucesso",
                "Canal criado com sucesso"
            )

        except sqlite3.IntegrityError:
            conexao.close()
            return resposta(tipo, "erro", "Esse canal ja existe")

    elif tipo == "listar_canais":

        cursor.execute(
            "SELECT nome FROM canais ORDER BY nome"
        )

        canais = cursor.fetchall()
        conexao.close()

        return {
            "tipo": tipo,
            "status": "sucesso",
            "mensagem": "Lista de canais",
            "dados": {
                "canais": [canal[0] for canal in canais]
            },
            "timestamp": obter_timestamp()
        }

    conexao.close()

    return resposta(tipo, "erro", "Operacao desconhecida")

def iniciar_servidor():
    criar_banco()
    with zmq.Context() as contexto:
        socket_rep = contexto.socket(zmq.REP)
        if SERVER_ENDPOINT:
            socket_rep.connect(SERVER_ENDPOINT)
            if os.getenv("SERVER_BIND"):
                socket_rep.bind(ENDERECO)
        else:
            socket_rep.bind(ENDERECO)
        try:
            while True:
                bytes_recebidos = socket_rep.recv()
                try:
                    requisicao = msgpack.unpackb(bytes_recebidos, raw=False)
                    print(f'RECEBIMENTO {requisicao}', flush=True)
                    resultado = processar_mensagem(requisicao)
                except Exception as erro:
                    print(f'Falha no processamento: {erro}', flush=True)
                    resultado = resposta('desconhecido', 'erro', 'Mensagem invalida')
                resultado["timestamp"] = obter_timestamp()
                print(f'ENVIO {resultado}', flush=True)
                socket_rep.send(msgpack.packb(resultado, use_bin_type=True))
        finally:
            socket_rep.close(0)

if __name__ == '__main__':
    iniciar_servidor()
