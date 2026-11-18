import zmq
import msgpack
import sqlite3
import os
import time
import threading
from cluster import Cluster
from datetime import datetime, timezone

SERVER_ENDPOINT = os.getenv("SERVER_ENDPOINT")
PUB_ENDPOINT = os.getenv("PUB_ENDPOINT", "tcp://pubsub-proxy:5557")
ENDERECO = os.getenv("SERVER_BIND", "tcp://*:5555")

PASTA = os.path.dirname(os.path.abspath(__file__))
BANCO = os.getenv("DB_PATH", os.path.join(PASTA, "servidor.db"))

relogio_logico = 0
lock_relogio = threading.RLock()
diferenca_horario = 0.0

def atualizar_relogio(relogio_recebido):
    global relogio_logico

    if isinstance(relogio_recebido, int) and relogio_recebido >= 0:
        with lock_relogio:
            relogio_logico = max(relogio_logico, relogio_recebido)

def incrementar_relogio():
    global relogio_logico
    with lock_relogio:
        relogio_logico += 1
        return relogio_logico

def obter_timestamp():
    horario = datetime.fromtimestamp(
        datetime.now(timezone.utc).timestamp() + diferenca_horario,
        timezone.utc
    )
    return horario.isoformat(timespec="milliseconds").replace("+00:00", "Z")

def metadados_envio():
    return {
        "timestamp": obter_timestamp(),
        "relogio_logico": incrementar_relogio()
    }

def criar_banco():
    conexao = sqlite3.connect(BANCO)
    cursor = conexao.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS logins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evento_id TEXT UNIQUE,
            usuario TEXT NOT NULL,
            timestamp TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS canais (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evento_id TEXT UNIQUE,
            nome TEXT NOT NULL UNIQUE,
            usuario TEXT NOT NULL,
            timestamp TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mensagens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evento_id TEXT UNIQUE,
            canal TEXT NOT NULL,
            usuario TEXT NOT NULL,
            mensagem TEXT NOT NULL,
            timestamp_envio TEXT NOT NULL,
            timestamp_publicacao TEXT NOT NULL
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

def processar_mensagem(requisicao, socket_pub):

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

    if tipo == "historico":
        canal = dados.get('canal', '')
        limite = max(1, min(100, int(dados.get('limite', 50))))
        inicio = max(0, int(dados.get('inicio', 0)))
        cursor.execute('SELECT canal,usuario,mensagem,timestamp_envio,timestamp_publicacao,evento_id FROM mensagens WHERE canal=? ORDER BY timestamp_envio,evento_id LIMIT ? OFFSET ?', (canal, limite, inicio))
        campos = ['canal','usuario','mensagem','timestamp_envio','timestamp_publicacao','evento_id']
        mensagens = [dict(zip(campos, linha)) for linha in cursor.fetchall()]
        conexao.close()
        return dict(tipo=tipo, status='sucesso', dados={'mensagens': mensagens, 'inicio': inicio, 'limite': limite})

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

    elif tipo == "publicar_mensagem":

        canal = dados.get("canal", "")
        usuario = dados.get("usuario", "")
        conteudo = dados.get("mensagem", "")

        if not all(
            isinstance(valor, str)
            for valor in (canal, usuario, conteudo)
        ):
            conexao.close()
            return resposta(tipo, "erro", "Dados da mensagem invalidos")

        canal = canal.strip()
        usuario = usuario.strip()
        conteudo = conteudo.strip()

        if not canal or not usuario or not conteudo:
            conexao.close()
            return resposta(
                tipo,
                "erro",
                "Canal, usuario e mensagem sao obrigatorios"
            )

        metadados_publicacao = metadados_envio()
        timestamp_publicacao = metadados_publicacao["timestamp"]

        publicacao = {
            "canal": canal,
            "usuario": usuario,
            "mensagem": conteudo,
            "timestamp_envio": timestamp,
            "timestamp_publicacao": timestamp_publicacao,
            "timestamp": timestamp_publicacao,
            "relogio_logico": metadados_publicacao["relogio_logico"]
        }

        try:
            cursor.execute("""
                INSERT INTO mensagens (
                    canal,
                    usuario,
                    mensagem,
                    timestamp_envio,
                    timestamp_publicacao
                )
                VALUES (?, ?, ?, ?, ?)
            """, (
                canal,
                usuario,
                conteudo,
                timestamp,
                timestamp_publicacao
            ))

            conexao.commit()

            socket_pub.send_multipart([
                (canal + "\0").encode("utf-8"),
                msgpack.packb(publicacao, use_bin_type=True)
            ])

            conexao.close()

            print("\n========== MENSAGEM PUBLICADA ==========")
            print(publicacao, flush=True)

            return {
                "tipo": tipo,
                "status": "sucesso",
                "mensagem": "Mensagem publicada com sucesso",
                "dados": {"canal": canal},
                "timestamp": timestamp_publicacao
            }

        except Exception:
            conexao.close()
            raise

    conexao.close()

    return resposta(tipo, "erro", "Operacao desconhecida")

def ajustar_horario(delta):
    global diferenca_horario
    diferenca_horario += delta / 1000

def iniciar_servidor():
    criar_banco()
    cluster = Cluster(BANCO, metadados_envio, atualizar_relogio, ajustar_horario,
                      lambda: (time.time() + diferenca_horario) * 1000)
    cluster.iniciar()
    with zmq.Context() as contexto:
        socket_rep = contexto.socket(zmq.REP)
        socket_pub = contexto.socket(zmq.PUB)
        socket_pub.connect(PUB_ENDPOINT)
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
                    if isinstance(requisicao, dict):
                        atualizar_relogio(requisicao.get('relogio_logico', 0))
                    print(f'RECEBIMENTO {requisicao}', flush=True)
                    cluster.contar()
                    resultado = processar_mensagem(requisicao, socket_pub)
                except Exception as erro:
                    print(f'Falha no processamento: {erro}', flush=True)
                    resultado = resposta('desconhecido', 'erro', 'Mensagem invalida')
                resultado.update(metadados_envio())
                print(f'ENVIO {resultado}', flush=True)
                socket_rep.send(msgpack.packb(resultado, use_bin_type=True))
                cluster.contar_troca()
        finally:
            socket_rep.close(0)
            socket_pub.close(0)

if __name__ == '__main__':
    iniciar_servidor()
