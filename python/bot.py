import os
import time
import random
from datetime import datetime, timezone

import zmq
import msgpack

BOT_ID = os.getenv("BOT_ID", "python-1")
USUARIO = f"bot-{BOT_ID}"
CANAL = f"canal-{BOT_ID}"

SERVIDOR = os.getenv(
    "SERVER_ENDPOINT",
    "tcp://localhost:5555"
)

SUB_ENDPOINT = os.getenv(
    "SUB_ENDPOINT",
    "tcp://pubsub-proxy:5558"
)

def obter_timestamp():
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )

def criar_mensagem(tipo, dados):
    return {
        "tipo": tipo,
        "dados": dados,
        "timestamp": obter_timestamp(),
    }

def enviar_requisicao(tipo, dados):
    requisicao = criar_mensagem(tipo, dados)
    print(f'ENVIO {requisicao}', flush=True)
    with zmq.Context.instance().socket(zmq.REQ) as pedido:
        pedido.setsockopt(zmq.LINGER, 0)
        pedido.setsockopt(zmq.RCVTIMEO, 3000)
        pedido.setsockopt(zmq.SNDTIMEO, 3000)
        pedido.connect(SERVIDOR)
        try:
            pedido.send(msgpack.packb(requisicao, use_bin_type=True))
            resposta = msgpack.unpackb(pedido.recv(), raw=False)
            print(f'RECEBIMENTO {resposta}', flush=True)
            return resposta
        except zmq.Again:
            return {'status': 'erro', 'mensagem': 'Tempo de resposta excedido', 'dados': {}}

def receber_mensagens(socket_sub, prazo):
    while time.monotonic() < prazo:
        if not socket_sub.poll(min(100, max(1, int((prazo-time.monotonic())*1000)))):
            continue
        canal, bytes_recebidos = socket_sub.recv_multipart()
        publicacao = msgpack.unpackb(bytes_recebidos, raw=False)
        print(f'PUB-SUB canal={canal.decode().rstrip(chr(0))} recebido_em={obter_timestamp()} conteudo={publicacao}', flush=True)

def iniciar_bot():
    with zmq.Context() as contexto:
        socket_sub = contexto.socket(zmq.SUB)
        socket_sub.connect(SUB_ENDPOINT)
        inscritos = set()
        try:
            while enviar_requisicao('login', {'usuario': USUARIO}).get('status') != 'sucesso':
                time.sleep(1)
            rodada = 0
            while True:
                resposta = enviar_requisicao('listar_canais', {})
                canais = resposta.get('dados', {}).get('canais', [])
                if len(canais) < 5:
                    enviar_requisicao('criar_canal', {'nome': f'{CANAL}-{rodada}', 'usuario': USUARIO})
                    rodada += 1
                    canais = enviar_requisicao('listar_canais', {}).get('dados', {}).get('canais', [])
                disponiveis = list(set(canais) - inscritos)
                random.shuffle(disponiveis)
                for canal in disponiveis[:max(0, 3-len(inscritos))]:
                    socket_sub.subscribe((canal + '\0').encode())
                    inscritos.add(canal)
                    print(f'Inscrito em {canal}', flush=True)
                if not canais:
                    receber_mensagens(socket_sub, time.monotonic()+1)
                    continue
                canal = random.choice(canais)
                for numero in range(1, 11):
                    enviar_requisicao('publicar_mensagem', {
                        'canal': canal, 'usuario': USUARIO,
                        'mensagem': f'Mensagem {numero} de {USUARIO}: {random.randint(1000,9999)}'})
                    receber_mensagens(socket_sub, time.monotonic()+1)
        finally:
            socket_sub.close(0)

if __name__ == '__main__':
    iniciar_bot()
