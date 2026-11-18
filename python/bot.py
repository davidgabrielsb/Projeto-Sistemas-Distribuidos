import os
import time
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

def iniciar_bot():
    while enviar_requisicao('login', {'usuario': USUARIO}).get('status') != 'sucesso':
        time.sleep(1)
    rodada = 0
    while True:
        resposta = enviar_requisicao('listar_canais', {})
        canais = resposta.get('dados', {}).get('canais', [])
        if len(canais) < 5:
            enviar_requisicao('criar_canal', {'nome': f'{CANAL}-{rodada}', 'usuario': USUARIO})
            rodada += 1
        time.sleep(3)

if __name__ == '__main__':
    iniciar_bot()
