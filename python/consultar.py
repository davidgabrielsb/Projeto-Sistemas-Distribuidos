import argparse
import os
from datetime import datetime, timezone

import msgpack
import zmq


def consultar(endpoint, tipo, dados):
    with zmq.Context() as contexto, contexto.socket(zmq.REQ) as socket:
        socket.linger = 0
        socket.rcvtimeo = 5000
        socket.sndtimeo = 5000
        socket.connect(endpoint)
        mensagem = {
            'tipo': tipo,
            'dados': dados,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        print(f'ENVIO {mensagem}', flush=True)
        socket.send(msgpack.packb(mensagem, use_bin_type=True))
        resposta = msgpack.unpackb(socket.recv(), raw=False)
        print(f'RECEBIMENTO {resposta}', flush=True)
        return resposta


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('operacao', choices=['listar_canais', 'historico'])
    parser.add_argument('--canal', default='geral')
    parser.add_argument('--inicio', type=int, default=0)
    parser.add_argument('--endpoint', default=os.getenv('SERVER_ENDPOINT', 'tcp://broker:5555'))
    args = parser.parse_args()
    consultar(args.endpoint, args.operacao, {'canal': args.canal, 'inicio': args.inicio, 'limite': 100})
