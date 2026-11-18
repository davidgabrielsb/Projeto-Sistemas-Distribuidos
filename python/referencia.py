import os
import threading
import time

import msgpack
import zmq

class Referencia:
    def __init__(self, metadados, receber, ajustar, horario):
        self.metadados = metadados
        self.receber = receber
        self.ajustar = ajustar
        self.horario = horario
        self.nome = os.getenv('SERVER_NAME', 'python-server')
        self.referencia = os.getenv('REFERENCE_ENDPOINT', 'tcp://reference:5560')
        self.rank = 0
        self.contagem = 0
        self.heartbeat_pendente = threading.Event()
    def mensagem(self, tipo, dados):
        return dict(tipo=tipo, dados=dados, **self.metadados())

    def registrar_log(self, direcao, mensagem):
        print(f'[{self.nome}] {direcao} {mensagem}', flush=True)

    def consultar(self, endpoint, tipo, dados):
        with zmq.Context.instance().socket(zmq.REQ) as socket:
            socket.setsockopt(zmq.LINGER, 0)
            socket.setsockopt(zmq.RCVTIMEO, 800)
            socket.setsockopt(zmq.SNDTIMEO, 800)
            socket.connect(endpoint)
            pedido = self.mensagem(tipo, dados)
            self.registrar_log('ENVIO', pedido)
            socket.send(msgpack.packb(pedido, use_bin_type=True))
            resposta = msgpack.unpackb(socket.recv(), raw=False)
            self.receber(resposta.get('relogio_logico', 0))
            self.registrar_log('RECEBIMENTO', resposta)
            return resposta['dados']

    def contar(self):
        self.contagem += 1
        if self.contagem % 10 == 0:
            self.heartbeat_pendente.set()
    def manter(self):
        ultimo_heartbeat = 0.0
        while True:
            try:
                agora = time.monotonic()
                if not self.rank:
                    self.rank = self.consultar(self.referencia, 'registrar_servidor',
                        {'nome': self.nome})['rank']
                if self.heartbeat_pendente.is_set() or agora - ultimo_heartbeat > 5:
                    self.heartbeat_pendente.clear()
                    inicio = time.time() * 1000
                    resposta = self.consultar(self.referencia, 'heartbeat', {'nome': self.nome, 'rank': self.rank})
                    if resposta.get('registrar'):
                        self.rank = 0
                    if 'hora' in resposta:
                        self.ajustar(resposta['hora'] + (time.time()*1000-inicio)/2 - self.horario())
                    ultimo_heartbeat = agora
            except Exception as erro:
                print(f'[{self.nome}] Manutencao: {erro}', flush=True)
            time.sleep(0.1)

    def iniciar(self):
        threading.Thread(target=self.manter, daemon=True).start()
