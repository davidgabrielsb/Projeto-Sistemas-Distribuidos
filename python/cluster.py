import json
import os
import sqlite3
import threading
import time
from pathlib import Path

import msgpack
import zmq

class Cluster:
    def __init__(self, banco, metadados, receber, ajustar, horario):
        self.banco = banco
        self.metadados = metadados
        self.receber = receber
        self.ajustar = ajustar
        self.horario = horario
        self.nome = os.getenv('SERVER_NAME', 'python-server')
        self.endpoint = os.getenv('PEER_ENDPOINT', f'tcp://{self.nome}:5570')
        self.referencia = os.getenv('REFERENCE_ENDPOINT', 'tcp://reference:5560')
        self.rank = 0
        self.coordenador = ''
        self.contagem = 0
        self.trocadas = 0
        self.heartbeat_pendente = threading.Event()
        self.sincronizacao_pendente = threading.Event()
        self.cursores = {}
        with self.conectar() as db:
            db.executescript(Path(__file__).with_name('replicacao.sql').read_text())

    def conectar(self):
        db = sqlite3.connect(self.banco, timeout=10)
        db.row_factory = sqlite3.Row
        return db

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

    def atender(self, pedido):
        tipo, dados = pedido['tipo'], pedido['dados']
        if tipo in ('eleicao', 'hora'):
            return dict(nome=self.nome, rank=self.rank, hora=self.horario(), coordenador=self.coordenador)
        if tipo == 'ajustar':
            self.ajustar(float(dados['delta']))
            return {'ajustado': True}
        if tipo == 'sincronizar':
            self.sincronizacao_pendente.set()
            return {'agendado': True}
        if tipo == 'eventos':
            with self.conectar() as db:
                eventos = [dict(row) for row in db.execute(
                    'SELECT * FROM eventos WHERE seq > ? ORDER BY seq LIMIT 100',
                    (int(dados.get('apos', 0)),))]
            return {'eventos': eventos}
        raise ValueError('Operacao de servidor desconhecida')

    def importar(self, eventos):
        with self.conectar() as db:
            for evento in eventos:
                tabela = evento['tabela']
                campos = CAMPOS[tabela]
                valores = json.loads(evento['dados'])
                novo = db.execute('INSERT OR IGNORE INTO eventos(id,tabela,dados) VALUES(?,?,?)',
                                  (evento['id'], tabela, evento['dados'])).rowcount
                if not novo:
                    continue
                nomes = campos + ['evento_id']
                sql = f"INSERT INTO {tabela} ({','.join(nomes)}) VALUES ({','.join('?' for _ in nomes)})"
                if tabela == 'canais':
                    sql += (' ON CONFLICT(nome) DO UPDATE SET usuario=excluded.usuario, '
                            'timestamp=excluded.timestamp, evento_id=excluded.evento_id '
                            'WHERE excluded.evento_id < canais.evento_id')
                db.execute(sql, [valores[c] for c in campos] + [evento['id']])

    def servir(self):
        with zmq.Context.instance().socket(zmq.REP) as socket:
            socket.bind(os.getenv('PEER_BIND', 'tcp://*:5570'))
            while True:
                try:
                    pedido = msgpack.unpackb(socket.recv(), raw=False)
                    self.receber(pedido.get('relogio_logico', 0))
                    self.registrar_log('RECEBIMENTO', pedido)
                    resposta = self.mensagem('resposta', self.atender(pedido))
                except Exception as erro:
                    resposta = self.mensagem('erro', {'erro': str(erro)})
                self.registrar_log('ENVIO', resposta)
                socket.send(msgpack.packb(resposta, use_bin_type=True))

    def observar(self):
        with zmq.Context.instance().socket(zmq.SUB) as socket:
            socket.connect(os.getenv('SUB_ENDPOINT', 'tcp://pubsub-proxy:5558'))
            socket.subscribe(b'servers\0')
            while True:
                _, dados = socket.recv_multipart()
                anuncio = msgpack.unpackb(dados, raw=False)
                self.receber(anuncio.get('relogio_logico', 0))
                self.coordenador = anuncio['dados']['nome']
                self.registrar_log('COORDENADOR', anuncio)

    def contar(self):
        self.contagem += 1
        if self.contagem % 10 == 0:
            self.heartbeat_pendente.set()
        self.contar_troca()

    def contar_troca(self):
        self.trocadas += 1
        if self.trocadas % 15 == 0:
            self.sincronizacao_pendente.set()

    def berkeley(self, vivos):
        diferencas = [(self.endpoint, 0.0)]
        for servidor in vivos:
            if servidor['nome'] == self.nome:
                continue
            try:
                inicio = self.horario()
                remoto = self.consultar(servidor['endpoint'], 'hora', {})['hora']
                diferencas.append((servidor['endpoint'], remoto - (inicio + self.horario()) / 2))
            except (zmq.ZMQError, KeyError):
                continue
        media = sum(delta for _, delta in diferencas) / len(diferencas)
        for endpoint, delta in diferencas:
            if endpoint == self.endpoint:
                self.ajustar(media)
            else:
                try:
                    self.consultar(endpoint, 'ajustar', {'delta': media - delta})
                except zmq.ZMQError:
                    pass
        print(f'[{self.nome}] Berkeley: {len(diferencas)} relogios, ajuste medio {media:.3f} ms', flush=True)

    def manter(self):
        with zmq.Context.instance().socket(zmq.PUB) as publicador:
            publicador.connect(os.getenv('PUB_ENDPOINT', 'tcp://pubsub-proxy:5557'))
            ultimo_heartbeat = ultima_eleicao = ultima_replica = 0.0
            vivos = []
            while True:
                try:
                    agora = time.monotonic()
                    if not self.rank:
                        self.rank = self.consultar(self.referencia, 'registrar_servidor',
                            {'nome': self.nome, 'endpoint': self.endpoint})['rank']
                    if self.heartbeat_pendente.is_set() or agora - ultimo_heartbeat > 5:
                        self.heartbeat_pendente.clear()
                        resposta = self.consultar(self.referencia, 'heartbeat', {'nome': self.nome, 'rank': self.rank})
                        if resposta.get('registrar'):
                            self.rank = 0
                        ultimo_heartbeat = agora
                    if agora - ultima_eleicao > 3:
                        servidores = self.consultar(self.referencia, 'listar_servidores', {})['servidores']
                        vivos = []
                        for servidor in servidores:
                            try:
                                info = self.consultar(servidor['endpoint'], 'eleicao', {'candidato': self.nome})
                                if info.get('rank', 0) > 0:
                                    vivos.append(dict(servidor, rank=info['rank']))
                            except zmq.ZMQError:
                                continue
                        if vivos:
                            eleito = max(vivos, key=lambda s: (s['rank'], s['nome']))['nome']
                            mudou = eleito != self.coordenador
                            self.coordenador = eleito
                            if eleito == self.nome:
                                anuncio = self.mensagem('coordenador', {'nome': eleito, 'rank': self.rank})
                                publicador.send_multipart([b'servers\0', msgpack.packb(anuncio, use_bin_type=True)])
                                self.registrar_log('PUBLICACAO', anuncio)
                                if mudou:
                                    self.sincronizacao_pendente.set()
                        ultima_eleicao = agora
                    if self.sincronizacao_pendente.is_set():
                        self.sincronizacao_pendente.clear()
                        if self.coordenador == self.nome:
                            self.berkeley(vivos)
                        else:
                            lider = next((s for s in vivos if s['nome'] == self.coordenador), None)
                            if lider:
                                self.consultar(lider['endpoint'], 'sincronizar', {})
                    if agora - ultima_replica > 2:
                        for servidor in vivos:
                            if servidor['nome'] == self.nome:
                                continue
                            endpoint = servidor['endpoint']
                            try:
                                eventos = self.consultar(endpoint, 'eventos', {'apos': self.cursores.get(endpoint, 0)})['eventos']
                                self.importar(eventos)
                                if eventos:
                                    self.cursores[endpoint] = eventos[-1]['seq']
                            except (zmq.ZMQError, KeyError):
                                continue
                        ultima_replica = agora
                except Exception as erro:
                    print(f'[{self.nome}] Manutencao: {erro}', flush=True)
                time.sleep(0.1)

    def iniciar(self):
        for tarefa in (self.servir, self.manter, self.observar):
            threading.Thread(target=tarefa, daemon=True).start()


CAMPOS = {
    'logins': ['usuario', 'timestamp'],
    'canais': ['nome', 'usuario', 'timestamp'],
    'mensagens': ['canal', 'usuario', 'mensagem', 'timestamp_envio', 'timestamp_publicacao'],
}
