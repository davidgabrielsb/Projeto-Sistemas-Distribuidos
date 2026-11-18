import os
import sqlite3
import time
from datetime import datetime, timezone
import msgpack
import zmq

ENDERECO = os.getenv('REFERENCE_BIND', 'tcp://*:5560')
TIMEOUT_HEARTBEAT = float(os.getenv('HEARTBEAT_TTL', '30'))
servidores = {}
relogio_logico = 0

def obter_horario():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

def remover_inativos():
    agora = time.monotonic()
    for nome in list(servidores):
        if agora - servidores[nome]['ultimo_heartbeat'] > TIMEOUT_HEARTBEAT:
            print(f'Servidor removido por falta de heartbeat: {nome}', flush=True)
            del servidores[nome]

def processar(requisicao, db):
    tipo = requisicao.get('tipo')
    dados = requisicao.get('dados', {})
    if not isinstance(dados, dict):
        raise ValueError('Dados invalidos')
    if tipo == 'registrar_servidor':
        nome, endpoint = dados.get('nome'), dados.get('endpoint')
        if not isinstance(nome, str) or not nome.strip() or not isinstance(endpoint, str) or not endpoint.startswith('tcp://'):
            raise ValueError('Nome ou endereco invalido')
        db.execute('INSERT OR IGNORE INTO ranks(nome) VALUES(?)', (nome,))
        db.commit()
        rank = db.execute('SELECT rank FROM ranks WHERE nome=?', (nome,)).fetchone()[0]
        servidores[nome] = dict(nome=nome, rank=rank, endpoint=endpoint, ultimo_heartbeat=time.monotonic())
        return {'nome': nome, 'rank': rank}
    if tipo == 'listar_servidores':
        return {'servidores': [{k: v for k, v in servidor.items() if k != 'ultimo_heartbeat'} for servidor in servidores.values()]}
    if tipo == 'heartbeat':
        servidor = servidores.get(dados.get('nome'))
        if not servidor or servidor['rank'] != dados.get('rank'):
            return {'registrar': True}
        servidor['ultimo_heartbeat'] = time.monotonic()
        return {'ativo': True}
    raise ValueError('Operacao desconhecida')

def iniciar():
    global relogio_logico
    with sqlite3.connect(os.getenv('REFERENCE_DB', 'reference.db')) as db, zmq.Context() as contexto:
        db.execute('CREATE TABLE IF NOT EXISTS ranks(rank INTEGER PRIMARY KEY AUTOINCREMENT,nome TEXT UNIQUE NOT NULL)')
        socket = contexto.socket(zmq.REP)
        socket.bind(ENDERECO)
        try:
            while True:
                remover_inativos()
                if not socket.poll(500):
                    continue
                try:
                    pedido = msgpack.unpackb(socket.recv(), raw=False)
                    if not isinstance(pedido, dict):
                        raise ValueError('Requisicao invalida')
                    relogio_logico = max(relogio_logico, int(pedido.get('relogio_logico', 0)))
                    print(f'RECEBIMENTO {pedido}', flush=True)
                    resposta = {'tipo': pedido.get('tipo'), 'status': 'sucesso', 'dados': processar(pedido, db)}
                except Exception as erro:
                    resposta = {'tipo': 'erro', 'status': 'erro', 'dados': {'erro': str(erro)}}
                relogio_logico += 1
                resposta.update(timestamp=obter_horario(), relogio_logico=relogio_logico)
                print(f'ENVIO {resposta}', flush=True)
                socket.send(msgpack.packb(resposta, use_bin_type=True))
        finally:
            socket.close(0)

if __name__ == '__main__':
    iniciar()
