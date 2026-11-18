import argparse,json,os,sqlite3,subprocess,sys,time,uuid,shutil
from pathlib import Path
import msgpack,zmq

parser=argparse.ArgumentParser()
parser.add_argument('--base',default='.')
parser.add_argument('--java',default=shutil.which('java'))
parser.add_argument('--node',default=shutil.which('node'))
parser.add_argument('--parte',type=int,default=1)
parser.add_argument('--saida',default='resultados-testes')
args=parser.parse_args()
base=Path(args.base).resolve()
saida=Path(args.saida).resolve()/f'parte-{args.parte}-{uuid.uuid4().hex[:6]}'
saida.mkdir(parents=True)
contexto=zmq.Context()
processos={}
arquivos=[]
servidores=[]
resultado=[]
ambiente=dict(os.environ,PYTHONUNBUFFERED='1',PYTHONIOENCODING='utf-8')
def registrar(texto):
 print(texto,flush=True);resultado.append(texto)
def iniciar(nome,cmd,pasta,extra={}):
 arquivo=open(saida/(nome+'.log'),'a',encoding='utf-8');arquivos.append(arquivo)
 processo=subprocess.Popen(cmd,cwd=pasta,env=dict(ambiente,**extra),stdout=arquivo,stderr=subprocess.STDOUT)
 processos[nome]=processo
 return processo

def rpc(porta,tipo,dados,timeout=3000):
 with contexto.socket(zmq.REQ) as socket:
  socket.linger=0;socket.rcvtimeo=timeout;socket.sndtimeo=timeout
  socket.connect(f'tcp://127.0.0.1:{porta}')
  pedido=dict(tipo=tipo,dados=dados,timestamp='2026-11-18T12:00:00.000Z',relogio_logico=100)
  socket.send(msgpack.packb(pedido,use_bin_type=True))
  resposta=msgpack.unpackb(socket.recv(),raw=False)
  assert resposta['timestamp']
  if args.parte >= 3: assert resposta['relogio_logico'] >= 100
  return resposta

def esperar(teste,descricao,segundos=40):
 fim=time.monotonic()+segundos
 while time.monotonic()<fim:
  try:
   valor=teste()
   if valor:return valor
  except (zmq.Again,sqlite3.OperationalError,KeyError):pass
  time.sleep(.4)
 raise AssertionError(descricao)

def comando(linguagem,bot=False):
 if linguagem=='python':return [sys.executable,'bot.py' if bot else 'servidor.py']
 if linguagem=='javascript':return [args.node,'bot.js' if bot else 'servidor.js']
 return [args.java,'-cp',f'{base}/java/target/classes{os.pathsep}{base}/java/target/dependency/*','com.sistemasdistribuidos.cliente.Bot' if bot else 'com.sistemasdistribuidos.servidor.Servidor']

def iniciar_servidor(s):
 extra=dict(SERVER_ENDPOINT='tcp://127.0.0.1:5556',SERVER_NAME=s['nome'],SERVER_BIND=f"tcp://*:{s['porta']}",PEER_BIND=f"tcp://*:{s['peer']}",PEER_ENDPOINT=f"tcp://127.0.0.1:{s['peer']}",REFERENCE_ENDPOINT='tcp://127.0.0.1:5560',PUB_ENDPOINT='tcp://127.0.0.1:5557',SUB_ENDPOINT='tcp://127.0.0.1:5558',DB_PATH=str(saida/(s['nome']+'.db')))
 iniciar(s['nome'],comando(s['linguagem']),base/s['linguagem'],extra)
 esperar(lambda:rpc(s['porta'],'listar_canais',{}).get('status')=='sucesso','Servidor nao iniciou '+s['nome'])

def estado(s):
 with sqlite3.connect(saida/(s['nome']+'.db')) as db:
  return {t:db.execute(f'SELECT {cols} FROM {t} ORDER BY evento_id').fetchall() for t,cols in [('logins','evento_id,usuario,timestamp'),('canais','evento_id,nome,usuario,timestamp'),('mensagens','evento_id,canal,usuario,mensagem,timestamp_envio,timestamp_publicacao')]}

def convergiu(lista):
 estados=[estado(s) for s in lista]
 return estados[0] if all(e==estados[0] for e in estados) else False

def lideres(lista):
 return [rpc(s['peer'],'hora',{})['dados']['coordenador'] for s in lista]

try:
 iniciar('broker',[sys.executable,'broker.py'],base/'broker')
 if args.parte>=2:iniciar('proxy',[sys.executable,'proxy.py'],base/'pubsub')
 if args.parte>=3:iniciar('reference',[sys.executable,'reference.py'],base/'reference',{'REFERENCE_DB':str(saida/'reference.db')})
 for linguagem in ['python','java','javascript']:
  for numero in range(1,3):
   i=len(servidores)
   s=dict(nome=f'{linguagem}-server-{numero}',linguagem=linguagem,porta=16570+i,peer=16580+i)
   servidores.append(s);iniciar_servidor(s)
 for s in servidores:
  assert rpc(s['porta'],'login',{'usuario':s['nome']})['status']=='sucesso'
  assert rpc(s['porta'],'criar_canal',{'nome':'canal-'+s['nome'],'usuario':s['nome']})['status']=='sucesso'
  assert rpc(s['porta'],'login',{'usuario':''})['status']=='erro'
  if args.parte>=2: assert rpc(s['porta'],'publicar_mensagem',{'canal':'geral','usuario':s['nome'],'mensagem':'Ola, acentuacao: a\u00e7\u00e3o!'})['status']=='sucesso'
 registrar('PASSOU: 6 servidores nas 3 linguagens; login, canais, validacao e publicacao conforme a etapa.')
 if args.parte>=3:
  esperar(lambda:len(rpc(5560,'listar_servidores',{})['dados']['servidores'])==6,'Registro dos seis servidores')
  for s in servidores:
   for i in range(30):rpc(s['porta'],'listar_canais',{})
  registrar('PASSOU: registro, MessagePack e relogios logicos em todas as respostas.')
 if args.parte>=2:
  sub=contexto.socket(zmq.SUB);sub.linger=0;sub.connect('tcp://127.0.0.1:5558');sub.subscribe(b'geral\0')
  time.sleep(1)
  for s in servidores:rpc(s['porta'],'publicar_mensagem',{'canal':'geral','usuario':s['nome'],'mensagem':'teste de assinatura'})
  recebidas=[]
  fim=time.monotonic()+6
  while len(recebidas)<6 and time.monotonic()<fim:
   if sub.poll(300):
    topico,raw=sub.recv_multipart();m=msgpack.unpackb(raw,raw=False);assert m['timestamp'];recebidas.append(m)
    if args.parte >= 3: assert m['relogio_logico'] > 0
  sub.close();assert len(recebidas)==6,len(recebidas)
  registrar('PASSOU: PUB/SUB binario com publicacoes dos seis servidores.')
 if args.parte>=4:
  lider=esperar(lambda:lideres(servidores)[0] if len(set(lideres(servidores)))==1 and lideres(servidores)[0] else False,'Eleicao inicial')
  registrar('PASSOU: coordenador inicial comum = '+lider)
  rpc(servidores[0]['peer'],'ajustar',{'delta':120000})
  coordenador=next(s for s in servidores if s['nome']==lider)
  rpc(coordenador['peer'],'sincronizar',{})
  def sincronizado():
   horas=[rpc(s['peer'],'hora',{})['dados']['hora'] for s in servidores]
   return max(horas)-min(horas)<2000
  esperar(sincronizado,'Berkeley nao convergiu')
  registrar('PASSOU: Berkeley corrigiu desvio artificial de 120 segundos (dispersao final < 2 s).')
 if args.parte>=5:
  estado_final=esperar(lambda:convergiu(servidores),'Replicacao inicial')
  registrar('PASSOU: replicas identicas: '+str({k:len(v) for k,v in estado_final.items()}))
  for s in servidores:
   h=rpc(s['porta'],'historico',{'canal':'geral','limite':100})
   assert len(h['dados']['mensagens'])==12
  registrar('PASSOU: historico completo consultavel em todas as linguagens.')
 if args.parte>=4:
  processos[lider].terminate();processos[lider].wait()
  vivos=[s for s in servidores if s['nome']!=lider]
  novo=esperar(lambda:lideres(vivos)[0] if len(set(lideres(vivos)))==1 and lideres(vivos)[0] not in ('',lider) else False,'Reeleicao apos falha',50)
  registrar('PASSOU: falha de '+lider+'; novo coordenador = '+novo)
  for s in vivos:
   if args.parte>=5:rpc(s['porta'],'publicar_mensagem',{'canal':'geral','usuario':s['nome'],'mensagem':'durante a falha'})
  iniciar_servidor(coordenador)
  if args.parte>=5:
   estado_final=esperar(lambda:convergiu(servidores),'Recuperacao apos reinicio',60)
   assert len(estado_final['mensagens'])==17
   registrar('PASSOU: servidor reiniciado recuperou todas as 17 mensagens sem duplicar eventos.')
  esperar(lambda:len(set(lideres(servidores)))==1,'Eleicao apos retorno')
 if args.parte>=4:
  ativos=servidores.copy()
  retirados=[]
  for linguagem in ['java','python']:
   for s in list(ativos):
    if s['linguagem']=='javascript' or (linguagem=='python' and s['linguagem']=='java'):
     processos[s['nome']].terminate();processos[s['nome']].wait();ativos.remove(s);retirados.append(s)
   eleito=esperar(lambda:lideres(ativos)[0] if len(set(lideres(ativos)))==1 and lideres(ativos)[0].startswith(linguagem+'-') else False,'Coordenador '+linguagem,50)
   lider_atual=next(s for s in ativos if s['nome']==eleito)
   rpc(ativos[0]['peer'],'ajustar',{'delta':60000})
   rpc(lider_atual['peer'],'sincronizar',{})
   def relogios_ativos():
    horas=[rpc(s['peer'],'hora',{})['dados']['hora'] for s in ativos]
    return max(horas)-min(horas)<2000
   esperar(relogios_ativos,'Berkeley coordenado por '+linguagem,50)
   registrar('PASSOU: eleicao e Berkeley coordenados por '+linguagem+'.')
  for s in retirados:iniciar_servidor(s)
  esperar(lambda:len(set(lideres(servidores)))==1,'Reintegracao de todos os servidores',50)
 if args.parte>=5:
  novo_servidor=dict(nome='java-server-novo',linguagem='java',porta=16576,peer=16586)
  iniciar_servidor(novo_servidor)
  estado_final=esperar(lambda:convergiu(servidores+[novo_servidor]),'Entrada de servidor vazio',60)
  registrar('PASSOU: novo servidor Java entrou com banco vazio e recuperou todo o historico.')
  processos[novo_servidor['nome']].terminate();processos[novo_servidor['nome']].wait()
 for i,s in enumerate(servidores):
  iniciar('bot-'+s['nome'],comando(s['linguagem'],True),base/s['linguagem'],dict(BOT_ID='teste-'+str(i),SERVER_ENDPOINT='tcp://127.0.0.1:5555',SUB_ENDPOINT='tcp://127.0.0.1:5558'))
 time.sleep(14)
 for nome,p in list(processos.items()):
  if nome.startswith('bot-'):
   assert p.poll() is None,'Bot encerrou '+nome
   p.terminate();p.wait()
   conteudo=(saida/(nome+'.log')).read_text(encoding='utf-8')
   assert 'sucesso' in conteudo
   if args.parte>=2:assert 'PUB-SUB' in conteudo or 'MENSAGEM RECEBIDA' in conteudo,'Bot nao recebeu '+nome
 registrar('PASSOU: 6 bots ativos pelo broker' + (' e recebendo publicacoes.' if args.parte >= 2 else '.'))
 if args.parte>=5:
  estado_final=esperar(lambda:convergiu(servidores),'Convergencia apos parar bots',60)
  registrar('PASSOU: convergencia final '+str({k:len(v) for k,v in estado_final.items()}))
 registrar('TODOS OS TESTES PASSARAM')
finally:
 for p in processos.values():
  if p.poll() is None:p.terminate()
 for p in processos.values():p.wait()
 for f in arquivos:f.close()
 contexto.term()
 (saida/'resultado.txt').write_text('\n'.join(resultado),encoding='utf-8')
 print('Logs:',saida,flush=True)

