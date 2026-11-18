# Projeto de Sistemas Distribuídos — Parte 2

Nesta etapa, foi implementada a comunicação por publicação e assinatura (Pub/Sub).

Os servidores armazenam as mensagens no SQLite e publicam as mensagens nos canais. Os bots podem se inscrever nos canais e receber as mensagens publicadas.

O proxy Pub/Sub utiliza ZeroMQ, com XSUB na porta 5557 e XPUB na porta 5558.

## Execução

Com o Docker Desktop aberto, execute:

docker compose up --build

Para encerrar:

docker compose down