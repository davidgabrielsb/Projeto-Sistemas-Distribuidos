# Parte 5 — Consistência e replicação

Os servidores trocam lotes de eventos para recuperar dados e convergir. A pasta reúne o código acumulado até esta etapa e roda por conta própria.

Com o Docker Desktop aberto, execute `docker compose up --build` nesta pasta. O Compose inicia dois servidores e dois bots por linguagem. Os bots começam sozinhos. Para parar, pressione Ctrl+C e execute `docker compose down`.

## Método de replicação

Usamos replicação por pull com consistência eventual. Cada servidor grava e confirma a operação no SQLite local. Em segundo plano, os servidores pedem uns aos outros lotes de eventos, aplicam os eventos ainda desconhecidos e ficam disponíveis para retransmiti-los a outros pares. Assim, um servidor que volta do ar ou começa vazio recupera o histórico disponível nas outras bases.

Cada evento tem um ID único. Quando duas réplicas criam o mesmo canal ao mesmo tempo, ambas conservam os eventos, e a tabela de canais escolhe os metadados do menor ID. A base histórica converge quando os servidores conseguem trocar os eventos pendentes. O ACK ao bot confirma a gravação local.
