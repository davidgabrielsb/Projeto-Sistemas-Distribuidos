const zmq = require('zeromq');
const msgpack = require('@msgpack/msgpack');
class Cluster {
    constructor(relogio) {
        this.relogio = relogio;
        this.nome = process.env.SERVER_NAME || 'javascript-server';
        this.endpoint = process.env.PEER_ENDPOINT || `tcp://${this.nome}:5570`;
        this.referencia = process.env.REFERENCE_ENDPOINT || 'tcp://reference:5560';
        this.rank = 0;
        this.coordenador = '';
        this.contagem = 0;
        this.trocadas = 0;
        this.heartbeatPendente = false;
        this.sincronizacaoPendente = false;
    }

    mensagem(tipo, dados) {
        return { tipo, dados, ...this.relogio.metadados() };
    }

    log(direcao, mensagem) {
        console.log(`[${this.nome}] ${direcao} ${JSON.stringify(mensagem)}`);
    }

    async consultar(endpoint, tipo, dados) {
        const socket = new zmq.Request({ linger: 0, receiveTimeout: 800, sendTimeout: 800 });
        try {
            socket.connect(endpoint);
            const pedido = this.mensagem(tipo, dados);
            this.log('ENVIO', pedido);
            await socket.send(msgpack.encode(pedido));
            const [bytes] = await socket.receive();
            const resposta = msgpack.decode(bytes);
            this.relogio.receber(resposta.relogio_logico);
            this.log('RECEBIMENTO', resposta);
            return resposta.dados;
        } finally {
            socket.close();
        }
    }

    atender(pedido) {
        const { tipo, dados } = pedido;
        if (tipo === 'eleicao' || tipo === 'hora') {
            return { nome: this.nome, rank: this.rank, hora: this.relogio.hora(), coordenador: this.coordenador };
        }
        if (tipo === 'ajustar') {
            this.relogio.ajustar(Number(dados.delta));
            return { ajustado: true };
        }
        if (tipo === 'sincronizar') {
            this.sincronizacaoPendente = true;
            return { agendado: true };
        }
        throw new Error('Operacao de servidor desconhecida');
    }

    async servir() {
        const socket = new zmq.Reply();
        await socket.bind(process.env.PEER_BIND || 'tcp://*:5570');
        for await (const [bytes] of socket) {
            let resposta;
            try {
                const pedido = msgpack.decode(bytes);
                this.relogio.receber(pedido.relogio_logico);
                this.log('RECEBIMENTO', pedido);
                resposta = this.mensagem('resposta', this.atender(pedido));
            } catch (erro) {
                resposta = this.mensagem('erro', { erro: erro.message });
            }
            this.log('ENVIO', resposta);
            await socket.send(msgpack.encode(resposta));
        }
    }

    async observar() {
        const socket = new zmq.Subscriber();
        socket.connect(process.env.SUB_ENDPOINT || 'tcp://pubsub-proxy:5558');
        socket.subscribe('servers\0');
        for await (const [, bytes] of socket) {
            const anuncio = msgpack.decode(bytes);
            this.relogio.receber(anuncio.relogio_logico);
            this.coordenador = anuncio.dados.nome;
            this.log('COORDENADOR', anuncio);
        }
    }

    contar() {
        this.contagem++;
        if (this.contagem % 10 === 0) this.heartbeatPendente = true;
        this.contarTroca();
    }

    contarTroca() {
        this.trocadas++;
        if (this.trocadas % 15 === 0) this.sincronizacaoPendente = true;
    }

    async berkeley(vivos) {
        const diferencas = [[this.endpoint, 0]];
        for (const servidor of vivos) {
            if (servidor.nome === this.nome) continue;
            try {
                const inicio = this.relogio.hora();
                const remoto = await this.consultar(servidor.endpoint, 'hora', {});
                diferencas.push([servidor.endpoint, remoto.hora - (inicio + this.relogio.hora()) / 2]);
            } catch (erro) { this.log('INDISPONIVEL', servidor.nome); }
        }
        const media = diferencas.reduce((soma, [, delta]) => soma + delta, 0) / diferencas.length;
        for (const [endpoint, delta] of diferencas) {
            if (endpoint === this.endpoint) this.relogio.ajustar(media);
            else {
                try { await this.consultar(endpoint, 'ajustar', { delta: media - delta }); }
                catch (erro) { this.log('INDISPONIVEL', endpoint); }
            }
        }
        this.log('BERKELEY', { participantes: diferencas.length, media });
    }

    async manter() {
        const publicador = new zmq.Publisher();
        publicador.connect(process.env.PUB_ENDPOINT || 'tcp://pubsub-proxy:5557');
        let ultimoHeartbeat = 0, ultimaEleicao = 0, vivos = [];
        while (true) {
            try {
                const agora = performance.now();
                if (!this.rank) {
                    const registro = await this.consultar(this.referencia, 'registrar_servidor', { nome: this.nome, endpoint: this.endpoint });
                    this.rank = registro.rank;
                }
                if (this.heartbeatPendente || agora - ultimoHeartbeat > 5000) {
                    this.heartbeatPendente = false;
                    const resposta = await this.consultar(this.referencia, 'heartbeat', { nome: this.nome, rank: this.rank });
                    if (resposta.registrar) this.rank = 0;
                    ultimoHeartbeat = agora;
                }
                if (agora - ultimaEleicao > 3000) {
                    const lista = await this.consultar(this.referencia, 'listar_servidores', {});
                    vivos = [];
                    for (const servidor of lista.servidores) {
                        try {
                            const info = await this.consultar(servidor.endpoint, 'eleicao', { candidato: this.nome });
                            if (info.rank > 0) vivos.push({ ...servidor, rank: info.rank });
                        } catch (erro) { this.log('INDISPONIVEL', servidor.nome); }
                    }
                    vivos.sort((a, b) => b.rank - a.rank || b.nome.localeCompare(a.nome));
                    if (vivos.length) {
                        const eleito = vivos[0].nome;
                        const mudou = eleito !== this.coordenador;
                        this.coordenador = eleito;
                        if (eleito === this.nome) {
                            const anuncio = this.mensagem('coordenador', { nome: eleito, rank: this.rank });
                            await publicador.send([Buffer.from('servers\0'), msgpack.encode(anuncio)]);
                            this.log('PUBLICACAO', anuncio);
                            if (mudou) this.sincronizacaoPendente = true;
                        }
                    }
                    ultimaEleicao = agora;
                }
                if (this.sincronizacaoPendente) {
                    this.sincronizacaoPendente = false;
                    if (this.coordenador === this.nome) await this.berkeley(vivos);
                    else {
                        const lider = vivos.find(s => s.nome === this.coordenador);
                        if (lider) await this.consultar(lider.endpoint, 'sincronizar', {});
                    }
                }
            } catch (erro) { this.log('MANUTENCAO', erro.message); }
            await new Promise(resolve => setTimeout(resolve, 100));
        }
    }

    iniciar() {
        this.manter().catch(console.error);
        this.servir().catch(erro => {
            console.error(erro);
            process.exit(1);
        });
        this.observar().catch(console.error);
    }
}

module.exports = Cluster;
