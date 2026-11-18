const zmq = require('zeromq');
const msgpack = require('@msgpack/msgpack');
class Referencia {
    constructor(relogio) {
        this.relogio = relogio;
        this.nome = process.env.SERVER_NAME || 'javascript-server';
        this.referencia = process.env.REFERENCE_ENDPOINT || 'tcp://reference:5560';
        this.rank = 0;
        this.contagem = 0;
        this.heartbeatPendente = false;
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

    contar() {
        this.contagem++;
        if (this.contagem % 10 === 0) this.heartbeatPendente = true;
    }

    async manter() {
        let ultimoHeartbeat = 0;
        while (true) {
            try {
                const agora = performance.now();
                if (!this.rank) {
                    const registro = await this.consultar(this.referencia, 'registrar_servidor', { nome: this.nome });
                    this.rank = registro.rank;
                }
                if (this.heartbeatPendente || agora - ultimoHeartbeat > 5000) {
                    this.heartbeatPendente = false;
                    const inicio = Date.now();
                    const resposta = await this.consultar(this.referencia, 'heartbeat', { nome: this.nome, rank: this.rank });
                    if (resposta.registrar) this.rank = 0;
                    if (resposta.hora) this.relogio.ajustar(resposta.hora + (Date.now() - inicio)/2 - this.relogio.hora());
                    ultimoHeartbeat = agora;
                }
            } catch (erro) { this.log('MANUTENCAO', erro.message); }
            await new Promise(resolve => setTimeout(resolve, 100));
        }
    }

    iniciar() {
        this.manter().catch(console.error);
    }
}

module.exports = Referencia;
