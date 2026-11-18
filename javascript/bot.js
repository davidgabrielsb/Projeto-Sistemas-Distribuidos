const zmq = require("zeromq");
const msgpack = require("@msgpack/msgpack");

const BOT_ID = process.env.BOT_ID || "javascript-1";
const USUARIO = `bot-${BOT_ID}`;
const CANAL = `canal-${BOT_ID}`;

const SERVIDOR =
    process.env.SERVER_ENDPOINT || "tcp://localhost:5555";

function criarMensagem(tipo, dados) {
    return {
        tipo,
        dados,
        timestamp: new Date().toISOString()
    };
}

async function enviarRequisicao(tipo, dados) {
    const pedido = new zmq.Request({ linger: 0, receiveTimeout: 3000, sendTimeout: 3000 });
    try {
        pedido.connect(SERVIDOR);
        const requisicao = criarMensagem(tipo, dados);
        console.log('ENVIO', requisicao);
        await pedido.send(msgpack.encode(requisicao));
        const [bytes] = await pedido.receive();
        const resposta = msgpack.decode(bytes);
        console.log('RECEBIMENTO', resposta);
        return resposta;
    } catch (erro) {
        return { status: 'erro', mensagem: erro.message, dados: {} };
    } finally { pedido.close(); }
}

function aguardar(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function realizarLogin() {

    while (true) {

        try {

            const resposta = await enviarRequisicao(
                "login",
                { usuario: USUARIO }
            );

            if (resposta.status === "sucesso") {

                console.log("Login realizado com sucesso!");
                return;
            }

            console.log("Falha no login. Tentando novamente...");

        } catch (erro) {

            console.log("Servidor indisponivel. Tentando novamente...");
        }

        await aguardar(1000);
    }
}

async function listarCanais() {

    const resposta = await enviarRequisicao(
        "listar_canais",
        {}
    );

    if (resposta.status !== "sucesso") {
        throw new Error("Nao foi possivel listar os canais.");
    }

    return resposta.dados.canais || [];
}

async function iniciarBot() {
    await realizarLogin();
    let rodada = 0;
    while (true) {
        let canais;
        try { canais = await listarCanais(); }
        catch (erro) { await aguardar(1000); continue; }
        if (canais.length < 5) {
            await enviarRequisicao('criar_canal', { nome: `${CANAL}-${rodada++}`, usuario: USUARIO });
        }
        await aguardar(3000);
    }
}

iniciarBot().catch(erro => { console.error(erro); process.exit(1); });
