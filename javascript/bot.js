const zmq = require("zeromq");
const msgpack = require("@msgpack/msgpack");
const relogio = require("./relogio");

const BOT_ID = process.env.BOT_ID || "javascript-1";
const USUARIO = `bot-${BOT_ID}`;
const CANAL = `canal-${BOT_ID}`;

const SERVIDOR =
    process.env.SERVER_ENDPOINT || "tcp://localhost:5555";

const SUB_ENDPOINT =
    process.env.SUB_ENDPOINT || "tcp://localhost:5558";

function criarMensagem(tipo, dados) {
    return {
        tipo,
        dados,
        ...relogio.metadados()
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
        relogio.receber(resposta.relogio_logico);
        console.log('RECEBIMENTO', resposta);
        return resposta;
    } catch (erro) {
        return { status: 'erro', mensagem: erro.message, dados: {} };
    } finally { pedido.close(); }
}

function aguardar(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function escolherAleatorio(lista) {
    return lista[Math.floor(Math.random() * lista.length)];
}

function gerarMensagemAleatoria() {

    const mensagens = [
        "Ola, pessoal!",
        "Como estao todos?",
        "Mensagem enviada pelo bot!",
        "Testando comunicacao PUB-SUB.",
        "Sistemas distribuidos funcionando!",
        "Nova mensagem no canal!",
        "Teste de publicacao realizado.",
        "Mensagem automatica recebida?",
        "Comunicacao entre bots!",
        "Pub/Sub em funcionamento!"
    ];

    return escolherAleatorio(mensagens);
}

async function receberMensagens(subscriber) {

    try {

        for await (const [topico, mensagemBytes] of subscriber) {

            const publicacao = msgpack.decode(mensagemBytes);
            relogio.receber(publicacao.relogio_logico);
            console.log("PUBLICACAO COMPLETA", publicacao);

            const timestampRecebimento = new Date().toISOString();

            console.log("\n========== MENSAGEM RECEBIDA ==========");
            console.log("Canal:", topico.toString());
            console.log("Usuario:", publicacao.usuario);
            console.log("Mensagem:", publicacao.mensagem);
            console.log("Horario de envio:", publicacao.timestamp_envio);
            console.log("Horario de recebimento:", timestampRecebimento);
            console.log("=======================================");
        }

    } catch (erro) {

        console.error(
            "Erro ao receber mensagem PUB-SUB:",
            erro.message
        );
    }
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
    const subscriber = new zmq.Subscriber();
    subscriber.connect(SUB_ENDPOINT);
    const inscritos = new Set();
    await realizarLogin();
    receberMensagens(subscriber);
    let rodada = 0;
    while (true) {
        let canais;
        try { canais = await listarCanais(); }
        catch (erro) { await aguardar(1000); continue; }
        if (canais.length < 5) {
            await enviarRequisicao('criar_canal', { nome: `${CANAL}-${rodada++}`, usuario: USUARIO });
        }
        const disponiveis = canais.filter(c => !inscritos.has(c));
        while (inscritos.size < 3 && disponiveis.length) {
            const canal = escolherAleatorio(disponiveis);
            disponiveis.splice(disponiveis.indexOf(canal), 1);
            subscriber.subscribe(canal + '\0');
            inscritos.add(canal);
            console.log('Inscrito em', canal);
        }
        if (!canais.length) { await aguardar(1000); continue; }
        const canal = escolherAleatorio(canais);
        for (let numero = 1; numero <= 10; numero++) {
            await enviarRequisicao('publicar_mensagem', { canal, usuario: USUARIO, mensagem: gerarMensagemAleatoria() });
            await aguardar(1000);
        }
    }
}

iniciarBot().catch(erro => { console.error(erro); process.exit(1); });
