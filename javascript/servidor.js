const zmq = require("zeromq");
const msgpack = require("@msgpack/msgpack");
const Database = require("better-sqlite3");

const SERVER_ENDPOINT = process.env.SERVER_ENDPOINT;
const PUB_ENDPOINT = process.env.PUB_ENDPOINT || "tcp://localhost:5557";

const ENDERECO = process.env.SERVER_BIND || "tcp://*:5555";
const BANCO = process.env.DB_PATH || "servidor.db";

const db = new Database(BANCO);

db.exec(`
    CREATE TABLE IF NOT EXISTS logins (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario TEXT NOT NULL,
        timestamp TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS canais (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL UNIQUE,
        usuario TEXT NOT NULL,
        timestamp TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS mensagens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        canal TEXT NOT NULL,
        usuario TEXT NOT NULL,
        mensagem TEXT NOT NULL,
        timestamp_envio TEXT NOT NULL,
        timestamp_publicacao TEXT NOT NULL
    );
`);

console.log("Banco de dados inicializado:", BANCO);

function obterTimestamp() {
    return new Date().toISOString();
}

function criarResposta(tipo, status, mensagem, dados = {}) {
    return {
        tipo,
        status,
        mensagem,
        dados,
        timestamp: obterTimestamp()
    };
}

async function processarMensagem(requisicao, publicador) {

    if (!requisicao || typeof requisicao !== "object") {
        return criarResposta(
            "desconhecido",
            "erro",
            "Mensagem invalida"
        );
    }

    const { tipo, dados, timestamp } = requisicao;

    if (typeof tipo !== "string") {
        return criarResposta(
            "desconhecido",
            "erro",
            "Tipo de mensagem invalido"
        );
    }

    if (typeof timestamp !== "string" || !timestamp.trim()) {
        return criarResposta(
            tipo,
            "erro",
            "Timestamp invalido"
        );
    }

    if (!dados || typeof dados !== "object" || Array.isArray(dados)) {
        return criarResposta(
            tipo,
            "erro",
            "Dados invalidos"
        );
    }

    if (tipo === 'historico') {
        const limite = Math.max(1, Math.min(100, Number(dados.limite || 50)));
        const inicio = Math.max(0, Number(dados.inicio || 0));
        const mensagens = db.prepare('SELECT canal,usuario,mensagem,timestamp_envio,timestamp_publicacao FROM mensagens WHERE canal=? ORDER BY timestamp_envio,id LIMIT ? OFFSET ?').all(dados.canal || '', limite, inicio);
        return criarResposta(tipo, 'sucesso', 'Historico do canal', { mensagens, inicio, limite });
    }

    if (tipo === "login") {

        const usuario = dados.usuario;

        if (typeof usuario !== "string" || !usuario.trim()) {
            return criarResposta(
                tipo,
                "erro",
                "Nome de usuario invalido"
            );
        }

        const comando = db.prepare(`
            INSERT INTO logins (usuario, timestamp)
            VALUES (?, ?)
        `);

        comando.run(usuario.trim(), timestamp);

        return criarResposta(
            tipo,
            "sucesso",
            "Login realizado com sucesso"
        );
    }

    if (tipo === "criar_canal") {

        const nome = dados.nome;
        const usuario = dados.usuario;

        if (
            typeof nome !== "string" ||
            typeof usuario !== "string"
        ) {
            return criarResposta(
                tipo,
                "erro",
                "Dados do canal invalidos"
            );
        }

        const nomeLimpo = nome.trim();
        const usuarioLimpo = usuario.trim();

        if (!nomeLimpo || !usuarioLimpo) {
            return criarResposta(
                tipo,
                "erro",
                "Nome do canal ou usuario invalido"
            );
        }

        try {

            const comando = db.prepare(`
                INSERT INTO canais (nome, usuario, timestamp)
                VALUES (?, ?, ?)
            `);

            comando.run(nomeLimpo, usuarioLimpo, timestamp);

            return criarResposta(
                tipo,
                "sucesso",
                "Canal criado com sucesso"
            );

        } catch (erro) {

            if (erro.code === "SQLITE_CONSTRAINT_UNIQUE") {
                return criarResposta(
                    tipo,
                    "erro",
                    "Esse canal ja existe"
                );
            }

            throw erro;
        }
    }

    if (tipo === "listar_canais") {

        const comando = db.prepare(`
            SELECT nome FROM canais ORDER BY nome
        `);

        const resultado = comando.all();
        const canais = resultado.map(canal => canal.nome);

        return criarResposta(
            tipo,
            "sucesso",
            "Lista de canais",
            { canais }
        );
    }

    if (tipo === "publicar_mensagem") {

        const canal = dados.canal;
        const usuario = dados.usuario;
        const mensagem = dados.mensagem;

        if (
            typeof canal !== "string" ||
            typeof usuario !== "string" ||
            typeof mensagem !== "string" ||
            !canal.trim() ||
            !usuario.trim() ||
            !mensagem.trim()
        ) {
            return criarResposta(
                tipo,
                "erro",
                "Dados da publicacao invalidos"
            );
        }

        const canalLimpo = canal.trim();
        const usuarioLimpo = usuario.trim();
        const timestampPublicacao = obterTimestamp();

        const publicacao = {
            canal: canalLimpo,
            usuario: usuarioLimpo,
            mensagem,
            timestamp_envio: timestamp,
            timestamp_publicacao: timestampPublicacao,
            timestamp: obterTimestamp()
        };

        const comando = db.prepare(`
            INSERT INTO mensagens (
                canal,
                usuario,
                mensagem,
                timestamp_envio,
                timestamp_publicacao
            )
            VALUES (?, ?, ?, ?, ?)
        `);

        comando.run(
            canalLimpo,
            usuarioLimpo,
            mensagem,
            timestamp,
            timestampPublicacao
        );

        await publicador.send([
            Buffer.from(canalLimpo + "\0"),
            msgpack.encode(publicacao)
        ]);

        console.log("\n========== MENSAGEM PUBLICADA ==========");
        console.log(publicacao);

        return criarResposta(
            tipo,
            "sucesso",
            "Mensagem publicada com sucesso",
            {
                canal: canalLimpo,
                timestamp_publicacao: timestampPublicacao,
            timestamp: obterTimestamp()
            }
        );
    }

    return criarResposta(
        tipo,
        "erro",
        "Operacao desconhecida"
    );
}

async function iniciarServidor() {

    const servidor = new zmq.Reply();
    const publicador = new zmq.Publisher();

    let enderecoUtilizado;

    try {

        if (SERVER_ENDPOINT) {

            servidor.connect(SERVER_ENDPOINT);
            if (process.env.SERVER_BIND) await servidor.bind(ENDERECO);
            enderecoUtilizado = SERVER_ENDPOINT;

            console.log("Conectado ao broker em", SERVER_ENDPOINT);

        } else {

            await servidor.bind(ENDERECO);
            enderecoUtilizado = ENDERECO;

            console.log("Servidor aguardando conexoes em", ENDERECO);
        }

        publicador.connect(PUB_ENDPOINT);

        console.log("Conectado ao proxy PUB-SUB em", PUB_ENDPOINT);

        console.log("======================================");
        console.log("SERVIDOR JAVASCRIPT INICIADO");
        console.log("Endereco:", enderecoUtilizado);
        console.log("Banco de dados:", BANCO);
        console.log("Publicacoes PUB-SUB:", PUB_ENDPOINT);
        console.log("Aguardando requisicoes...");
        console.log("======================================");

        for await (const [mensagem] of servidor) {

            try {

                const requisicao = msgpack.decode(mensagem);

                console.log("\n========== MENSAGEM RECEBIDA ==========");
                console.log(requisicao);

                const resultado = await processarMensagem(
                    requisicao,
                    publicador
                );

                console.log("\n========== RESPOSTA ENVIADA ==========");
                console.log(resultado);

                const resposta = msgpack.encode(resultado);

                await servidor.send(resposta);

            } catch (erro) {

                console.error(
                    "Erro ao processar mensagem:",
                    erro.message
                );

                const respostaErro = criarResposta(
                    "desconhecido",
                    "erro",
                    "Mensagem invalida"
                );

                await servidor.send(msgpack.encode(respostaErro));
            }
        }

    } finally {

        servidor.close();
        publicador.close();
        db.close();

        console.log("Servidor JavaScript encerrado.");
    }
}

iniciarServidor().catch(erro => {

    console.error("Erro ao iniciar servidor:", erro);

    process.exit(1);
});
