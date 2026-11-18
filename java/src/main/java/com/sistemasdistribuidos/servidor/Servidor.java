package com.sistemasdistribuidos.servidor;

import com.sistemasdistribuidos.comum.Protocolo;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Servidor {

    private static final String SERVER_ENDPOINT =
            System.getenv("SERVER_ENDPOINT");

    private static final String PUB_ENDPOINT =
            System.getenv().getOrDefault(
                    "PUB_ENDPOINT",
                    "tcp://pubsub-proxy:5557"
            );

    private static final String ENDERECO =
            System.getenv().getOrDefault(
                    "SERVER_BIND",
                    "tcp://*:5555"
            );

    private static final String CAMINHO_BANCO =
            System.getenv().getOrDefault(
                    "DB_PATH",
                    "servidor.db"
            );

    private static final String BANCO =
            "jdbc:sqlite:" + CAMINHO_BANCO;

    private static Connection conectarBanco() throws SQLException {
        Connection db = DriverManager.getConnection(BANCO);
        try (Statement comando = db.createStatement()) { comando.execute("PRAGMA busy_timeout=10000"); }
        return db;
    }

    private static void criarBanco() throws SQLException {

        try (Connection conexao = conectarBanco();
             Statement comando = conexao.createStatement()) {

            comando.execute("""
                CREATE TABLE IF NOT EXISTS logins (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    usuario TEXT NOT NULL,
                    timestamp TEXT NOT NULL
                )
            """);

            comando.execute("""
                CREATE TABLE IF NOT EXISTS canais (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nome TEXT NOT NULL UNIQUE,
                    usuario TEXT NOT NULL,
                    timestamp TEXT NOT NULL
                )
            """);

            comando.execute("""
                CREATE TABLE IF NOT EXISTS mensagens (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    canal TEXT NOT NULL,
                    usuario TEXT NOT NULL,
                    mensagem TEXT NOT NULL,
                    timestamp_envio TEXT NOT NULL,
                    timestamp_publicacao TEXT NOT NULL
                )
            """);
        }

        System.out.println("Banco de dados inicializado: " + BANCO);
    }

    private static Map<String, Object> resposta(
            String tipo,
            String status,
            String mensagem,
            Map<String, Object> dados) {

        Map<String, Object> resultado = new LinkedHashMap<>();

        resultado.put("tipo", tipo);
        resultado.put("status", status);
        resultado.put("mensagem", mensagem);
        resultado.put("dados", dados);
        resultado.put("timestamp", Protocolo.obterTimestamp());

        return resultado;
    }

    private static Map<String, Object> processarMensagem(
            Map<String, Object> requisicao,
            ZMQ.Socket socketPub
    ) throws SQLException, IOException {

        if (requisicao == null) {
            return resposta(
                    "desconhecido",
                    "erro",
                    "Mensagem invalida",
                    new LinkedHashMap<>()
            );
        }

        Object tipoObjeto = requisicao.get("tipo");
        Object dadosObjeto = requisicao.get("dados");
        Object timestampObjeto = requisicao.get("timestamp");

        if (!(tipoObjeto instanceof String)) {
            return resposta(
                    "desconhecido",
                    "erro",
                    "Tipo de mensagem invalido",
                    new LinkedHashMap<>()
            );
        }

        String tipo = (String) tipoObjeto;
        if (!(timestampObjeto instanceof String)
                || ((String) timestampObjeto).isBlank()) {

            return resposta(
                    tipo,
                    "erro",
                    "Timestamp invalido",
                    new LinkedHashMap<>()
            );
        }

        if (!(dadosObjeto instanceof Map)) {
            return resposta(
                    tipo,
                    "erro",
                    "Dados invalidos",
                    new LinkedHashMap<>()
            );
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> dados = (Map<String, Object>) dadosObjeto;

        String timestamp = (String) timestampObjeto;

        if (tipo.equals("historico")) {
            int limite = Math.max(1, Math.min(100, ((Number)dados.getOrDefault("limite", 50)).intValue()));
            int inicio = Math.max(0, ((Number)dados.getOrDefault("inicio", 0)).intValue());
            List<Object> mensagens = new ArrayList<>();
            try (Connection db = conectarBanco(); PreparedStatement comando = db.prepareStatement(
                    "SELECT canal,usuario,mensagem,timestamp_envio,timestamp_publicacao FROM mensagens WHERE canal=? ORDER BY timestamp_envio,id LIMIT ? OFFSET ?")) {
                comando.setString(1, (String)dados.getOrDefault("canal", ""));
                comando.setInt(2, limite);
                comando.setInt(3, inicio);
                try (ResultSet linhas = comando.executeQuery()) {
                    while (linhas.next()) {
                        Map<String, Object> mensagem = new LinkedHashMap<>();
                        for (String campo : List.of("canal", "usuario", "mensagem", "timestamp_envio", "timestamp_publicacao")) mensagem.put(campo, linhas.getString(campo));
                        mensagens.add(mensagem);
                    }
                }
            }
            return resposta(tipo, "sucesso", "Historico do canal", Map.of("mensagens", mensagens, "inicio", inicio, "limite", limite));
        }

        if (tipo.equals("login")) {

            Object usuarioObjeto = dados.get("usuario");

            if (!(usuarioObjeto instanceof String)
                    || ((String) usuarioObjeto).isBlank()) {

                return resposta(
                        tipo,
                        "erro",
                        "Nome de usuario invalido",
                        new LinkedHashMap<>()
                );
            }

            String usuario = ((String) usuarioObjeto).trim();

            try (Connection conexao = conectarBanco();
                 PreparedStatement comando = conexao.prepareStatement(
                         "INSERT INTO logins (usuario, timestamp) VALUES (?, ?)")) {

                comando.setString(1, usuario);
                comando.setString(2, timestamp);
                comando.executeUpdate();
            }

            return resposta(
                    tipo,
                    "sucesso",
                    "Login realizado com sucesso",
                    new LinkedHashMap<>()
            );
        }

        else if (tipo.equals("criar_canal")) {

            Object nomeObjeto = dados.get("nome");
            Object usuarioObjeto = dados.get("usuario");

            if (!(nomeObjeto instanceof String)
                    || !(usuarioObjeto instanceof String)) {

                return resposta(
                        tipo,
                        "erro",
                        "Dados do canal invalidos",
                        new LinkedHashMap<>()
                );
            }

            String nome = ((String) nomeObjeto).trim();
            String usuario = ((String) usuarioObjeto).trim();

            if (nome.isEmpty() || usuario.isEmpty()) {
                return resposta(
                        tipo,
                        "erro",
                        "Nome do canal ou usuario invalido",
                        new LinkedHashMap<>()
                );
            }

            try (Connection conexao = conectarBanco();
                 PreparedStatement comando = conexao.prepareStatement(
                         "INSERT INTO canais (nome, usuario, timestamp) VALUES (?, ?, ?)")) {

                comando.setString(1, nome);
                comando.setString(2, usuario);
                comando.setString(3, timestamp);

                comando.executeUpdate();

                return resposta(
                        tipo,
                        "sucesso",
                        "Canal criado com sucesso",
                        new LinkedHashMap<>()
                );

            } catch (SQLException erro) {

                if (erro.getMessage() != null
                        && erro.getMessage().contains("UNIQUE constraint failed")) {

                    return resposta(
                            tipo,
                            "erro",
                            "Esse canal ja existe",
                            new LinkedHashMap<>()
                    );
                }

                throw erro;
            }
        }

        else if (tipo.equals("listar_canais")) {

            List<String> canais = new ArrayList<>();

            try (Connection conexao = conectarBanco();
                 PreparedStatement comando = conexao.prepareStatement(
                         "SELECT nome FROM canais ORDER BY nome");
                 ResultSet resultado = comando.executeQuery()) {

                while (resultado.next()) {
                    canais.add(resultado.getString("nome"));
                }
            }

            Map<String, Object> dadosResposta = new LinkedHashMap<>();
            dadosResposta.put("canais", canais);

            return resposta(
                    tipo,
                    "sucesso",
                    "Lista de canais",
                    dadosResposta
            );
        }

        else if (tipo.equals("publicar_mensagem")) {

            Object canalObjeto = dados.get("canal");
            Object usuarioObjeto = dados.get("usuario");
            Object mensagemObjeto = dados.get("mensagem");

            if (!(canalObjeto instanceof String)
                    || !(usuarioObjeto instanceof String)
                    || !(mensagemObjeto instanceof String)) {

                return resposta(
                        tipo,
                        "erro",
                        "Dados da mensagem invalidos",
                        new LinkedHashMap<>()
                );
            }

            String canal = ((String) canalObjeto).trim();
            String usuario = ((String) usuarioObjeto).trim();
            String conteudo = ((String) mensagemObjeto).trim();

            if (canal.isEmpty() || usuario.isEmpty() || conteudo.isEmpty()) {

                return resposta(
                        tipo,
                        "erro",
                        "Canal, usuario e mensagem sao obrigatorios",
                        new LinkedHashMap<>()
                );
            }

            String timestampPublicacao = Protocolo.obterTimestamp();

            Map<String, Object> publicacao = new LinkedHashMap<>();

            publicacao.put("canal", canal);
            publicacao.put("usuario", usuario);
            publicacao.put("mensagem", conteudo);
            publicacao.put("timestamp_envio", timestamp);
            publicacao.put("timestamp_publicacao", timestampPublicacao);
            publicacao.put("timestamp", timestampPublicacao);

    
            try (Connection conexao = conectarBanco();
                 PreparedStatement comando = conexao.prepareStatement("""
                     INSERT INTO mensagens (
                         canal,
                         usuario,
                         mensagem,
                         timestamp_envio,
                         timestamp_publicacao
                     )
                     VALUES (?, ?, ?, ?, ?)
                 """)) {

                comando.setString(1, canal);
                comando.setString(2, usuario);
                comando.setString(3, conteudo);
                comando.setString(4, timestamp);
                comando.setString(5, timestampPublicacao);

                comando.executeUpdate();
            }

            byte[] mensagemCodificada = Protocolo.codificar(publicacao);

            socketPub.sendMore(canal + "\0");
            socketPub.send(mensagemCodificada, 0);

            System.out.println("\n========== MENSAGEM PUBLICADA ==========");
            System.out.println(publicacao);

            Map<String, Object> dadosResposta = new LinkedHashMap<>();
            dadosResposta.put("canal", canal);

            return resposta(
                    tipo,
                    "sucesso",
                    "Mensagem publicada com sucesso",
                    dadosResposta
            );
        }

        return resposta(
                tipo,
                "erro",
                "Operacao desconhecida",
                new LinkedHashMap<>()
        );
    }

    public static void main(String[] args) throws Exception {
        criarBanco();
        try (ZContext contexto = new ZContext();
             ZMQ.Socket socket = contexto.createSocket(SocketType.REP);
             ZMQ.Socket publicador = contexto.createSocket(SocketType.PUB)) {
            publicador.connect(PUB_ENDPOINT);
            if (SERVER_ENDPOINT != null && !SERVER_ENDPOINT.isBlank()) {
                socket.connect(SERVER_ENDPOINT);
                if (System.getenv("SERVER_BIND") != null) socket.bind(ENDERECO);
            }
            else socket.bind(ENDERECO);
            while (!Thread.currentThread().isInterrupted()) {
                byte[] bytes = socket.recv();
                Map<String, Object> resultado;
                try {
                    Map<String, Object> requisicao = Protocolo.decodificar(bytes);
                    System.out.println("RECEBIMENTO " + requisicao);
                    resultado = processarMensagem(requisicao, publicador);
                } catch (Exception erro) {
                    System.out.println("Falha no processamento: " + erro);
                    resultado = resposta("desconhecido", "erro", "Mensagem invalida", Map.of());
                }
                System.out.println("ENVIO " + resultado);
                socket.send(Protocolo.codificar(resultado));
            }
        }
    }
}
