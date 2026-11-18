package com.sistemasdistribuidos.cliente;

import com.sistemasdistribuidos.comum.Protocolo;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Bot {

    private static final String SERVIDOR =
            System.getenv().getOrDefault(
                    "SERVER_ENDPOINT",
                    "tcp://localhost:5555"
            );

    private static final String BOT_ID =
            System.getenv().getOrDefault("BOT_ID", "java-1");

    private static final String USUARIO = "bot-" + BOT_ID;
    private static final String CANAL = "canal-" + BOT_ID;

    private static Map<String, Object> criarDados(Object... valores) {

        Map<String, Object> dados = new LinkedHashMap<>();

        for (int i = 0; i < valores.length; i += 2) {
            dados.put((String) valores[i], valores[i + 1]);
        }

        return dados;
    }

    private static Map<String, Object> enviarRequisicao(ZContext contexto, String tipo, Map<String, Object> dados) throws IOException {
        try (ZMQ.Socket socket = contexto.createSocket(SocketType.REQ)) {
            socket.setLinger(0);
            socket.setReceiveTimeOut(3000);
            socket.setSendTimeOut(3000);
            socket.connect(SERVIDOR);
            Map<String, Object> requisicao = Protocolo.criarMensagem(tipo, dados);
            System.out.println("ENVIO " + requisicao);
            socket.send(Protocolo.codificar(requisicao));
            byte[] bytes = socket.recv();
            if (bytes == null) return Map.of("status", "erro", "dados", Map.of(), "mensagem", "Tempo de resposta excedido");
            Map<String, Object> resposta = Protocolo.decodificar(bytes);
            System.out.println("RECEBIMENTO " + resposta);
            return resposta;
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> listarCanais(
            ZContext contexto
    ) throws IOException {

        Map<String, Object> resposta = enviarRequisicao(
                contexto,
                "listar_canais",
                new LinkedHashMap<>()
        );

        List<String> canais = new ArrayList<>();

        if (!"sucesso".equals(resposta.get("status"))) {
            return canais;
        }

        Object dadosObjeto = resposta.get("dados");

        if (dadosObjeto instanceof Map) {

            Map<String, Object> dados =
                    (Map<String, Object>) dadosObjeto;

            Object canaisObjeto = dados.get("canais");

            if (canaisObjeto instanceof List) {

                for (Object canal : (List<?>) canaisObjeto) {

                    if (canal instanceof String) {
                        canais.add((String) canal);
                    }
                }
            }
        }

        return canais;
    }

    public static void main(String[] args) throws Exception {
        try (ZContext contexto = new ZContext()) {
            while (!"sucesso".equals(enviarRequisicao(contexto, "login", criarDados("usuario", USUARIO)).get("status"))) Thread.sleep(1000);
            int rodada = 0;
            while (true) {
                List<String> canais = listarCanais(contexto);
                if (canais.size() < 5) enviarRequisicao(contexto, "criar_canal", criarDados("nome", CANAL + "-" + rodada++, "usuario", USUARIO));
                Thread.sleep(3000);
            }
        }
    }
}
