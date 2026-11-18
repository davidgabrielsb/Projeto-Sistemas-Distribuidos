package com.sistemasdistribuidos.cliente;

import com.sistemasdistribuidos.comum.Protocolo;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Bot {

    private static final String SERVIDOR =
            System.getenv().getOrDefault(
                    "SERVER_ENDPOINT",
                    "tcp://localhost:5555"
            );

    private static final String SUB_ENDPOINT =
            System.getenv().getOrDefault(
                    "SUB_ENDPOINT",
                    "tcp://localhost:5558"
            );

    private static final String BOT_ID =
            System.getenv().getOrDefault("BOT_ID", "java-1");

    private static final String USUARIO = "bot-" + BOT_ID;
    private static final String CANAL = "canal-" + BOT_ID;

    private static final Random RANDOM = new Random();

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

    private static void receberMensagens(ZMQ.Socket socket, long prazo) throws IOException {
        while (System.nanoTime() < prazo) {
            byte[] topico = socket.recv();
            if (topico == null) continue;
            Map<String, Object> mensagem = Protocolo.decodificar(socket.recv());
            System.out.println("PUB-SUB recebido_em=" + Protocolo.obterTimestamp() + " conteudo=" + mensagem);
        }
    }

    public static void main(String[] args) throws Exception {
        try (ZContext contexto = new ZContext(); ZMQ.Socket subscriber = contexto.createSocket(SocketType.SUB)) {
            subscriber.connect(SUB_ENDPOINT);
            subscriber.setReceiveTimeOut(100);
            java.util.Set<String> inscritos = new java.util.HashSet<>();
            while (!"sucesso".equals(enviarRequisicao(contexto, "login", criarDados("usuario", USUARIO)).get("status"))) Thread.sleep(1000);
            int rodada = 0;
            while (true) {
                List<String> canais = listarCanais(contexto);
                if (canais.size() < 5) enviarRequisicao(contexto, "criar_canal", criarDados("nome", CANAL + "-" + rodada++, "usuario", USUARIO));
                List<String> disponiveis = new ArrayList<>(canais);
                Collections.shuffle(disponiveis);
                for (String canal : disponiveis) {
                    if (inscritos.size() < 3 && inscritos.add(canal)) {
                        subscriber.subscribe((canal + "\0").getBytes(StandardCharsets.UTF_8));
                        System.out.println("Inscrito em " + canal);
                    }
                }
                if (canais.isEmpty()) { receberMensagens(subscriber, System.nanoTime()+1_000_000_000L); continue; }
                String canal = canais.get(RANDOM.nextInt(canais.size()));
                for (int numero = 1; numero <= 10; numero++) {
                    enviarRequisicao(contexto, "publicar_mensagem", criarDados("canal", canal, "usuario", USUARIO,
                        "mensagem", "Mensagem " + numero + " de " + USUARIO + ": " + RANDOM.nextInt(10000)));
                    receberMensagens(subscriber, System.nanoTime()+1_000_000_000L);
                }
            }
        }
    }
}
