package com.sistemasdistribuidos.servidor;

import com.sistemasdistribuidos.comum.Protocolo;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Referencia {
    private final String nome = env("SERVER_NAME", "java-server");
    private final String referencia = env("REFERENCE_ENDPOINT", "tcp://reference:5560");
    private volatile long rank;
    private int contagem;
    private final AtomicBoolean heartbeatPendente = new AtomicBoolean();
    private static String env(String chave, String padrao) {
        return System.getenv().getOrDefault(chave, padrao);
    }

    private void log(String direcao, Object mensagem) {
        System.out.println("[" + nome + "] " + direcao + " " + mensagem);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> consultar(ZContext contexto, String endereco, String tipo, Map<String, Object> dados) throws Exception {
        try (ZMQ.Socket socket = contexto.createSocket(SocketType.REQ)) {
            socket.setLinger(0);
            socket.setReceiveTimeOut(800);
            socket.setSendTimeOut(800);
            socket.connect(endereco);
            Map<String, Object> pedido = Protocolo.criarMensagem(tipo, dados);
            log("ENVIO", pedido);
            socket.send(Protocolo.codificar(pedido));
            byte[] bytes = socket.recv();
            if (bytes == null) throw new java.io.IOException("Sem resposta: " + endereco);
            Map<String, Object> resposta = Protocolo.decodificar(bytes);
            Protocolo.atualizarRelogioLogico(resposta.get("relogio_logico"));
            log("RECEBIMENTO", resposta);
            return (Map<String, Object>) resposta.get("dados");
        }
    }

    public void contar() {
        contagem++;
        if (contagem % 10 == 0) heartbeatPendente.set(true);
    }

    @SuppressWarnings("unchecked")
    private void manter() {
        try (ZContext contexto = new ZContext()) {
            long ultimoHeartbeat = 0;
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    long agora = System.nanoTime() / 1_000_000;
                    if (rank == 0) rank = ((Number) consultar(contexto, referencia, "registrar_servidor",
                        Map.of("nome", nome)).get("rank")).longValue();
                    if (heartbeatPendente.getAndSet(false) || agora - ultimoHeartbeat > 5000) {
                        long inicio = System.currentTimeMillis();
                        Map<String, Object> resposta = consultar(contexto, referencia, "heartbeat", Map.of("nome", nome, "rank", rank));
                        if (Boolean.TRUE.equals(resposta.get("registrar"))) rank = 0;
                        if (resposta.containsKey("hora"))
                            Protocolo.ajustar(((Number) resposta.get("hora")).doubleValue() + (System.currentTimeMillis()-inicio)/2.0 - Protocolo.hora());
                        ultimoHeartbeat = agora;
                    }
                } catch (Exception erro) { log("MANUTENCAO", erro); }
                Thread.sleep(100);
            }
        } catch (Exception erro) { log("FALHA MANUTENCAO", erro); }
    }

    public void iniciar() {
        for (Runnable tarefa : List.<Runnable>of(this::manter)) {
            Thread thread = new Thread(tarefa);
            thread.setDaemon(true);
            thread.start();
        }
    }
}
