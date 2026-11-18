package com.sistemasdistribuidos.servidor;

import com.sistemasdistribuidos.comum.Protocolo;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Cluster {
    private final String nome = env("SERVER_NAME", "java-server");
    private final String endpoint = env("PEER_ENDPOINT", "tcp://" + nome + ":5570");
    private final String referencia = env("REFERENCE_ENDPOINT", "tcp://reference:5560");
    private volatile long rank;
    private volatile String coordenador = "";
    private int contagem;
    private int trocadas;
    private final AtomicBoolean heartbeatPendente = new AtomicBoolean();
    private final AtomicBoolean sincronizacaoPendente = new AtomicBoolean();
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

    @SuppressWarnings("unchecked")
    private Map<String, Object> atender(Map<String, Object> pedido) throws Exception {
        String tipo = (String) pedido.get("tipo");
        Map<String, Object> dados = (Map<String, Object>) pedido.get("dados");
        if (tipo.equals("eleicao") || tipo.equals("hora"))
            return Map.of("nome", nome, "rank", rank, "hora", Protocolo.hora(), "coordenador", coordenador);
        if (tipo.equals("ajustar")) {
            Protocolo.ajustar(((Number) dados.get("delta")).doubleValue());
            return Map.of("ajustado", true);
        }
        if (tipo.equals("sincronizar")) {
            sincronizacaoPendente.set(true);
            return Map.of("agendado", true);
        }
        throw new IllegalArgumentException("Operacao de servidor desconhecida");
    }

    private void servir() {
        try (ZContext contexto = new ZContext(); ZMQ.Socket socket = contexto.createSocket(SocketType.REP)) {
            socket.bind(env("PEER_BIND", "tcp://*:5570"));
            while (!Thread.currentThread().isInterrupted()) {
                byte[] bytes = socket.recv();
                Map<String, Object> resposta;
                try {
                    Map<String, Object> pedido = Protocolo.decodificar(bytes);
                    Protocolo.atualizarRelogioLogico(pedido.get("relogio_logico"));
                    log("RECEBIMENTO", pedido);
                    resposta = Protocolo.criarMensagem("resposta", atender(pedido));
                } catch (Exception erro) { resposta = Protocolo.criarMensagem("erro", Map.of("erro", erro.toString())); }
                log("ENVIO", resposta);
                socket.send(Protocolo.codificar(resposta));
            }
        } catch (Exception erro) { log("FALHA RPC", erro); System.exit(1); }
    }

    @SuppressWarnings("unchecked")
    private void observar() {
        try (ZContext contexto = new ZContext(); ZMQ.Socket socket = contexto.createSocket(SocketType.SUB)) {
            socket.connect(env("SUB_ENDPOINT", "tcp://pubsub-proxy:5558"));
            socket.subscribe("servers\0".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            while (!Thread.currentThread().isInterrupted()) {
                socket.recv();
                Map<String, Object> anuncio = Protocolo.decodificar(socket.recv());
                Protocolo.atualizarRelogioLogico(anuncio.get("relogio_logico"));
                coordenador = (String) ((Map<String, Object>) anuncio.get("dados")).get("nome");
                log("COORDENADOR", anuncio);
            }
        } catch (Exception erro) { log("FALHA SUB", erro); }
    }

    public void contar() {
        contagem++;
        if (contagem % 10 == 0) heartbeatPendente.set(true);
        contarTroca();
    }

    public void contarTroca() {
        trocadas++;
        if (trocadas % 15 == 0) sincronizacaoPendente.set(true);
    }

    private void berkeley(ZContext contexto, List<Map<String, Object>> vivos) throws Exception {
        Map<String, Double> diferencas = new LinkedHashMap<>();
        diferencas.put(endpoint, 0.0);
        for (Map<String, Object> servidor : vivos) {
            if (nome.equals(servidor.get("nome"))) continue;
            try {
                double inicio = Protocolo.hora();
                Map<String, Object> remoto = consultar(contexto, (String) servidor.get("endpoint"), "hora", Map.of());
                diferencas.put((String) servidor.get("endpoint"), ((Number) remoto.get("hora")).doubleValue() - (inicio + Protocolo.hora())/2);
            } catch (Exception erro) { log("INDISPONIVEL", servidor.get("nome")); }
        }
        double media = diferencas.values().stream().mapToDouble(Double::doubleValue).average().orElse(0);
        for (var item : diferencas.entrySet()) {
            if (item.getKey().equals(endpoint)) Protocolo.ajustar(media);
            else {
                try { consultar(contexto, item.getKey(), "ajustar", Map.of("delta", media - item.getValue())); }
                catch (Exception erro) { log("INDISPONIVEL", item.getKey()); }
            }
        }
        log("BERKELEY", Map.of("participantes", diferencas.size(), "media", media));
    }

    @SuppressWarnings("unchecked")
    private void manter() {
        try (ZContext contexto = new ZContext(); ZMQ.Socket publicador = contexto.createSocket(SocketType.PUB)) {
            publicador.connect(env("PUB_ENDPOINT", "tcp://pubsub-proxy:5557"));
            long ultimoHeartbeat = 0, ultimaEleicao = 0;
            List<Map<String, Object>> vivos = new ArrayList<>();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    long agora = System.nanoTime() / 1_000_000;
                    if (rank == 0) rank = ((Number) consultar(contexto, referencia, "registrar_servidor",
                        Map.of("nome", nome, "endpoint", endpoint)).get("rank")).longValue();
                    if (heartbeatPendente.getAndSet(false) || agora - ultimoHeartbeat > 5000) {
                        Map<String, Object> resposta = consultar(contexto, referencia, "heartbeat", Map.of("nome", nome, "rank", rank));
                        if (Boolean.TRUE.equals(resposta.get("registrar"))) rank = 0;
                        ultimoHeartbeat = agora;
                    }
                    if (agora - ultimaEleicao > 3000) {
                        List<Map<String, Object>> servidores = (List<Map<String, Object>>) consultar(contexto, referencia, "listar_servidores", Map.of()).get("servidores");
                        vivos = new ArrayList<>();
                        for (Map<String, Object> servidor : servidores) {
                            try {
                                Map<String, Object> info = consultar(contexto, (String) servidor.get("endpoint"), "eleicao", Map.of("candidato", nome));
                                if (((Number) info.get("rank")).longValue() > 0) {
                                    Map<String, Object> vivo = new HashMap<>(servidor);
                                    vivo.put("rank", info.get("rank"));
                                    vivos.add(vivo);
                                }
                            } catch (Exception erro) { log("INDISPONIVEL", servidor.get("nome")); }
                        }
                        if (!vivos.isEmpty()) {
                            Map<String, Object> eleito = Collections.max(vivos, Comparator.comparingLong(s -> ((Number)s.get("rank")).longValue()));
                            boolean mudou = !coordenador.equals(eleito.get("nome"));
                            coordenador = (String) eleito.get("nome");
                            if (nome.equals(coordenador)) {
                                Map<String, Object> anuncio = Protocolo.criarMensagem("coordenador", Map.of("nome", nome, "rank", rank));
                                publicador.sendMore("servers\0");
                                publicador.send(Protocolo.codificar(anuncio));
                                log("PUBLICACAO", anuncio);
                                if (mudou) sincronizacaoPendente.set(true);
                            }
                        }
                        ultimaEleicao = agora;
                    }
                    if (sincronizacaoPendente.getAndSet(false)) {
                        if (nome.equals(coordenador)) berkeley(contexto, vivos);
                        else for (Map<String, Object> servidor : vivos) {
                            if (coordenador.equals(servidor.get("nome"))) consultar(contexto, (String)servidor.get("endpoint"), "sincronizar", Map.of());
                        }
                    }
                } catch (Exception erro) { log("MANUTENCAO", erro); }
                Thread.sleep(100);
            }
        } catch (Exception erro) { log("FALHA MANUTENCAO", erro); }
    }

    public void iniciar() {
        for (Runnable tarefa : List.<Runnable>of(this::servir, this::manter, this::observar)) {
            Thread thread = new Thread(tarefa);
            thread.setDaemon(true);
            thread.start();
        }
    }
}
