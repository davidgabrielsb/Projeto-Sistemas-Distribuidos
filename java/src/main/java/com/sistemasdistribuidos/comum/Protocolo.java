package com.sistemasdistribuidos.comum;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Protocolo {

    private static final DateTimeFormatter FORMATO_DATA =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                    .withZone(ZoneOffset.UTC);

    public static String obterTimestamp() {
        return FORMATO_DATA.format(Instant.now());
    }

    public static Map<String, Object> criarMensagem(
            String tipo,
            Map<String, Object> dados) {

        Map<String, Object> mensagem = new LinkedHashMap<>();

        mensagem.put("tipo", tipo);
        mensagem.put("dados", dados);
        mensagem.put("timestamp", obterTimestamp());

        return mensagem;
    }

    public static byte[] codificar(Map<String, Object> mensagem)
            throws IOException {

        try (var packer = MessagePack.newDefaultBufferPacker()) {
            empacotarMapa(packer, mensagem);
            return packer.toByteArray();
        }
    }

    public static Map<String, Object> decodificar(byte[] bytes)
            throws IOException {

        try (MessageUnpacker unpacker =
                     MessagePack.newDefaultUnpacker(bytes)) {

            Value valor = unpacker.unpackValue();

            Object resultado = converterValor(valor);

            if (!(resultado instanceof Map)) {
                throw new IOException("Mensagem invalida");
            }

            return (Map<String, Object>) resultado;
        }
    }

    private static void empacotarMapa(
            MessagePacker packer,
            Map<String, Object> mapa) throws IOException {

        packer.packMapHeader(mapa.size());

        for (Map.Entry<String, Object> entrada : mapa.entrySet()) {
            packer.packString(entrada.getKey());
            empacotarValor(packer, entrada.getValue());
        }
    }

    private static void empacotarValor(
            MessagePacker packer,
            Object valor) throws IOException {

        if (valor == null) {
            packer.packNil();

        } else if (valor instanceof String) {
            packer.packString((String) valor);

        } else if (valor instanceof Integer || valor instanceof Long) {
            packer.packLong(((Number) valor).longValue());

        } else if (valor instanceof Double || valor instanceof Float) {
            packer.packDouble(((Number) valor).doubleValue());

        } else if (valor instanceof Boolean) {
            packer.packBoolean((Boolean) valor);

        } else if (valor instanceof Map) {
            empacotarMapa(
                    packer,
                    (Map<String, Object>) valor
            );

        } else if (valor instanceof List) {
            List<?> lista = (List<?>) valor;

            packer.packArrayHeader(lista.size());

            for (Object item : lista) {
                empacotarValor(packer, item);
            }

        } else {
            throw new IOException("Tipo de dado nao suportado");
        }
    }

    private static Object converterValor(Value valor) {

        if (valor.isNilValue()) {
            return null;
        }

        if (valor.isStringValue()) {
            return valor.asStringValue().asString();
        }

        if (valor.isIntegerValue()) {
            return valor.asIntegerValue().toLong();
        }

        if (valor.isFloatValue()) {
            return valor.asFloatValue().toDouble();
        }

        if (valor.isBooleanValue()) {
            return valor.asBooleanValue().getBoolean();
        }

        if (valor.isMapValue()) {
            Map<String, Object> mapa = new LinkedHashMap<>();

            for (Map.Entry<Value, Value> entrada :
                    valor.asMapValue().map().entrySet()) {

                String chave = entrada.getKey()
                        .asStringValue().asString();

                mapa.put(chave, converterValor(entrada.getValue()));
            }

            return mapa;
        }

        if (valor.isArrayValue()) {
            List<Object> lista = new ArrayList<>();

            for (Value item : valor.asArrayValue().list()) {
                lista.add(converterValor(item));
            }

            return lista;
        }

        throw new IllegalArgumentException(
                "Tipo MessagePack nao reconhecido"
        );
    }
}
