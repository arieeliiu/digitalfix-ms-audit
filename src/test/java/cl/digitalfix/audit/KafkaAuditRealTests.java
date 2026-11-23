package cl.digitalfix.audit;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import cl.digitalfix.audit.controller.AuditoriaController;
import cl.digitalfix.audit.repository.EventoAuditoriaRepository;
import cl.digitalfix.audit.service.AuditoriaService;
import static org.junit.jupiter.api.Assertions.*;

// Broker, servidor HTTP y firma JWT locales reales; no certifica Oracle ni Entra externos.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "digitalfix.kafka.enabled=true", "digitalfix.kafka.espera-reintento=20ms",
        "spring.datasource.url=jdbc:h2:mem:audit-kafka-prueba;MODE=Oracle;DB_CLOSE_DELAY=-1",
        "digitalfix.kafka.bootstrap-servers=${digitalfix.pruebas.kafka.bootstrap:localhost:9092,localhost:9093,localhost:9094}"})
@EnabledIfSystemProperty(named = "digitalfix.pruebas.kafka", matches = "true")
class KafkaAuditRealTests {
    static final String GRUPO = "audit-prueba-" + UUID.randomUUID();
    static final long ORDEN = java.util.concurrent.ThreadLocalRandom.current().nextLong(1000000000L, 2000000000L);
    static final long ORDEN_FALLO = ORDEN + 1;
    @DynamicPropertySource static void grupo(DynamicPropertyRegistry propiedades) {
        propiedades.add("digitalfix.kafka.group-id", () -> GRUPO);
    }
    @Autowired KafkaTemplate<String, String> productor;
    @Autowired EventoAuditoriaRepository repositorio;
    @Autowired AuditoriaService servicio;
    @Autowired JdbcTemplate jdbc;
    @Autowired DefaultErrorHandler errores;
    @Autowired AuditoriaController controller;
    @org.springframework.beans.factory.annotation.Value("${local.server.port}") int puerto;

    @org.springframework.boot.test.context.TestConfiguration(proxyBeanMethods = false)
    static class IdentidadLocal {
        static final java.security.KeyPair CLAVES = claves();
        private static java.security.KeyPair claves() {
            try {
                var generador = java.security.KeyPairGenerator.getInstance("RSA");
                generador.initialize(2048);
                return generador.generateKeyPair();
            } catch (Exception e) { throw new ExceptionInInitializerError(e); }
        }
        @org.springframework.context.annotation.Bean JwtDecoder jwtDecoder() {
            var decoder = NimbusJwtDecoder.withPublicKey((java.security.interfaces.RSAPublicKey) CLAVES.getPublic()).build();
            decoder.setJwtValidator(JwtValidators.createDefaultWithIssuer("https://emisor.local"));
            return decoder;
        }
    }

    @Test void consumeAmbosTopicosDeduplicaConsultaYEnrutaFallos() throws Exception {
        var config = new Properties();
        config.put("bootstrap.servers", System.getProperty("digitalfix.pruebas.kafka.bootstrap", "localhost:9092,localhost:9093,localhost:9094"));
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("value.deserializer", StringDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        try (var admin = Admin.create(config); var dlts = new KafkaConsumer<String, String>(config)) {
            List<TopicPartition> origen = particiones(admin, List.of("workorders.events", "audit.timeline"));
            List<TopicPartition> destinos = particiones(admin, List.of("workorders.events.audit.DLT", "audit.timeline.audit.DLT"));
            dlts.assign(destinos);
            dlts.seekToEnd(destinos);
            destinos.forEach(p -> dlts.position(p, Duration.ofSeconds(10)));
            esperar(() -> alDia(admin, origen), "consumo del backlog existente");
            System.out.println("AUDIT_REAL backlog orden 1=" + servicio.timeline(1L).size());
            var id = UUID.randomUUID();
            enviar("workorders.events", 1, Long.toString(ORDEN), ConsumidorEventosOrdenTests.creada(id, ORDEN, "2026-11-23T15:00:00Z"));
            var idEdicion = UUID.randomUUID();
            String edicion = ConsumidorEventosOrdenTests.evento(idEdicion, "ORDEN_CONTACTOS_ACTUALIZADOS", ORDEN,
                    "EDICION_CONTACTOS", "CREADA", "CREADA", "oid-cliente", "2026-11-23T15:01:00Z");
            enviar("audit.timeline", 1, Long.toString(ORDEN), edicion);
            enviar("audit.timeline", 1, Long.toString(ORDEN), edicion);
            esperar(() -> servicio.timeline(ORDEN).size() == 2 && alDia(admin, origen), "persistencia/idempotencia");
            assertTrue(repositorio.existsByEventId(idEdicion.toString()));
            assertEquals(Set.of("workorders.events", "audit.timeline"), servicio.timeline(ORDEN).stream()
                    .map(e -> e.kafkaTopico()).collect(java.util.stream.Collectors.toSet()));
            try (var http = java.net.http.HttpClient.newHttpClient()) {
                var respuesta = consultar(http, token("Admin"));
                assertEquals(200, respuesta.statusCode());
                assertEquals(2, tools.jackson.databind.json.JsonMapper.builder().build().readTree(respuesta.body()).size());
                assertEquals(401, consultar(http, null).statusCode());
                assertEquals(403, consultar(http, token("Cliente")).statusCode());
                String invalido = token("Admin");
                int punto = invalido.lastIndexOf('.') + 1;
                invalido = invalido.substring(0, punto) + (invalido.charAt(punto) == 'A' ? 'B' : 'A') + invalido.substring(punto + 1);
                assertEquals(401, consultar(http, invalido).statusCode());
            }

            String malo = "{invalido-" + UUID.randomUUID();
            enviar("workorders.events", 1, "error", malo);
            enviar("audit.timeline", 2, "error", malo);
            var fallos = new ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>>();
            long limite = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (fallos.size() < 2 && System.nanoTime() < limite) {
                for (var r : dlts.poll(Duration.ofMillis(200))) if (r.value().equals(malo)) fallos.add(r);
            }
            assertEquals(2, fallos.size());
            assertTrue(fallos.stream().anyMatch(r -> r.topic().equals("workorders.events.audit.DLT") && r.partition() == 1));
            assertTrue(fallos.stream().anyMatch(r -> r.topic().equals("audit.timeline.audit.DLT") && r.partition() == 2));
            esperar(() -> alDia(admin, origen), "offsets tras DLT");

            var intentos = new AtomicInteger();
            errores.setRetryListeners((r, ex, intento) -> { if (Long.toString(ORDEN_FALLO).equals(r.key())) intentos.set(intento); });
            var idFallo = UUID.randomUUID();
            String falloTemporal = ConsumidorEventosOrdenTests.creada(idFallo, ORDEN_FALLO, "2026-11-23T15:02:00Z");
            jdbc.execute("ALTER TABLE auditoria_eventos RENAME TO auditoria_eventos_prueba_pausa");
            boolean enDlt = false;
            try {
                enviar("workorders.events", 0, Long.toString(ORDEN_FALLO), falloTemporal);
                limite = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                while (!enDlt && System.nanoTime() < limite) {
                    for (var r : dlts.poll(Duration.ofMillis(200))) if (r.value().equals(falloTemporal)) enDlt = true;
                }
            } finally {
                jdbc.execute("ALTER TABLE auditoria_eventos_prueba_pausa RENAME TO auditoria_eventos");
            }
            assertTrue(enDlt, "El fallo de persistencia debe alcanzar DLT");
            assertEquals(4, intentos.get(), "Intento inicial y tres reintentos");
            assertFalse(repositorio.existsByEventId(idFallo.toString()));
            esperar(() -> alDia(admin, origen), "lag final cero");
            System.out.println("AUDIT_REAL OK: ambos tópicos, API 200/401/403, duplicado, DLT por origen, 3 reintentos y lag cero");
        }
    }

    private void enviar(String topico, int particion, String key, String valor) throws Exception {
        var registro = new ProducerRecord<String, String>(topico, particion, key, valor);
        registro.headers().add("correlationId", key.getBytes(StandardCharsets.UTF_8));
        productor.send(registro).get(10, TimeUnit.SECONDS);
    }

    private java.net.http.HttpResponse<String> consultar(java.net.http.HttpClient cliente, String token) throws Exception {
        var solicitud = java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:" + puerto + "/api/audit/ordenes/" + ORDEN + "/timeline"))
                .timeout(Duration.ofSeconds(5));
        if (token != null) solicitud.header("Authorization", "Bearer " + token);
        return cliente.send(solicitud.GET().build(), java.net.http.HttpResponse.BodyHandlers.ofString());
    }

    private static String token(String rol) throws Exception {
        var base64 = Base64.getUrlEncoder().withoutPadding();
        var ahora = java.time.Instant.now();
        var json = tools.jackson.databind.json.JsonMapper.builder().build();
        String header = base64.encodeToString("{\"alg\":\"RS256\"}".getBytes(StandardCharsets.UTF_8));
        String payload = base64.encodeToString(json.writeValueAsString(Map.of("iss", "https://emisor.local", "sub", "oid-prueba",
                "iat", ahora.getEpochSecond(), "exp", ahora.plusSeconds(300).getEpochSecond(), "roles", List.of(rol))).getBytes(StandardCharsets.UTF_8));
        String contenido = header + "." + payload;
        var firma = java.security.Signature.getInstance("SHA256withRSA");
        firma.initSign(IdentidadLocal.CLAVES.getPrivate());
        firma.update(contenido.getBytes(StandardCharsets.UTF_8));
        return contenido + "." + base64.encodeToString(firma.sign());
    }

    private static List<TopicPartition> particiones(Admin admin, List<String> topicos) throws Exception {
        var infos = admin.describeTopics(topicos).allTopicNames().get(15, TimeUnit.SECONDS);
        List<TopicPartition> lista = new ArrayList<>();
        infos.forEach((nombre, info) -> info.partitions().forEach(p -> lista.add(new TopicPartition(nombre, p.partition()))));
        return lista;
    }

    private static boolean alDia(Admin admin, List<TopicPartition> origen) {
        try {
            var posiciones = admin.listConsumerGroupOffsets(GRUPO).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            var finales = admin.listOffsets(origen.stream().collect(java.util.stream.Collectors.toMap(p -> p,
                    p -> org.apache.kafka.clients.admin.OffsetSpec.latest()))).all().get(5, TimeUnit.SECONDS);
            return origen.stream().allMatch(p -> finales.get(p).offset() == 0
                    || (posiciones.get(p) != null && posiciones.get(p).offset() >= finales.get(p).offset()));
        } catch (Exception e) { return false; }
    }

    private static void esperar(BooleanSupplier condicion, String descripcion) throws Exception {
        long limite = System.nanoTime() + Duration.ofSeconds(25).toNanos();
        while (System.nanoTime() < limite) {
            if (condicion.getAsBoolean()) return;
            Thread.sleep(100);
        }
        fail("Timeout: " + descripcion);
    }
}
