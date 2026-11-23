package cl.digitalfix.audit;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import static org.junit.jupiter.api.Assertions.*;

// Verifica el offset del grupo real cuando el DLT no existe y cuando vuelve a estar disponible.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "digitalfix.kafka.enabled=false", "digitalfix.kafka.espera-reintento=20ms",
        "spring.datasource.url=jdbc:h2:mem:audit-dlt-prueba;MODE=Oracle;DB_CLOSE_DELAY=-1",
        "digitalfix.kafka.bootstrap-servers=${digitalfix.pruebas.kafka.bootstrap:localhost:9092,localhost:9093,localhost:9094}"})
@EnabledIfSystemProperty(named = "digitalfix.pruebas.kafka", matches = "true")
class KafkaDltFalloRealTests {
    static final String GRUPO = "audit-dlt-prueba-" + UUID.randomUUID();
    static final String DLT = "audit.timeline.audit.prueba-" + UUID.randomUUID();
    @DynamicPropertySource static void configurar(DynamicPropertyRegistry p) {
        p.add("digitalfix.kafka.group-id", () -> GRUPO);
        p.add("digitalfix.kafka.topico-dlt-ediciones", () -> DLT);
    }
    @Autowired KafkaListenerEndpointRegistry registro;
    @Autowired KafkaTemplate<String, String> productor;
    @Autowired DefaultErrorHandler errores;

    @Test void soloAvanzaElOffsetCuandoElDltConfirma() throws Exception {
        var config = new Properties();
        config.put("bootstrap.servers", System.getProperty("digitalfix.pruebas.kafka.bootstrap", "localhost:9092,localhost:9093,localhost:9094"));
        config.put("group.id", GRUPO);
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("value.deserializer", StringDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        var container = registro.getListenerContainer("auditoria-eventos-orden");
        var fallos = new AtomicInteger();
        errores.setRetryListeners(new RetryListener() {
            @Override public void failedDelivery(ConsumerRecord<?, ?> r, Exception e, int intento) { }
            @Override public void recoveryFailed(ConsumerRecord<?, ?> r, Exception original, Exception fallo) { fallos.incrementAndGet(); }
        });
        boolean creado = false;
        try (var admin = Admin.create(config)) {
            // El grupo nuevo comienza al final: no reprocesa eventos de otras pruebas.
            var infos = admin.describeTopics(List.of("workorders.events", "audit.timeline"))
                    .allTopicNames().get(15, TimeUnit.SECONDS);
            List<TopicPartition> particiones = new ArrayList<>();
            infos.forEach((nombre, info) -> info.partitions().forEach(p -> particiones.add(new TopicPartition(nombre, p.partition()))));
            try (var semilla = new KafkaConsumer<String, String>(config)) {
                semilla.assign(particiones);
                semilla.seekToEnd(particiones);
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                particiones.forEach(p -> offsets.put(p, new OffsetAndMetadata(semilla.position(p, Duration.ofSeconds(10)))));
                semilla.commitSync(offsets);
            }
            container.start();
            esperar(() -> container.getAssignedPartitions() != null && container.getAssignedPartitions().size() == 6);
            String malo = "{fallo-dlt-" + UUID.randomUUID();
            var enviado = productor.send(new ProducerRecord<>("audit.timeline", 0, "fallo-dlt", malo)).get(10, TimeUnit.SECONDS);
            var particion = new TopicPartition("audit.timeline", 0);
            esperar(() -> fallos.get() > 0);
            assertTrue(offset(admin, particion) <= enviado.getRecordMetadata().offset(), "No confirmar el registro que no llegó al DLT");
            admin.createTopics(List.of(new NewTopic(DLT, 3, (short) 3).configs(Map.of("min.insync.replicas", "2"))))
                    .all().get(15, TimeUnit.SECONDS);
            creado = true;
            esperar(() -> offset(admin, particion) > enviado.getRecordMetadata().offset());
            try (var lector = new KafkaConsumer<String, String>(config)) {
                lector.assign(List.of(new TopicPartition(DLT, 0)));
                lector.seekToBeginning(List.of(new TopicPartition(DLT, 0)));
                boolean encontrado = false;
                long limite = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                while (!encontrado && System.nanoTime() < limite) {
                    for (var r : lector.poll(Duration.ofMillis(200))) if (malo.equals(r.value())) encontrado = true;
                }
                assertTrue(encontrado);
            }
            System.out.println("DLT_REAL OK: offset detenido ante fallo y confirmado al recuperar el DLT");
        } finally {
            container.stop();
            try (var admin = Admin.create(config)) {
                if (creado) admin.deleteTopics(List.of(DLT)).all().get(15, TimeUnit.SECONDS);
                admin.deleteConsumerGroups(List.of(GRUPO)).all().get(15, TimeUnit.SECONDS);
            }
        }
    }

    private static long offset(Admin admin, TopicPartition p) {
        try { return admin.listConsumerGroupOffsets(GRUPO).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS).get(p).offset(); }
        catch (Exception e) { throw new IllegalStateException(e); }
    }
    private static void esperar(BooleanSupplier condicion) throws Exception {
        long limite = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < limite) { if (condicion.getAsBoolean()) return; Thread.sleep(100); }
        fail("Timeout esperando recuperación/offset del consumidor real");
    }
}
