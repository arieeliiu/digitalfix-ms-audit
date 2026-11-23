package cl.digitalfix.audit.config;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.SendResult;

import cl.digitalfix.audit.messaging.kafka.EventoInvalido;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

// Comportamiento del manejador de errores, con un KafkaTemplate simulado (sin broker).
class KafkaConsumerConfigTests {
    static final String DLT = "workorders.events.audit.DLT";
    ConsumerRecord<String, String> registro = new ConsumerRecord<>("workorders.events", 2, 15L, "22", "{malo");

    @SuppressWarnings("unchecked")
    KafkaTemplate<String, String> template() {
        KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        when(template.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
        var fabrica = mock(org.springframework.kafka.core.ProducerFactory.class);
        when(fabrica.getConfigurationProperties()).thenReturn(java.util.Map.of("delivery.timeout.ms", 1000));
        when(template.getProducerFactory()).thenReturn(fabrica);
        return template;
    }

    // El recoverer pregunta al consumidor si la partición existe en el DLT; si no, deja que Kafka elija.
    // El DLT real tiene 3 particiones (digitalfix-infra/kafka).
    @SuppressWarnings("unchecked")
    Consumer<String, String> consumidor() {
        Consumer<String, String> consumidor = mock(Consumer.class);
        var particiones = java.util.stream.IntStream.range(0, 3)
                .mapToObj(p -> new org.apache.kafka.common.PartitionInfo(DLT, p, null, null, null)).toList();
        when(consumidor.partitionsFor(eq(DLT), any(Duration.class))).thenReturn(particiones);
        when(consumidor.partitionsFor(DLT)).thenReturn(particiones);
        return consumidor;
    }

    @Test void elDltEsElDeAuditYMantieneLaParticion() {
        var destino = KafkaConsumerConfig.destinoDlt(DLT, registro);
        assertEquals(DLT, destino.topic());
        assertEquals(2, destino.partition());
    }

    @Test void cadaOrigenTieneSuDlt() {
        var edicion = new ConsumerRecord<String, String>("audit.timeline", 1, 0, "22", "{malo");
        assertEquals(new org.apache.kafka.common.TopicPartition("audit.timeline.audit.DLT", 1),
                KafkaConsumerConfig.destinoDlt(DLT, edicion));
    }

    @SuppressWarnings("unchecked")
    @Test void dltSinConfirmacionNoSeConsideraRecuperado() {
        var template = template();
        when(template.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("DLT no disponible")));
        var manejador = new KafkaConsumerConfig().manejadorErrores(template, DLT, 0, Duration.ofMillis(1));
        assertFalse(manejador.handleOne(new EventoInvalido("json_invalido"), registro,
                consumidor(), mock(MessageListenerContainer.class)));
    }

    @SuppressWarnings("unchecked")
    @Test void eventoInvalidoVaDirectoAlDltSinReintentos() {
        var template = template();
        var manejador = new KafkaConsumerConfig().manejadorErrores(template, DLT, 3, Duration.ofMillis(1));

        boolean recuperado = manejador.handleOne(new EventoInvalido("json_invalido"), registro,
                consumidor(), mock(MessageListenerContainer.class));

        assertTrue(recuperado, "un error permanente se resuelve en el primer intento");
        var enviado = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(enviado.capture());
        assertEquals(DLT, enviado.getValue().topic());
        assertEquals(2, enviado.getValue().partition());
        assertEquals("{malo", enviado.getValue().value());
    }

    @SuppressWarnings("unchecked")
    @Test void errorTemporalSeReintentaAntesDeIrAlDlt() {
        var template = template();
        var manejador = new KafkaConsumerConfig().manejadorErrores(template, DLT, 2, Duration.ofMillis(1));
        var oracleCaido = new IllegalStateException("oracle caido");

        // Intento 1 y 2 fallan: se reintenta, todavía no va al DLT
        assertFalse(manejador.handleOne(oracleCaido, registro, consumidor(), mock(MessageListenerContainer.class)));
        assertFalse(manejador.handleOne(oracleCaido, registro, consumidor(), mock(MessageListenerContainer.class)));
        verify(template, never()).send(any(ProducerRecord.class));

        // Intento 3 (agotados los 2 reintentos): ahora sí va al DLT
        assertTrue(manejador.handleOne(oracleCaido, registro, consumidor(), mock(MessageListenerContainer.class)));
        verify(template).send(any(ProducerRecord.class));
    }
}
