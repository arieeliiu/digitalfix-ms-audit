package cl.digitalfix.audit.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import cl.digitalfix.audit.messaging.kafka.EventoInvalido;

// Configuración del consumidor Kafka de ms-audit.
//
//  consumerFactory            -> cómo se conecta y lee (grupo, deserializers, desde dónde empezar)
//  kafkaTemplateDlt           -> productor que solo se usa para enviar mensajes fallidos al DLT
//  manejadorErrores           -> qué hacer cuando el listener lanza una excepción (reintentar o DLT)
//  kafkaListenerContainerFactory -> une todo lo anterior y lo usa el @KafkaListener
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Bean
    ConsumerFactory<String, String> consumerFactory(
            @Value("${digitalfix.kafka.bootstrap-servers}") String servidores) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidores);
        // Key = ordenId y value = JSON del Sobre, ambos como texto (igual que el productor).
        // StringDeserializer no falla con bytes "raros", así que los errores de formato aparecen
        // al convertir el JSON en ConsumidorEventosOrden, donde sí los controlamos.
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Grupo nuevo sin offset guardado: empezar desde el PRIMER mensaje que siga en el tópico.
        // Así, la primera vez que arranca ms-audit, recupera los eventos de los últimos 7 días.
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // No hacer commit automático por tiempo: el commit lo hace el contenedor
        // DESPUÉS de que el listener terminó bien (ver AckMode.RECORD más abajo).
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    ProducerFactory<String, String> productorDlt(
            @Value("${digitalfix.kafka.bootstrap-servers}") String servidores) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servidores);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // El mensaje fallido no se puede perder: confirmación del leader y de todos los ISR
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    KafkaTemplate<String, String> kafkaTemplateDlt(ProducerFactory<String, String> productorDlt) {
        return new KafkaTemplate<>(productorDlt);
    }

    // Qué hacer cuando ConsumidorEventosOrden lanza una excepción.
    //
    //  - Error temporal (ej. Oracle caído): reintentar "reintentos" veces, esperando
    //    "espera" entre intentos (backoff fijo, para no saturar la base con reintentos seguidos).
    //  - Error permanente (EventoInvalido): sin reintentos, directo al DLT.
    //  - Agotados los reintentos: al DLT. Así un mensaje malo no bloquea a los que vienen
    //    detrás en la misma partición.
    //
    // DeadLetterPublishingRecoverer agrega headers con la excepción y el tópico/partición/offset
    // de origen, para poder investigar el fallo y reinyectar el mensaje después.
    @Bean
    DefaultErrorHandler manejadorErrores(
            KafkaTemplate<String, String> kafkaTemplateDlt,
            @Value("${digitalfix.kafka.topico-dlt}") String topicoDlt,
            @Value("${digitalfix.kafka.reintentos}") long reintentos,
            @Value("${digitalfix.kafka.espera-reintento}") Duration espera) {

        // Por defecto Spring enviaría a "<tópico>.DLT" (workorders.events.DLT), compartido con ms-report.
        // Cada consumidor tiene su propio DLT, así que indicamos el destino: misma partición de origen.
        var recuperador = new DeadLetterPublishingRecoverer(kafkaTemplateDlt,
                (ConsumerRecord<?, ?> registro, Exception error) -> destinoDlt(topicoDlt, registro));

        var manejador = new DefaultErrorHandler(recuperador, new FixedBackOff(espera.toMillis(), reintentos));
        manejador.addNotRetryableExceptions(EventoInvalido.class);
        return manejador;
    }

    // Separado para poder probarlo: el DLT tiene 3 particiones, igual que el tópico de origen.
    static TopicPartition destinoDlt(String topicoDlt, ConsumerRecord<?, ?> registro) {
        return new TopicPartition(topicoDlt, registro.partition());
    }

    // El @KafkaListener usa este bean por su nombre ("kafkaListenerContainerFactory").
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler manejadorErrores,
            @Value("${digitalfix.kafka.enabled}") boolean habilitado) {
        var fabrica = new ConcurrentKafkaListenerContainerFactory<String, String>();
        fabrica.setConsumerFactory(consumerFactory);
        fabrica.setCommonErrorHandler(manejadorErrores);
        // Commit del offset después de CADA mensaje procesado sin error
        fabrica.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        // Con digitalfix.kafka.enabled=false el listener no arranca (tests sin broker)
        fabrica.setAutoStartup(habilitado);
        return fabrica;
    }
}
