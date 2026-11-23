package cl.digitalfix.audit;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;

import cl.digitalfix.audit.messaging.kafka.ContratoEventos.EventoOrden;
import cl.digitalfix.audit.messaging.kafka.ContratoEventos.SobreEvento;
import cl.digitalfix.audit.repository.EventoAuditoriaRepository;
import cl.digitalfix.audit.service.AuditoriaService;

import static org.junit.jupiter.api.Assertions.*;

// H2 real: las restricciones y las transacciones no se sustituyen por mocks.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class IntegridadAuditoriaTests {
    @Autowired EventoAuditoriaRepository repositorio;
    @Autowired AuditoriaService servicio;

    @BeforeEach void limpiar() { repositorio.deleteAll(); }

    @Test void reentregaDeEventoPersistidoNoDuplicaFilas() {
        var evento = evento("22");
        assertTrue(servicio.registrar(evento, 0, 0));
        assertFalse(servicio.registrar(evento, 0, 0));
        assertEquals(1, repositorio.count());
    }

    @Test void errorDeIntegridadDistintoDeIdentidadSePropaga() {
        // Supera el tamaño de la columna: no existe un eventId duplicado que permita ignorarlo.
        var evento = evento("x".repeat(101));
        assertThrows(DataIntegrityViolationException.class, () -> servicio.registrar(evento, 0, 0));
        assertFalse(repositorio.existsByEventId(evento.eventId().toString()));
        assertEquals(0, repositorio.count());
    }

    @Test void dosInsertsConcurrentesResuelvenLaColisionComoUnSoloEvento() throws Exception {
        var barrera = new CyclicBarrier(2);
        var consultas = new AtomicInteger();
        var colisiones = new AtomicInteger();
        // Solo sincroniza las primeras consultas; todos los accesos se ejecutan contra JPA/H2.
        var coordinado = (EventoAuditoriaRepository) Proxy.newProxyInstance(
                EventoAuditoriaRepository.class.getClassLoader(), new Class<?>[]{EventoAuditoriaRepository.class},
                (proxy, metodo, argumentos) -> {
                    try {
                        var resultado = metodo.invoke(repositorio, argumentos);
                        if (metodo.getName().equals("existsByEventId") && consultas.getAndIncrement() < 2) {
                            assertEquals(false, resultado);
                            barrera.await(10, TimeUnit.SECONDS);
                        }
                        return resultado;
                    } catch (InvocationTargetException e) {
                        if (e.getCause() instanceof DataIntegrityViolationException) colisiones.incrementAndGet();
                        throw e.getCause();
                    }
                });
        var concurrente = new AuditoriaService(coordinado);
        var evento = evento("22");
        try (var hilos = Executors.newFixedThreadPool(2)) {
            var primero = hilos.submit(() -> concurrente.registrar(evento, 0, 0));
            var segundo = hilos.submit(() -> concurrente.registrar(evento, 0, 0));
            boolean a = primero.get(20, TimeUnit.SECONDS);
            boolean b = segundo.get(20, TimeUnit.SECONDS);
            assertNotEquals(a, b, "Solo un insert debe resultar nuevo");
        }
        assertEquals(1, colisiones.get(), "Debe ejercitarse la restricción única, no solo la consulta previa");
        assertEquals(1, repositorio.count());
    }

    private static SobreEvento evento(String correlationId) {
        var fecha = Instant.parse("2026-11-23T15:00:00Z");
        return new SobreEvento("1.0", "ORDEN_CREADA", UUID.randomUUID(), fecha,
                UUID.randomUUID(), correlationId,
                new EventoOrden(22L, "CREACION", null, "CREADA", null, "oid-prueba", "Admin", fecha));
    }
}
