package cl.digitalfix.audit;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import cl.digitalfix.audit.controller.AuditoriaController;
import cl.digitalfix.audit.messaging.kafka.ConsumidorEventosOrden;
import cl.digitalfix.audit.repository.EventoAuditoriaRepository;
import cl.digitalfix.audit.service.AuditoriaService;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

// Permisos y respuestas HTTP de /api/audit (mismo estilo que los tests de Workorders).
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class AuditoriaControllerTests {
    @Autowired AuditoriaService servicio;
    @Autowired ConsumidorEventosOrden consumidor;
    @Autowired EventoAuditoriaRepository repositorio;
    MockMvc mvc;

    @BeforeEach void preparar() {
        repositorio.deleteAll();
        mvc = MockMvcBuilders.standaloneSetup(new AuditoriaController(servicio)).build();
        consumidor.recibir(ConsumidorEventosOrdenTests.creada(UUID.randomUUID(), 22, "2026-11-23T03:40:49Z"),
                0, 0, "22", null);
    }

    JwtAuthenticationToken token(String... roles) {
        var jwt = Jwt.withTokenValue("local").header("alg", "RS256")
                .claim("oid", "oid-usuario").claim("roles", List.of(roles)).build();
        return new JwtAuthenticationToken(jwt, List.of());
    }

    @Test void auditorVeElTimeline() throws Exception {
        mvc.perform(get("/api/audit/ordenes/22/timeline").principal(token("Auditor")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].tipo").value("ORDEN_CREADA"))
                .andExpect(jsonPath("$[0].estadoNuevo").value("CREADA"))
                .andExpect(jsonPath("$[0].kafkaOffset").value(0));
    }

    @Test void adminTambienPuedeYOrdenSinEventosDevuelveListaVacia() throws Exception {
        mvc.perform(get("/api/audit/ordenes/999/timeline").principal(token("Admin")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test void sinTokenEs401() throws Exception {
        mvc.perform(get("/api/audit/ordenes/22/timeline")).andExpect(status().isUnauthorized());
    }

    @Test void clienteYOperadorSon403() throws Exception {
        mvc.perform(get("/api/audit/ordenes/22/timeline").principal(token("Cliente"))).andExpect(status().isForbidden());
        mvc.perform(get("/api/audit/eventos").principal(token("Operador"))).andExpect(status().isForbidden());
    }

    @Test void busquedaConFiltrosYPaginacion() throws Exception {
        mvc.perform(get("/api/audit/eventos").param("tipo", "ORDEN_CREADA").param("tamano", "5")
                        .principal(token("Auditor")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(1))
                .andExpect(jsonPath("$.tamano").value(5))
                .andExpect(jsonPath("$.contenido[0].ordenId").value(22));
    }

    @Test void parametrosInvalidosSon400() throws Exception {
        mvc.perform(get("/api/audit/eventos").param("tamano", "500").principal(token("Auditor")))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/api/audit/eventos").param("desde", "2026-11-30T00:00:00Z")
                        .param("hasta", "2026-11-01T00:00:00Z").principal(token("Auditor")))
                .andExpect(status().isBadRequest());
    }
}
