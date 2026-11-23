package cl.digitalfix.audit.controller;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import cl.digitalfix.audit.dto.response.EventoAuditoriaResponse;
import cl.digitalfix.audit.dto.response.PaginaResponse;
import cl.digitalfix.audit.service.AuditoriaService;

// API de SOLO LECTURA para el Auditor (y el Admin). No hay POST, PUT ni DELETE:
// la auditoría solo se escribe desde Kafka.
//
// Igual que Workorders: todo exige un JWT válido de Entra (autoconfiguración de Spring)
// y aquí se revisa el claim "roles".
@RestController
@RequestMapping("/api/audit")
public class AuditoriaController {

    static final Set<String> ROLES_LECTURA = Set.of("Admin", "Auditor");

    private final AuditoriaService servicio;

    public AuditoriaController(AuditoriaService servicio) {
        this.servicio = servicio;
    }

    // Timeline de una orden: GET /api/audit/ordenes/22/timeline
    // Una orden sin eventos devuelve una lista vacía.
    @GetMapping("/ordenes/{ordenId}/timeline")
    public List<EventoAuditoriaResponse> timeline(@PathVariable Long ordenId, JwtAuthenticationToken autenticacion) {
        exigirRolLectura(autenticacion);
        return servicio.timeline(ordenId);
    }

    // Búsqueda: GET /api/audit/eventos?actorId=...&tipo=ORDEN_ESTADO_CAMBIADO&desde=2026-11-01T00:00:00Z&pagina=0&tamano=20
    // Todos los filtros son opcionales. Resultado del más reciente al más antiguo.
    @GetMapping("/eventos")
    public PaginaResponse<EventoAuditoriaResponse> buscar(
            @RequestParam(required = false) Long ordenId,
            @RequestParam(required = false) String actorId,
            @RequestParam(required = false) String tipo,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant desde,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant hasta,
            @RequestParam(defaultValue = "0") int pagina,
            @RequestParam(defaultValue = "20") int tamano,
            JwtAuthenticationToken autenticacion) {

        exigirRolLectura(autenticacion);
        if (pagina < 0 || tamano < 1 || tamano > AuditoriaService.TAMANO_MAXIMO)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "pagina >= 0 y tamano entre 1 y " + AuditoriaService.TAMANO_MAXIMO);
        if (desde != null && hasta != null && desde.isAfter(hasta))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "desde no puede ser posterior a hasta");
        return servicio.buscar(ordenId, actorId, tipo, desde, hasta, pagina, tamano);
    }

    // 401 sin token, 403 si el token no trae Admin ni Auditor en "roles".
    private void exigirRolLectura(JwtAuthenticationToken autenticacion) {
        if (autenticacion == null || !autenticacion.isAuthenticated())
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
        var roles = autenticacion.getToken().getClaimAsStringList("roles");
        if (roles == null || roles.stream().noneMatch(ROLES_LECTURA::contains))
            throw new ResponseStatusException(HttpStatus.FORBIDDEN);
    }
}
