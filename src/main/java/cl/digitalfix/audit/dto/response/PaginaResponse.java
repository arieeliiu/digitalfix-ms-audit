package cl.digitalfix.audit.dto.response;

import java.util.List;

// Una página de resultados de la búsqueda.
public record PaginaResponse<T>(List<T> contenido, int pagina, int tamano, long total) {}
