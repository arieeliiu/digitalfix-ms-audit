package cl.digitalfix.audit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// ms-audit: lee los eventos de las órdenes desde Kafka (workorders.events),
// los guarda en Oracle y los muestra por /api/audit (solo lectura, Admin y Auditor).
@SpringBootApplication
public class AuditApplication {
    public static void main(String[] args) {
        SpringApplication.run(AuditApplication.class, args);
    }
}
