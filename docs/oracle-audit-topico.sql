-- Preparación para ejecución manual autorizada. No se ejecuta al iniciar la aplicación.
-- Revisar propietario y respaldo: Oracle DDL confirma implícitamente.
-- La tabla previa solo consumía workorders.events; conservar valores que ya tengan origen.
DECLARE
    cantidad NUMBER;
    tipo VARCHAR2(128);
    longitud NUMBER;
BEGIN
    SELECT COUNT(*) INTO cantidad FROM USER_TABLES WHERE TABLE_NAME = 'AUDITORIA_EVENTOS';
    IF cantidad = 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Crear primero la tabla AUDITORIA_EVENTOS en el entorno autorizado');
    END IF;
    SELECT COUNT(*) INTO cantidad FROM USER_TAB_COLUMNS
        WHERE TABLE_NAME = 'AUDITORIA_EVENTOS' AND COLUMN_NAME = 'KAFKA_TOPICO';
    IF cantidad = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE auditoria_eventos ADD kafka_topico VARCHAR2(249 CHAR)';
    END IF;
    SELECT DATA_TYPE, CHAR_LENGTH INTO tipo, longitud FROM USER_TAB_COLUMNS
        WHERE TABLE_NAME = 'AUDITORIA_EVENTOS' AND COLUMN_NAME = 'KAFKA_TOPICO';
    IF tipo <> 'VARCHAR2' OR longitud < 249 THEN
        RAISE_APPLICATION_ERROR(-20002, 'KAFKA_TOPICO existente es incompatible; revisar manualmente');
    END IF;
END;
/

UPDATE auditoria_eventos SET kafka_topico = 'workorders.events' WHERE kafka_topico IS NULL;
COMMIT;
