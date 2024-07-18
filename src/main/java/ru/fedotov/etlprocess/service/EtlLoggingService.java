package ru.fedotov.etlprocess.service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.fedotov.etlprocess.model.EtlLog;
import ru.fedotov.etlprocess.repository.EtlLogRepository;

import java.sql.Timestamp;
import java.time.LocalDateTime;

@Service
public class EtlLoggingService {

    @PersistenceContext
    private EntityManager entityManager;

    void logProcessStart(String processName) {
        Timestamp startTime = new Timestamp(System.currentTimeMillis());
        entityManager.createNativeQuery(
                        "INSERT INTO logs.etl_logs (process_name, oper_time, status) VALUES (?, ?, 'STARTED')")
                .setParameter(1, processName)
                .setParameter(2, startTime)
                .executeUpdate();
    }

    void logProcessEnd(String processName, String status) {
        Timestamp endTime = new Timestamp(System.currentTimeMillis());
        entityManager.createNativeQuery(
                        "INSERT INTO logs.etl_logs (process_name, oper_time, status) VALUES (?, ?, ?)")
                .setParameter(1, processName)
                .setParameter(2, endTime)
                .setParameter(3, status)
                .executeUpdate();
    }

    void logProcessError(String processName, String errorMessage) {
        entityManager.createNativeQuery(
                        "INSERT INTO logs.etl_logs (process_name, oper_time, status) VALUES (?, NOW(), ?)")
                .setParameter(1, processName)
                .setParameter(2, "FAILED: " + errorMessage)
                .executeUpdate();
    }


}
