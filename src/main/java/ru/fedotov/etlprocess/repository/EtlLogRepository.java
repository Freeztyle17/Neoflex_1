package ru.fedotov.etlprocess.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.fedotov.etlprocess.model.EtlLog;
@Repository
public interface EtlLogRepository extends JpaRepository<EtlLog, Long> {
}