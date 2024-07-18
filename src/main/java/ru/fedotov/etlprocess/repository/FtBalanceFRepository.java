package ru.fedotov.etlprocess.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.fedotov.etlprocess.model.FtBalanceF;

@Repository
public interface FtBalanceFRepository extends JpaRepository<FtBalanceF, Long> {
}
