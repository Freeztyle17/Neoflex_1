package ru.fedotov.etlprocess.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.fedotov.etlprocess.model.PostingSummary;

import java.util.Date;
import java.util.List;

@Repository
public interface PostingSummaryRepository extends JpaRepository<PostingSummary, Date> {

    @Query(nativeQuery = true, value = "SELECT * FROM ds.get_credit_debet_summary(:date)")
    List<PostingSummary> getCreditDebitSummary(@Param("date") Date date);

}
