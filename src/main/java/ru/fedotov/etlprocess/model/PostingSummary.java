package ru.fedotov.etlprocess.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.Date;

@Entity
@Getter
@Setter
public class PostingSummary {

    @Id
    private Date date;
    private BigDecimal maxCredit;
    private BigDecimal minCredit;
    private BigDecimal maxDebet;
    private BigDecimal minDebet;

}
