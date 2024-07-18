package ru.fedotov.etlprocess.model;

import jakarta.persistence.*;
import lombok.*;

import java.sql.Date;

@Entity
@Table(name = "md_exchange_rate_d", schema = "raw")
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class MdExchangeRateD {


    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;


    @Column(name = "data_actual_date")
    private Date dataActualDate;
    @Column(name = "data_actual_end_date")
    private Date dataActualEndDate;
    @Column(name = "currency_rk")
    private Long currencyRk;
    @Column(name = "reduced_cource")
    private Double reduced_cource;
    @Column(name = "code_iso_num")
    private String codeIsoChar;

}
