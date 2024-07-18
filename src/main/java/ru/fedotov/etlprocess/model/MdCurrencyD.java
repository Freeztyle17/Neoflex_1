package ru.fedotov.etlprocess.model;

import jakarta.persistence.*;
import lombok.*;

import java.sql.Date;

@Entity
@Table(name = "md_currency_d", schema = "raw")
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class MdCurrencyD {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "currency_rk")
    private Long currencyRk;
    @Column(name = "data_actual_date")
    private Date dataActualDate;
    @Column(name = "data_actual_end_date")
    private Date dataActualEndDate;
    @Column(name = "currency_code")
    private String currencyCode;
    @Column(name = "code_iso_char")
    private String codeIsoChar;

}
