package ru.fedotov.etlprocess.model;

import jakarta.persistence.*;
import lombok.*;

import java.sql.Date;

@Entity
@Table(name = "md_account_d", schema = "raw")
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class MdAccountD {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "data_actual_date")
    private Date dataActualDate;
    @Column(name = "data_actual_end_date")
    private Date dataActualEndDate;
    @Column(name = "account_rk")
    private Long accountRk;
    @Column(name = "account_number")
    private String accountNumber;
    @Column(name = "char_type")
    private String charType;
    @Column(name = "currency_rk")
    private Long currencyRk;
    @Column(name = "currency_code")
    private String currencyCode;

}
