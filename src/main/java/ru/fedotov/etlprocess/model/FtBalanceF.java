package ru.fedotov.etlprocess.model;

import jakarta.persistence.*;
import lombok.*;

import java.util.Date;

@Entity
@Table(name = "ft_balance_f", schema = "raw")
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class FtBalanceF {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(name = "on_date")
    private Date onDate;
    @Column(name = "account_rk")
    private int accountRk;
    @Column(name = "currency_rk")
    private int currencyRk;
    @Column(name = "balance_out")
    private Double balanceOut;

}
