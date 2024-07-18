package ru.fedotov.etlprocess.model;

import jakarta.persistence.*;
import lombok.*;

import java.sql.Date;

@Entity
@Table(name = "ft_posting_f", schema = "raw")
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class FtPostingF {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "oper_date")
    private Date operDate;
    @Column(name = "credit_account_rk")
    private Long creditAccountRk;
    @Column(name = "debet_account_rk")
    private Long debetAccountRk;
    @Column(name = "credit_amount")
    private Double creditAmount;
    @Column(name = "debet_amount")
    private Double debetAmount;
}
