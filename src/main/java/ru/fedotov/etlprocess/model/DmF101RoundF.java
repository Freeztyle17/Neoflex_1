package ru.fedotov.etlprocess.model;

import jakarta.persistence.*;
import lombok.*;

import java.util.Date;

@Entity
@Table(name = "dm_f101_round_f")
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class DmF101RoundF {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long Id;

    @Column(name="from_date")
    private Date fromDate ;
    @Column(name="to_date")
    private Date toDate;
    @Column(name="chapter")
    private String chapter;
    @Column(name="ledger_account")
    private String ledgerAccount;
    @Column(name="characteristic")
    private String characteristic;
    @Column(name="balance_in_rub")
    private Double balanceInRub;
    @Column(name="r_balance_in_rub")
    private Double rBalanceInRub;
    @Column(name="balance_in_val")
    private Double balanceInVal;
    @Column(name="r_balance_in_val")
    private Double rBalanceInVal;
    @Column(name="balance_in_total")
    private Double balanceInTotal;
    @Column(name="r_balance_in_total")
    private Double rBalanceInTotal;
    @Column(name="turn_deb_rub")
    private Double turnDebRub;
    @Column(name="r_turn_deb_rub")
    private Double rTurnDebRub;
    @Column(name="turn_deb_val")
    private Double turnDebVal;
    @Column(name="r_turn_deb_val")
    private Double rTurnDebVal;
    @Column(name="turn_deb_total")
    private Double turnDebTotal;
    @Column(name="r_turn_deb_total")
    private Double rTurnDebTotal;
    @Column(name="turn_cre_rub")
    private Double turnCreRub;
    @Column(name="r_turn_cre_rub")
    private Double rTurnCreRub;
    @Column(name="turn_cre_val")
    private Double turnCreVal;
    @Column(name="rTurnCreVal")
    private Double rTurnCreVal;
    @Column(name="turn_cre_total")
    private Double turnCreTotal;
    @Column(name="r_turn_cre_total")
    private Double rTurnCreTotal;
    @Column(name="balance_out_rub")
    private Double balanceOutRub;
    @Column(name="r_balance_out_rub")
    private Double rBalanceOutRub;
    @Column(name="balance_out_val")
    private Double balanceOutVal;
    @Column(name="r_balance_out_val")
    private Double rBalanceOutVal;
    @Column(name="balance_out_total")
    private Double balanceOutTotal;
    @Column(name="r_balance_out_total")
    private Double rBalanceOutTotal;

}
