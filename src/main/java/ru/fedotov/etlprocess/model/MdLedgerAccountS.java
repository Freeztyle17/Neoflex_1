package ru.fedotov.etlprocess.model;

import jakarta.persistence.*;
import lombok.*;

import java.sql.Date;

@Entity
@Table(name = "md_ledger_account_s", schema = "raw")
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class MdLedgerAccountS {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "chapter")
    private String chapter;
    @Column(name = "chapter_name")
    private String chapterName;
    @Column(name = "section_number")
    private Long sectionNumber;
    @Column(name = "section_name")
    private String sectionName;
    @Column(name = "subsection_name")
    private String subsectionName;
    @Column(name = "ledger1_account")
    private Long ledger1Account;
    @Column(name = "ledger1_account_name")
    private String ledger1AccountName;
    @Column(name = "ledger_account")
    private Long ledgerAccount;
    @Column(name = "ledger_account_name")
    private String ledgerAccountName;
    @Column(name = "characteristic")
    private String characteristic;
    @Column(name = "is_resident")
    private int isResident;
    @Column(name = "is_reserve")
    private int isReserve;
    @Column(name = "is_reserved")
    private int isReserved;
    @Column(name = "is_loan")
    private int isLoan;
    @Column(name = "is_reserved_assets")
    private int isReservedAssets;
    @Column(name = "is_overdue")
    private int isOverdue;
    @Column(name = "is_interest")
    private int isInterest;
    @Column(name = "pair_account")
    private String pairAccount;
    @Column(name = "start_date")
    private Date startDate;
    @Column(name = "end_date")
    private Date endDate;
    @Column(name = "is_rub_only")
    private int isRubOnly;
    @Column(name = "min_term")
    private String minTerm;
    @Column(name = "min_term_measure")
    private String minTermMeasure;
    @Column(name = "max_term")
    private String maxTerm;
    @Column(name = "max_term_measure")
    private String maxTermMeasure;
    @Column(name = "ledger_acc_full_name_translit")
    private String ledgerAccFullNameTranslit;
    @Column(name = "is_revaluation")
    private String isRevaluation;
    @Column(name = "is_correct")
    private String isCorrect;
}
