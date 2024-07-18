package ru.fedotov.etlprocess.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.time.LocalDateTime;
@Entity
@Table(name = "etl_log", schema="logs")
@RequiredArgsConstructor
@Getter
@Setter
@ToString
public class EtlLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "time_of_oper")
    private LocalDateTime timeOfOper;
    @Column(name = "object_of_oper")
    private String objectOfOper;
    @Column(name = "description")
    private String description;


}
