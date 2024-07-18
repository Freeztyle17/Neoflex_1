package ru.fedotov.etlprocess.Controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.fedotov.etlprocess.service.CsvFileService;
import ru.fedotov.etlprocess.service.EtlService;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@RestController
@RequestMapping("/api")
public class EtlController {

    @Autowired
    private EtlService etlService;

    @Autowired
    private CsvFileService csvFileService;

    @PostMapping("/test")
    public ResponseEntity loadFtBalanceF() {
        try {
            List<String> lst = new ArrayList<>();
            lst.add("ft_balance_f");
            lst.add("ft_posting_f");
            lst.add("md_account_d");
            lst.add("md_currency_d");
            lst.add("md_exchange_rate_d");
            lst.add("md_ledger_account_s");

            etlService.loadCsvData();
            Thread.sleep(5000);
            etlService.transferData(lst);
            return ResponseEntity.ok("Данные успешно загружены.");
        } catch (IOException e) {
            return ResponseEntity.badRequest().body("Ошибка при загрузке данных: " + e.getMessage());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    @GetMapping("/readCsv/{filename}")
    public String readCsvFile(@PathVariable String filename) {
        csvFileService.readCsvFile(filename);
        return  "CSV file read successfully";
    }

    @GetMapping("/export")
    public String exportData(@RequestParam String tableName,@RequestParam String schemaName) {
        csvFileService.exportDataToCSV(tableName, schemaName);
        return "Data exported successfully!";
    }

    @GetMapping("/import")
    public String importData(@RequestParam String tableName, @RequestParam String schemaName) {
        csvFileService.importDataFromCSV(tableName, schemaName);
        return "Data imported successfully!";
    }

    @GetMapping("/savePostingSummary")
    public String savePostingSummary(@RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") Date date) {
        try {
            csvFileService.savePostingSummaryToCsv(date);
            return "Posting summary saved successfully.";
        } catch (IOException e) {
            e.printStackTrace();
            return "Error saving posting summary.";
        }
    }
}
