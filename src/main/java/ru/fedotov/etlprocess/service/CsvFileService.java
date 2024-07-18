package ru.fedotov.etlprocess.service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import jakarta.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import ru.fedotov.etlprocess.model.PostingSummary;
import ru.fedotov.etlprocess.repository.PostingSummaryRepository;

import java.io.*;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


@Service
public class CsvFileService {

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private EntityManager entityManager;

    @Autowired
    private PostingSummaryRepository repository;

    @Autowired
    EtlLoggingService etlLoggingService;

    public void readCsvFile(String filename) {
        try {
            Resource resource = resourceLoader.getResource("classpath:/csvfiles/"+filename+".csv");

            BufferedReader br = new BufferedReader(new InputStreamReader(resource.getInputStream()));
            String line;
            while((line = br.readLine()) != null) {
                System.out.println(line);
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void exportDataToCSV(String tableName, String schemaName) {
        // Получение названий колонок из базы данных
        String filePath = "src/main/resources/export/"+tableName+".csv";
        String columnsHeader = getColumnNames(tableName, schemaName);

        try (FileWriter writer = new FileWriter(filePath)) {


            writer.append(columnsHeader);
            writer.append('\n');

            Query query = entityManager.createNativeQuery("SELECT * FROM "+schemaName+ "." + tableName);
            List<Object[]> rows = query.getResultList();

            for (Object[] row : rows) {
                for (int i = 0; i < row.length; i++) {

                    if (row[i] != null) {
                        writer.append(row[i].toString());
                    }

                    if (i != row.length - 1) {
                        writer.append(';');
                    }
                }
                writer.append('\n');
            }


        } catch (IOException e) {
            e.printStackTrace();
            ;
        }
    }

    @Transactional
    public void importDataFromCSV(String tableName, String schemaName) {
        String filePath = "src/main/resources/import/" + tableName + ".csv";

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {

            etlLoggingService.logProcessStart("Import data to CSV");

            String line;
            boolean headerSkipped = false; // флаг для пропуска первой строки (заголовка)

            while ((line = reader.readLine()) != null) {
                if (!headerSkipped) {
                    headerSkipped = true;
                    continue; // пропускаем первую строку (заголовок)
                }

                String[] columns = line.split(";");

                for (int i = 0; i < columns.length; i++) {
                    if (columns[i].isEmpty()) {
                        columns[i] = null; // заменяем пустую строку на null
                    }
                }

                // Формируем SQL-запрос для вставки данных
                StringBuilder sqlBuilder = new StringBuilder();
                sqlBuilder.append("INSERT INTO ").append(schemaName).append(".").append(tableName).append(" VALUES (");

                for (int i = 0; i < columns.length; i++) {
                    if (i > 0) {
                        sqlBuilder.append(", ");
                    }
                    if (columns[i] == null) {
                        sqlBuilder.append("null"); // вставляем null, если значение равно null
                    } else if (isNumeric(columns[i])) {
                        sqlBuilder.append(columns[i]); // если числовое значение
                    } else {
                        sqlBuilder.append("'").append(columns[i]).append("'"); // если строковое значение
                    }
                }

                sqlBuilder.append(")");

                // Выполняем SQL-запрос
                entityManager.createNativeQuery(sqlBuilder.toString()).executeUpdate();
            }

            System.out.println("Данные успешно импортированы из CSV в таблицу " + tableName);
            etlLoggingService.logProcessEnd("Import data to CSV", "SUCCESS");
        } catch (IOException e) {
            etlLoggingService.logProcessError("Import data Failed ", e.getMessage());
            e.printStackTrace();
        }
    }

    // Метод для проверки, является ли строка числом
    private boolean isNumeric(String str) {
        if (str == null) {
            return false;
        }
        return str.matches("-?\\d+(\\.\\d+)?");
    }

    public void savePostingSummaryToCsv(Date date) throws IOException {
        List<PostingSummary> summaries = repository.getCreditDebitSummary(date);

        if (summaries.isEmpty()) {
            // Если список summaries пуст, можно выбросить исключение или выполнить другие действия
            throw new IllegalArgumentException("No data found for the given date: " + date);
        }

        // Получаем заголовки столбцов из первого элемента списка summaries
        PostingSummary firstSummary = summaries.get(0);
        Field[] fields = PostingSummary.class.getDeclaredFields();

        try (FileWriter writer = new FileWriter("src/main/resources/export/posting_summary.csv")) {
            // Write CSV header
            writer.append(String.join(";", getFieldNames(fields))).append("\n");

            // Write CSV data
            for (PostingSummary summary : summaries) {
                writer.append(String.join(";", getFieldValues(fields, summary))).append("\n");
            }

            writer.flush();
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
    private String[] getFieldNames(Field[] fields) {
        List<String> fieldNames = new ArrayList<>();
        for (Field field : fields) {
            fieldNames.add(field.getName());
        }
        return fieldNames.toArray(new String[0]);
    }

    // Вспомогательный метод для получения значений полей
    private String[] getFieldValues(Field[] fields, PostingSummary summary) throws IllegalAccessException {
        List<String> fieldValues = new ArrayList<>();
        for (Field field : fields) {
            field.setAccessible(true);
            Object value = field.get(summary);
            fieldValues.add(value != null ? value.toString() : "");
        }
        return fieldValues.toArray(new String[0]);
    }
    private String getColumnNames(String tableName, String schemaName) {
        Query query = entityManager.createNativeQuery(
                "SELECT column_name " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = :schemaName " +  // схема таблицы
                        "AND table_name = :tableName"); // название таблицы
        query.setParameter("schemaName", schemaName);
        query.setParameter("tableName", tableName);
        List<String> columns = query.getResultList();
        return String.join(",", columns);
    }


//    @Transactional
//    public void importDataFromCSV(String filePath) {
//        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
//            String line;
//            boolean isFirstLine = true;
//            while ((line = reader.readLine()) != null) {
//                if (isFirstLine) {
//                    isFirstLine = false;
//                    continue; // пропустить заголовок
//                }
//
//                String[] data = line.split(";");
//                // Пример: Предполагается, что порядок колонок в CSV соответствует таблице dm_f101_round_f_v2
//
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

}
