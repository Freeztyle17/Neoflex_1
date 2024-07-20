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
        String filePath = "src/main/resources/export/"+tableName+".csv";
        String columnsHeader = getColumnNames(tableName, schemaName);
        etlLoggingService.logProcessStart("Export data to CSV");

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
            etlLoggingService.logProcessError("Export data to CSV", e.getMessage());;
        }

        etlLoggingService.logProcessEnd("Export data to CSV", "SUCCESS");
    }

    @Transactional
    public void importDataFromCSV(String tableName, String schemaName) throws ClassNotFoundException {
        String filePath = "src/main/resources/import/" + tableName + ".csv";
        etlLoggingService.logProcessStart("Import data to CSV");

        String versionedTableName = determineTableName(tableName);

        createTable(versionedTableName, tableName, schemaName);

        sendData(filePath, versionedTableName, schemaName);

        etlLoggingService.logProcessEnd("Import data to CSV", "SUCCESS");
    }


    private boolean isNumeric(String str) {
        if (str == null) {
            return false;
        }
        return str.matches("-?\\d+(\\.\\d+)?");
    }

    public void savePostingSummaryToCsv(Date date) throws IOException {
        List<PostingSummary> summaries = repository.getCreditDebitSummary(date);

        if (summaries.isEmpty()) {
            throw new IllegalArgumentException("No data found for the given date: " + date);
        }

        PostingSummary firstSummary = summaries.get(0);
        Field[] fields = PostingSummary.class.getDeclaredFields();

        try (FileWriter writer = new FileWriter("src/main/resources/export/posting_summary.csv")) {

            writer.append(String.join(";", getFieldNames(fields))).append("\n");


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
                        "WHERE table_schema = :schemaName " +
                        "AND table_name = :tableName");
        query.setParameter("schemaName", schemaName);
        query.setParameter("tableName", tableName);
        List<String> columns = query.getResultList();
        return String.join(";", columns);
    }

    public String convertTableNameToClassName(String tableName) {
        // Разбиваем имя таблицы по символу "_"
        String[] parts = tableName.split("_");

        // Строим результирующую строку
        StringBuilder result = new StringBuilder();
        for (String part : parts) {
            // Преобразуем первую букву к верхнему регистру и добавляем к результату
            if (!part.isEmpty()) {
                result.append(Character.toUpperCase(part.charAt(0)));
                if (part.length() > 1) {
                    result.append(part.substring(1).toLowerCase());
                }
            }
        }

        return result.toString();
    }

    private String determineTableName(String baseName) {

        String queryStr = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE :baseName";
        Query query = entityManager.createNativeQuery(queryStr);
        query.setParameter("baseName", baseName + "_v%");
        Long count = (Long) query.getSingleResult();

        return baseName + "_v" + (count + 2);
    }

    @Transactional
    private void createTable(String tableName, String originalTableName, String schemaName) {
        entityManager.createNativeQuery("CREATE TABLE IF NOT EXISTS " +schemaName +"."+ tableName + " AS SELECT * FROM " + schemaName +"."+ originalTableName+" WHERE 1 = 0")
                .executeUpdate();
    }

    private void sendData(String filePath, String versionedTableName, String schemaName){
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {

            String line;
            boolean headerSkipped = false;

            while ((line = reader.readLine()) != null) {
                if (!headerSkipped) {
                    headerSkipped = true;
                    continue;
                }

                String[] columns = line.split(";");

                for (int i = 0; i < columns.length; i++) {
                    if (columns[i].isEmpty()) {
                        columns[i] = null;
                    }
                }


                StringBuilder sqlBuilder = new StringBuilder();
                sqlBuilder.append("INSERT INTO ").append(schemaName).append(".").append(versionedTableName).append(" VALUES (");

                for (int i = 0; i < columns.length; i++) {
                    if (i > 0) {
                        sqlBuilder.append(", ");
                    }
                    if (columns[i] == null) {
                        sqlBuilder.append("null");
                    } else if (isNumeric(columns[i])) {
                        sqlBuilder.append(columns[i]);
                    } else {
                        sqlBuilder.append("'").append(columns[i]).append("'");
                    }
                }

                sqlBuilder.append(")");


                entityManager.createNativeQuery(sqlBuilder.toString()).executeUpdate();
            }

            System.out.println("Данные успешно импортированы из CSV в таблицу " + versionedTableName);

        } catch (IOException e) {
            etlLoggingService.logProcessError("Import data Failed ", e.getMessage());
            e.printStackTrace();
        }
    }
}
