package ru.fedotov.etlprocess.service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityTransaction;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import jakarta.transaction.Transactional;
import org.hibernate.SessionFactory;
import org.postgresql.util.PSQLException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.fedotov.etlprocess.model.FtBalanceF;
import ru.fedotov.etlprocess.repository.FtBalanceFRepository;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


@Service
public class EtlService {

    @Autowired
    private FtBalanceFRepository ftBalanceFRepository;
    @Autowired
    private ResourceLoader resourceLoader;
    @PersistenceContext
    private EntityManager entityManager;
    @Autowired
    private SessionFactory sessionFactory;
    @Autowired
    private EtlLoggingService etlLoggingService;

    @Transactional
    public void loadCsvData() throws IOException, ParseException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, InterruptedException {
        ResourcePatternResolver resourceResolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resourceResolver.getResources("classpath:csvfiles/*.csv");
        List<String> tableNames = new ArrayList<>();



        for (Resource resource : resources) {
            etlLoggingService.logProcessStart("Load CSV data to RAW from - " +resource.getFilename());

            String encoding = determineEncoding(resource.getFilename());
            BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream(), encoding));

            String line;
            reader.readLine();



            int dotIndex = resource.getFilename().lastIndexOf(".");
            if (dotIndex != -1) {
                tableNames.add(resource.getFilename().substring(0, dotIndex));
            }


            Class<?> entityClass = Class.forName("ru.fedotov.etlprocess.model." + convertFileNameToClassName(resource.getFilename())); // Замените 'com.yourpackage' на ваш пакет
            Constructor<?> constructor = entityClass.getConstructor();

            while ((line = reader.readLine()) != null) {
                String[] values = line.split(";"); // Разделитель в CSV-файле

                Object entity = constructor.newInstance();


                Field[] fields = entityClass.getDeclaredFields();
                for (int i = 0; i < fields.length; i++) {
                    Field field = fields[i];
                    field.setAccessible(true);
                    Class<?> fieldType = field.getType();

                    if (i < values.length) {

                            if (fieldType == int.class || fieldType == Integer.class) {
                                field.set(entity, Integer.parseInt(values[i]));
                            } else if (fieldType == Long.class) {
                                field.set(entity, Long.parseLong(values[i]));
                            } else if (fieldType == String.class) {
                                field.set(entity, values[i]);
                            } else if (fieldType == java.sql.Date.class || fieldType == Date.class) {
                                Date newDate = convertDate(values[i]);
                                field.set(entity, newDate);
                            } else if (fieldType == Double.class) {
                                field.set(entity, Double.parseDouble(values[i]));
                            }

                    }
                }

                //System.out.println(entity.toString());
                entityManager.merge(entity);
            }
            entityManager.flush();
            reader.close();
            etlLoggingService.logProcessEnd("Load CSV Data to RAW from -" + resource.getFilename(), "COMPLETED");
        }


        Thread.sleep(5000);
        transferData(tableNames);
    }

    @Transactional
    public void transferData(List<String> tableNames) throws ClassNotFoundException {


        for (String tableName : tableNames) {
            etlLoggingService.logProcessStart("Transfer Data from RAW - " + tableName);
            try {

                String nativeQuery = "INSERT INTO ds." + tableName + " SELECT * FROM raw." + tableName;
                int rowsAffected = entityManager.createNativeQuery(nativeQuery).executeUpdate();
                System.out.println("Перенесено записей: " + rowsAffected);
            } catch (Exception e) {
                if (e.getCause() instanceof PSQLException) {
                    PSQLException psqlException = (PSQLException) e.getCause();
                    if ("23505".equals(psqlException.getSQLState())) {

                        etlLoggingService.logProcessError("Transfer Data - " + tableName, e.getMessage());
                        saveRejectedData(tableName, e.getMessage());
                    } else {

                        etlLoggingService.logProcessError("Transfer Data - " + tableName, e.getMessage());
                    }
                } else {

                    etlLoggingService.logProcessError("Transfer Data - " + tableName, e.getMessage());
                }
            }
            etlLoggingService.logProcessEnd("Transfer Data from RAW - " + tableName, "SUCCESS");
        }

    }




    private void saveRejectedData(String tableName, String errorMessage) {
        try {

            String insertQuery = "INSERT INTO raw.rejected_data (table_name, error_message) VALUES (?, ?)";
            Query query = entityManager.createNativeQuery(insertQuery);
            query.setParameter(1, tableName);
            query.setParameter(2, errorMessage);
            query.executeUpdate();
        } catch (Exception e) {


            etlLoggingService.logProcessError("Save Rejected Data - " + tableName, e.getMessage());
        }
    }

    public Date convertDate(String dateString) throws ParseException {
        SimpleDateFormat formatter;
        if (dateString.contains(".")) {
            formatter = new SimpleDateFormat("dd.MM.yy");
        } else {
            formatter = new SimpleDateFormat("yyyy-MM-dd");
        }

        Date utilDate = formatter.parse(dateString);
        return new java.sql.Date(utilDate.getTime());
    }

    public String convertFileNameToClassName(String fileName) {
        int dotIndex = fileName.lastIndexOf(".");
        if (dotIndex != -1) {
            fileName = fileName.substring(0, dotIndex);
        }

        String[] parts = fileName.split("_");

        StringBuilder result = new StringBuilder();
        for (String part : parts) {
            if (!part.isEmpty()) {
                result.append(Character.toUpperCase(part.charAt(0)));
                result.append(part.substring(1).toLowerCase());
            }
        }

        return result.toString();
    }


    private void clearTable(String tableName, String schemaName, EntityManager entityManager) {
        EntityTransaction transaction = entityManager.getTransaction();
        transaction.begin();
        entityManager.createNativeQuery("DELETE FROM " + schemaName + "." + tableName).executeUpdate();
        transaction.commit();
    }

    private void insertDataIntoTable(List<String[]> dataRows, String tableName, String schemaName, EntityManager entityManager) {
        EntityTransaction transaction = entityManager.getTransaction();
        transaction.begin();

        for (String[] row : dataRows) {
            StringBuilder sql = new StringBuilder("INSERT INTO ");
            sql.append(schemaName).append(".").append(tableName).append(" VALUES (");

            for (int i = 0; i < row.length; i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append("'").append(row[i]).append("'");
            }

            sql.append(")");

            entityManager.createNativeQuery(sql.toString()).executeUpdate();
        }

        transaction.commit();
    }

    private String determineEncoding(String filename) {
        if(filename.equals("md_ledger_account_s.csv")){
            return "Windows-1251";
        }
        return "UTF-8";
    }

}
