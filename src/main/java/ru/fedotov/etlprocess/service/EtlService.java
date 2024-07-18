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

        etlLoggingService.logProcessStart("Load CSV data");

        for (Resource resource : resources) {

            BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()));

            String line;


            // Пропускаем заголовок CSV-файла
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

                // Заполняем поля объекта
                Field[] fields = entityClass.getDeclaredFields();
                for (int i = 0; i < fields.length; i++) {
                    Field field = fields[i];
                    field.setAccessible(true); // Делаем поле доступным
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
        }

        Thread.sleep(5000);
            // Логирование окончания загрузки данных
        etlLoggingService.logProcessEnd("Load CSV Data", "COMPLETED");


    }

    @Transactional
    public void transferData(List<String> tableNames) throws ClassNotFoundException {

        for (String tableName : tableNames) {
            try {
                // Используем INSERT ... SELECT ... ON CONFLICT DO NOTHING для обработки конфликтов
                String nativeQuery = "INSERT INTO ds." + tableName + " SELECT * FROM raw." + tableName;
                int rowsAffected = entityManager.createNativeQuery(nativeQuery).executeUpdate();
                System.out.println("Перенесено записей: " + rowsAffected);
            } catch (Exception e) {
                if (e.getCause() instanceof PSQLException) {
                    PSQLException psqlException = (PSQLException) e.getCause();
                    if ("23505".equals(psqlException.getSQLState())) {
                        // Ошибка повторяющегося ключа - сохраняем данные в rejected_data
                        etlLoggingService.logProcessError("Transfer Data - " + tableName, e.getMessage());
                        saveRejectedData(tableName, e.getMessage());
                    } else {
                        // Логирование других SQL ошибок
                        etlLoggingService.logProcessError("Transfer Data - " + tableName, e.getMessage());
                    }
                } else {
                    // Логирование других исключений
                    etlLoggingService.logProcessError("Transfer Data - " + tableName, e.getMessage());
                }
            }
        }

    }




    private void saveRejectedData(String tableName, String errorMessage) {
        try {
            // Предположим, что rejected_data имеет поля (table_name, error_message)
            String insertQuery = "INSERT INTO raw.rejected_data (table_name, error_message) VALUES (?, ?)";
            Query query = entityManager.createNativeQuery(insertQuery);
            query.setParameter(1, tableName);
            query.setParameter(2, errorMessage);
            query.executeUpdate();
        } catch (Exception e) {
            // Логирование ошибок сохранения в rejected_data
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

        // Разбиваем строку по символу "_"
        String[] parts = fileName.split("_");

        // Строим результирующую строку
        StringBuilder result = new StringBuilder();
        for (String part : parts) {
            // Преобразуем первую букву к верхнему регистру и добавляем к результату
            if (!part.isEmpty()) {
                result.append(Character.toUpperCase(part.charAt(0)));
                result.append(part.substring(1).toLowerCase());
            }
        }

        return result.toString();
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

    public void importDataFromCSV(String filePath, String tableName, String schemaName, EntityManager entityManager) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            List<String[]> dataRows = new ArrayList<>();

            // Чтение данных из CSV файла
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(";");
                dataRows.add(data);
            }

            // Удаление старых данных из таблицы
            clearTable(tableName, schemaName, entityManager);

            // Вставка новых данных в таблицу
            insertDataIntoTable(dataRows, tableName, schemaName, entityManager);

        } catch (IOException e) {
            e.printStackTrace();
        }
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

//    public void logStartOfETLProcess(String objectOfOper) {
//        etlLoggingService.logStartOfETLProcess(objectOfOper);
//    }
//    public void logEndOfETLProcess(String objectOfOper, int recordsCount) {
//        etlLoggingService.logEndOfETLProcess(objectOfOper, recordsCount);
//    }
//    public void logRecordToTable(String objectOfOper, int recordsCount) {
//        etlLoggingService.logRecordToTable(objectOfOper, recordsCount);
//    }


//    // Загрузка данных из CSV в raw схему для заданной таблицы
//    @Transactional
//    private void loadToRaw(String csvFilePath, String tableName) {
//        logStartOfETLProcess("raw." + tableName);
//
//        List<?> entities = readEntitiesFromCsv(csvFilePath, tableName);
//        saveToRawSchema(entities);
//
//        logRecordToTable("raw." + tableName, entities.size());
//        logEndOfETLProcess("raw." + tableName, entities.size());
//    }
//
//    // Преобразование и загрузка данных из raw в DS схему для заданной таблицы
//    @Transactional
//    private void transformAndLoadToDS(String tableName) {
//        logStartOfETLProcess("DS." + tableName);
//
//        List<?> rawEntities = getFromRawSchema(tableName);
//        List<?> entities = transformRawEntities(rawEntities, tableName);
//        saveToDSSchema(entities);
//
//        logRecordToTable("DS." + tableName, entities.size());
//        logEndOfETLProcess("DS." + tableName, entities.size());
//    }

    // Метод для чтения данных из CSV файла для заданной таблицы
//    private List<?> readEntitiesFromCsv(String csvFilePath, String tableName) {
//        List<?> entities = new ArrayList<>();
//        try (BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
//            String line;
//            while ((line = reader.readLine()) != null) {
//                // Чтение данных из CSV и создание объектов для каждой таблицы
//                switch (tableName) {
//                    case "ft_balance_f":
//                        entities.add(createBalanceFromCsv(line));
//                        break;
//                    case "ft_posting_f":
//                        entities.add(createPostingFromCsv(line));
//                        break;
//                    // Добавление обработки других таблиц по аналогии
//                    default:
//                        // Если таблица не поддерживается, можно выбрасывать исключение или игнорировать
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return entities;
//    }
//
//    // Создание объекта Balance из строки CSV
//    private Balance createBalanceFromCsv(String line) {
//        // Реализация создания объекта Balance из строки CSV
//    }
//
//    // Создание объекта Posting из строки CSV
//    private Posting createPostingFromCsv(String line) {
//        // Реализация создания объекта Posting из строки CSV
//    }
//
//    // Сохранение данных в raw схему
//    private void saveToRawSchema(List<?> entities) {
//        for (Object entity : entities) {
//            // Сохранение в raw схему в зависимости от типа сущности
//            if (entity instanceof Balance) {
//                rawBalanceRepository.save((Balance) entity);
//            } else if (entity instanceof Posting) {
//                rawPostingRepository.save((Posting) entity);
//            }
//            // Добавление сохранения других сущностей по аналогии
//        }
//        // Вызов flush для немедленной записи в базу данных
//        rawBalanceRepository.flush();
//        rawPostingRepository.flush();
//        // Добавление flush для других репозиториев по аналогии
//    }
//
//    // Получение данных из raw схемы
//    private List<?> getFromRawSchema(String tableName) {
//        // Реализация получения данных из raw схемы в зависимости от типа таблицы
//        switch (tableName) {
//            case "ft_balance_f":
//                return rawBalanceRepository.findAll();
//            case "ft_posting_f":
//                return rawPostingRepository.findAll();
//            // Добавление получения данных из других таблиц по аналогии
//            default:
//                return new ArrayList<>();
//        }
//    }
//
//    // Преобразование raw данных в целевую структуру для заданной таблицы
//    private List<?> transformRawEntities(List<?> rawEntities, String tableName) {
//        // Реализация преобразования raw данных в зависимости от типа таблицы
//        switch (tableName) {
//            case "ft_balance_f":
//                return transformRawBalances((List<RawBalance>) rawEntities);
//            case "ft_posting_f":
//                return transformRawPostings((List<RawPosting>) rawEntities);
//            // Добавление преобразования для других таблиц по аналогии
//            default:
//                return new ArrayList<>();
//        }
//    }
//
//    // Преобразование raw балансов в целевую структуру
//    private List<Balance> transformRawBalances(List<RawBalance> rawBalances) {
//        // Реализация преобразования raw балансов
//    }
//
//    // Преобразование raw проводок в целевую структуру
//    private List<Posting> transformRawPostings(List<RawPosting> rawPostings) {
//        // Реализация преобразования raw проводок
//    }
//
//    // Сохранение данных в DS схему
//    private void saveToDSSchema(List<?> entities) {
//        for (Object entity : entities) {
//            // Сохранение в DS схему в зависимости от типа сущности
//            if (entity instanceof Balance) {
//                balanceRepository.save((Balance) entity);
//            } else if (entity instanceof Posting) {
//                postingRepository.save((Posting) entity);
//            }
//            // Добавление сохранения других сущностей по аналогии
//        }
//        // Вызов flush для немедленной записи в базу данных
//        balanceRepository.flush();
//        postingRepository.flush();
//        // Добавление flush для других репозиториев по аналогии
//    }
//
//    // Логирование записи данных в таблицу для заданной таблицы
//    private void logRecordToTable(String tableName, int recordsCount) {
//        etlLoggingService.logRecordToTable(tableName, recordsCount);
//    }
//
//    // Использование Spring Scheduler для выполнения ETL процесса
//    @Scheduled(fixedDelay = 5000) // каждые 5 секунд
//    public void executeETLProcess() {
//        logStartOfETLProcess("All Tables");
//
//        // Пример загрузки данных из CSV в raw схему для каждой таблицы
//        loadToRaw("/path/to/ft_balance_f.csv", "ft_balance_f");
//        loadToRaw("/path/to/ft_posting_f.csv", "ft_posting_f");
//        // Добавление других таблиц по аналогии
//
//        // Пример преобразования и загрузки в DS схему для каждой таблицы
//        transformAndLoadToDS("ft_balance_f");
//        transformAndLoadToDS("ft_posting_f");
//        // Добавление других таблиц по аналогии
//
//        logEndOfETLProcess("All Tables", 0); // 0 - потому что не считаем количество записей тут
//    }

//    public void loadFtBalanceFFromCsv(String csvFilePath) {
//        List<FtBalanceF> ftBalanceFList = new ArrayList<>();
//
//        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
//            String line;
//            while ((line = br.readLine()) != null) {
//                String[] data = line.split(";");
//
//                FtBalanceF ftBalanceF = new FtBalanceF();
//                //ftBalanceF.setOnDate();
//                ftBalanceF.setAccountRk(Long.parseLong(data[1]));
//                ftBalanceF.setBalanceOut(Double.parseDouble(data[2]));
//
//                ftBalanceFList.add(ftBalanceF);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        ftBalanceFRepository.saveAll(ftBalanceFList);
//    }



}
