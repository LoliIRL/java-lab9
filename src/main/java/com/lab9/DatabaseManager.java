package com.lab9;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

public class DatabaseManager {
    private Connection connection;
    private final String URL = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
    private final String USER = "sa";
    private final String PASS = "";

    public void connect() throws SQLException {
        connection = DriverManager.getConnection(URL, USER, PASS);
        System.out.println("✅ Подключение к базе данных установлено");
    }

    public void disconnect() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            System.out.println("✅ Подключение к базе данных закрыто");
        }
    }

    // ЗАДАНИЕ 1: Получить список музыкальных композиций
    public void createAndFillMusicTable() throws SQLException {
        String createSQL = "CREATE TABLE IF NOT EXISTS music (" +
                "id INT AUTO_INCREMENT PRIMARY KEY, " +
                "title VARCHAR(255) NOT NULL, " +
                "artist VARCHAR(255) NOT NULL)";

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createSQL);
            System.out.println("✅ Таблица 'music' создана");
        }

        // Добавляем начальные данные
        String[][] songs = {
                {"Bohemian Rhapsody", "Queen"},
                {"Hotel California", "Eagles"},
                {"Sweet Child O' Mine", "Guns N' Roses"},
                {"Like a Rolling Stone", "Bob Dylan"},
                {"Smells Like Teen Spirit", "Nirvana"}
        };

        String insertSQL = "INSERT INTO music (title, artist) VALUES (?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(insertSQL)) {
            for (String[] song : songs) {
                ps.setString(1, song[0]);
                ps.setString(2, song[1]);
                ps.executeUpdate();
            }
        }
        System.out.println("✅ Начальные данные добавлены в таблицу 'music'");
    }

    public void getAllMusic() throws SQLException {
        String sql = "SELECT * FROM music";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            System.out.println("\n=== ЗАДАНИЕ 1: Список музыкальных композиций ===");
            while (rs.next()) {
                System.out.printf("ID: %d | Название: %-25s | Исполнитель: %s%n",
                        rs.getInt("id"),
                        rs.getString("title"),
                        rs.getString("artist"));
            }
        }
    }

    // ЗАДАНИЕ 2: Композиции без букв m и t (без учета регистра)
    public void getMusicWithoutMT() throws SQLException {
        String sql = "SELECT * FROM music WHERE LOWER(title) NOT LIKE '%m%' AND LOWER(title) NOT LIKE '%t%'";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            System.out.println("\n=== ЗАДАНИЕ 2: Композиции без букв 'm' и 't' ===");
            while (rs.next()) {
                System.out.printf("ID: %d | Название: %-25s | Исполнитель: %s%n",
                        rs.getInt("id"),
                        rs.getString("title"),
                        rs.getString("artist"));
            }
        }
    }

    // ЗАДАНИЕ 3: Добавить любимую композицию
    public void addFavoriteSong(String title, String artist) throws SQLException {
        String sql = "INSERT INTO music (title, artist) VALUES (?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, title);
            ps.setString(2, artist);
            ps.executeUpdate();
            System.out.println("\n=== ЗАДАНИЕ 3: Любимая композиция добавлена ===");
            System.out.println("✅ Добавлена: " + title + " - " + artist);
        }
    }

    // ЗАДАНИЕ 4: Работа с JSON - создание таблиц и загрузка данных
    public void createBooksAndVisitorsTables() throws SQLException {
        String visitorsSQL = "CREATE TABLE IF NOT EXISTS visitors (" +
                "id INT PRIMARY KEY, " +
                "name VARCHAR(255) NOT NULL, " +
                "email VARCHAR(255) UNIQUE)";

        String booksSQL = "CREATE TABLE IF NOT EXISTS books (" +
                "id INT PRIMARY KEY, " +
                "title VARCHAR(255) NOT NULL, " +
                "author VARCHAR(255) NOT NULL, " +
                "year INT, " +
                "genre VARCHAR(100))";

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(visitorsSQL);
            stmt.execute(booksSQL);
            System.out.println("\n=== ЗАДАНИЕ 4: Таблицы созданы ===");
            System.out.println("✅ Таблицы 'visitors' и 'books' созданы");
        }
    }

    @SuppressWarnings("unchecked")
    public void loadDataFromJson() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("books.json");
        Map<String, Object> data = mapper.readValue(inputStream, Map.class);

        // Загружаем посетителей (только уникальные)
        List<Map<String, Object>> visitors = (List<Map<String, Object>>) data.get("visitors");
        String visitorSQL = "MERGE INTO visitors (id, name, email) KEY(id) VALUES (?, ?, ?)";

        try (PreparedStatement ps = connection.prepareStatement(visitorSQL)) {
            for (Map<String, Object> visitor : visitors) {
                ps.setInt(1, (Integer) visitor.get("id"));
                ps.setString(2, (String) visitor.get("name"));
                ps.setString(3, (String) visitor.get("email"));
                ps.executeUpdate();
            }
        }

        // Загружаем книги (только уникальные)
        List<Map<String, Object>> books = (List<Map<String, Object>>) data.get("books");
        String bookSQL = "MERGE INTO books (id, title, author, year, genre) KEY(id) VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement ps = connection.prepareStatement(bookSQL)) {
            for (Map<String, Object> book : books) {
                ps.setInt(1, (Integer) book.get("id"));
                ps.setString(2, (String) book.get("title"));
                ps.setString(3, (String) book.get("author"));
                ps.setInt(4, (Integer) book.get("year"));
                ps.setString(5, (String) book.get("genre"));
                ps.executeUpdate();
            }
        }

        System.out.println("✅ Данные из JSON загружены в базу (только уникальные записи)");

        // Показываем загруженные данные
        showVisitors();
        showBooks();
    }

    private void showVisitors() throws SQLException {
        String sql = "SELECT * FROM visitors";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            System.out.println("\n📋 Загруженные посетители:");
            while (rs.next()) {
                System.out.printf("ID: %d | Имя: %-20s | Email: %s%n",
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getString("email"));
            }
        }
    }

    private void showBooks() throws SQLException {
        String sql = "SELECT * FROM books";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            System.out.println("\n📚 Загруженные книги:");
            while (rs.next()) {
                System.out.printf("ID: %d | %-30s | %-20s | %d | %s%n",
                        rs.getInt("id"),
                        rs.getString("title"),
                        rs.getString("author"),
                        rs.getInt("year"),
                        rs.getString("genre"));
            }
        }
    }

    // ЗАДАНИЕ 5: Отсортированный список книг по году издания
    public void getBooksSortedByYear() throws SQLException {
        String sql = "SELECT * FROM books ORDER BY year";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            System.out.println("\n=== ЗАДАНИЕ 5: Книги отсортированные по году издания ===");
            while (rs.next()) {
                System.out.printf("%d | %-30s | %-20s | %s%n",
                        rs.getInt("year"),
                        rs.getString("title"),
                        rs.getString("author"),
                        rs.getString("genre"));
            }
        }
    }

    // ЗАДАНИЕ 6: Книги младше 2000 года
    public void getBooksBefore2000() throws SQLException {
        String sql = "SELECT * FROM books WHERE year < 2000";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            System.out.println("\n=== ЗАДАНИЕ 6: Книги изданные до 2000 года ===");
            while (rs.next()) {
                System.out.printf("%d | %-30s | %-20s | %s%n",
                        rs.getInt("year"),
                        rs.getString("title"),
                        rs.getString("author"),
                        rs.getString("genre"));
            }
        }
    }

    // ЗАДАНИЕ 7: Добавить информацию о себе и любимые книги
    public void addPersonalInfoAndBooks() throws SQLException {
        // Добавляем себя как посетителя
        String visitorSQL = "MERGE INTO visitors (id, name, email) KEY(id) VALUES (?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(visitorSQL)) {
            ps.setInt(1, 100);
            ps.setString(2, "Максим Студент");
            ps.setString(3, "maxim.student@university.com");
            ps.executeUpdate();
        }

        // Добавляем любимые книги
        Object[][] favoriteBooks = {
                {100, "Чистый код", "Роберт Мартин", 2008, "Программирование"},
                {101, "Java. Эффективное программирование", "Джошуа Блох", 2001, "Программирование"},
                {102, "Совершенный код", "Стив Макконнелл", 1993, "Программирование"}
        };

        String bookSQL = "MERGE INTO books (id, title, author, year, genre) KEY(id) VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement ps = connection.prepareStatement(bookSQL)) {
            for (Object[] book : favoriteBooks) {
                ps.setInt(1, (Integer) book[0]);
                ps.setString(2, (String) book[1]);
                ps.setString(3, (String) book[2]);
                ps.setInt(4, (Integer) book[3]);
                ps.setString(5, (String) book[4]);
                ps.executeUpdate();
            }
        }

        System.out.println("\n=== ЗАДАНИЕ 7: Добавлена персональная информация ===");
        System.out.println("✅ Добавлен посетитель: Максим Студент");
        System.out.println("✅ Добавлены любимые книги по программированию");

        // Выводим добавленные данные
        System.out.println("\n👤 Все посетители (включая добавленного):");
        showVisitors();

        System.out.println("\n📚 Все книги (включая добавленные):");
        showBooks();
    }

    // ЗАДАНИЕ 8: Удаление таблиц
    public void dropTables() throws SQLException {
        String[] tables = {"books", "visitors", "music"};

        try (Statement stmt = connection.createStatement()) {
            for (String table : tables) {
                try {
                    stmt.execute("DROP TABLE " + table);
                    System.out.println("✅ Таблица '" + table + "' удалена");
                } catch (SQLException e) {
                    System.out.println("ℹ️ Таблица '" + table + "' не существует или уже удалена");
                }
            }
        }
        System.out.println("\n=== ЗАДАНИЕ 8: Все таблицы удалены ===");
    }
}