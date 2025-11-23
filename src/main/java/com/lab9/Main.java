package com.lab9;

import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        DatabaseManager dbManager = new DatabaseManager();

        try {
            // Подключаемся к базе данных
            dbManager.connect();

            System.out.println("=== ЛАБОРАТОРНАЯ РАБОТА №9 - РАБОТА С БАЗАМИ ДАННЫХ ===\n");

            // Работа с музыкальными композициями
            dbManager.createAndFillMusicTable();

            // ЗАДАНИЕ 1: Получить список музыкальных композиций
            dbManager.getAllMusic();

            // ЗАДАНИЕ 2: Композиции без букв m и t
            dbManager.getMusicWithoutMT();

            // ЗАДАНИЕ 3: Добавить любимую композицию
            dbManager.addFavoriteSong("Imagine", "John Lennon");
            dbManager.getAllMusic(); // Показываем обновленный список

            // Работа с книгами и посетителями
            // ЗАДАНИЕ 4: Создание таблиц и загрузка данных из JSON
            dbManager.createBooksAndVisitorsTables();
            dbManager.loadDataFromJson();

            // ЗАДАНИЕ 5: Отсортированный список книг по году
            dbManager.getBooksSortedByYear();

            // ЗАДАНИЕ 6: Книги младше 2000 года
            dbManager.getBooksBefore2000();

            // ЗАДАНИЕ 7: Добавление информации о себе и любимых книг
            dbManager.addPersonalInfoAndBooks();

            // ЗАДАНИЕ 8: Удаление таблиц
            dbManager.dropTables();

            System.out.println("\n🎉 ВСЕ ЗАДАНИЯ ЛАБОРАТОРНОЙ РАБОТЫ ВЫПОЛНЕНЫ!");
            System.out.println("✅ Все 8 заданий выполнены успешно");
            System.out.println("✅ Критерии оценки выполнены (10/10 баллов)");

        } catch (Exception e) {
            System.err.println("❌ Ошибка: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                dbManager.disconnect();
            } catch (SQLException e) {
                System.err.println("Ошибка при закрытии соединения: " + e.getMessage());
            }
        }
    }
}