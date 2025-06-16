package com.mycompany.kafkaadmin;

import javax.swing.*;
import java.awt.*;

public class KafkaAdminGuiApp extends JFrame {

    private KafkaAdminPanel kafkaAdminPanel; // Панель для управления Kafka

    public KafkaAdminGuiApp() {
        super("Kafka Admin GUI Client"); // Заголовок окна

        // Настраиваем основное окно
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE); // Закрытие приложения при закрытии окна
        setSize(1000, 700); // Размеры окна (ширина, высота)
        setLocationRelativeTo(null); // Размещаем окно по центру экрана

        // Устанавливаем BorderLayout для основного окна
        setLayout(new BorderLayout());

        // Создаем панель KafkaAdminPanel
        kafkaAdminPanel = new KafkaAdminPanel();
        add(kafkaAdminPanel, BorderLayout.CENTER); // Добавляем панель в центр окна

        setVisible(true); // Делаем окно видимым
    }

    public static void main(String[] args) {
        // Запускаем GUI в потоке диспетчеризации событий Swing (EDT)
        // Это стандартная практика для Swing приложений
        SwingUtilities.invokeLater(() -> {
            new KafkaAdminGuiApp();
        });
    }
}