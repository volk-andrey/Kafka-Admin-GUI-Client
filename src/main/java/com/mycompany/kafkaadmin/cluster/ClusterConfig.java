package com.mycompany.kafkaadmin.cluster;

import java.util.Properties;
import java.util.UUID; // Для генерации уникального ID для каждой конфигурации

public class ClusterConfig {
    private String id; // Уникальный идентификатор
    private String name; // Название кластера (для отображения в UI)
    private Properties connectionProperties; // Параметры подключения Kafka Admin Client

    // Конструктор для создания новой конфигурации
    public ClusterConfig(String name, Properties connectionProperties) {
        this.id = UUID.randomUUID().toString(); // Генерируем уникальный ID
        this.name = name;
        this.connectionProperties = connectionProperties;
    }

    // Конструктор для загрузки существующей конфигурации (с уже имеющимся ID)
    public ClusterConfig(String id, String name, Properties connectionProperties) {
        this.id = id;
        this.name = name;
        this.connectionProperties = connectionProperties;
    }

    // Геттеры
    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Properties getConnectionProperties() {
        return connectionProperties;
    }

    // Сеттеры (позволяем изменять имя и свойства подключения)
    public void setName(String name) {
        this.name = name;
    }

    public void setConnectionProperties(Properties connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

    // Метод toString для корректного отображения в JList/JComboBox
    @Override
    public String toString() {
        return name; // Отображаем только имя в списке
    }

    // Реализуем equals и hashCode, чтобы корректно работать со списками и коллекциями
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterConfig that = (ClusterConfig) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}