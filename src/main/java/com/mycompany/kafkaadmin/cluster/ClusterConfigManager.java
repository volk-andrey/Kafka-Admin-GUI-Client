package com.mycompany.kafkaadmin.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class ClusterConfigManager {

    private static final Logger log = LoggerFactory.getLogger(ClusterConfigManager.class);
    private static final String CONFIG_FILE_NAME = "kafka_cluster_configs.json";
    private final File configFile;
    private final ObjectMapper objectMapper;

    private List<ClusterConfig> configs;

    public ClusterConfigManager() {
        // Определяем путь к файлу конфигурации.
        // Можно использовать пользовательскую директорию, например, System.getProperty("user.home") + File.separator + ".kafka-admin-gui"
        // Но для простоты пока оставим в текущей рабочей директории.
        this.configFile = new File(CONFIG_FILE_NAME);
        this.objectMapper = new ObjectMapper();
        // Включаем форматирование JSON для удобства чтения
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

        this.configs = new ArrayList<>();
        loadConfigs();
    }

    private void loadConfigs() {
        if (configFile.exists() && configFile.length() > 0) {
            try (FileReader reader = new FileReader(configFile)) {
                // Jackson не может напрямую десериализовать Map<String, String> в Properties,
                // поэтому будем использовать промежуточный Map.
                List<MapContainer> rawConfigs = objectMapper.readValue(reader, new TypeReference<List<MapContainer>>() {});
                this.configs = rawConfigs.stream().map(mc -> {
                    Properties props = new Properties();
                    props.putAll(mc.properties); // Копируем все из Map в Properties
                    return new ClusterConfig(mc.id, mc.name, props);
                }).collect(Collectors.toList());
                log.info("Loaded {} cluster configurations from {}", configs.size(), configFile.getAbsolutePath());
            } catch (IOException e) {
                log.error("Failed to load cluster configurations from {}: {}", configFile.getAbsolutePath(), e.getMessage(), e);
                // Если не удалось загрузить, начинаем с пустого списка
                this.configs = new ArrayList<>();
            }
        } else {
            log.info("Configuration file {} not found or empty. Starting with no saved configurations.", configFile.getAbsolutePath());
        }
    }

    public void saveConfigs() {
        try (FileWriter writer = new FileWriter(configFile)) {
            // Перед сериализацией, преобразуем Properties в Map<String, String>
            List<MapContainer> rawConfigs = configs.stream().map(config -> {
                MapContainer mc = new MapContainer();
                mc.id = config.getId();
                mc.name = config.getName();
                // Properties наследует Hashtable, но не Map<String, String>,
                // поэтому копируем поэлементно.
                config.getConnectionProperties().forEach((key, value) -> mc.properties.put(key.toString(), value.toString()));
                return mc;
            }).collect(Collectors.toList());

            objectMapper.writeValue(writer, rawConfigs);
            log.info("Saved {} cluster configurations to {}", configs.size(), configFile.getAbsolutePath());
        } catch (IOException e) {
            log.error("Failed to save cluster configurations to {}: {}", configFile.getAbsolutePath(), e.getMessage(), e);
            JOptionPane.showMessageDialog(null, "Error saving configurations: " + e.getMessage(), "Save Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    public List<ClusterConfig> getConfigs() {
        return new ArrayList<>(configs); // Возвращаем копию, чтобы избежать внешних модификаций
    }

    public void addConfig(ClusterConfig config) {
        // Проверяем, существует ли уже конфигурация с таким ID (для предотвращения дубликатов при сохранении)
        // Хотя UUID должен гарантировать уникальность, это также полезно для обновления.
        removeConfig(config.getId()); // Удаляем старую, если она есть
        configs.add(config);
        saveConfigs();
    }

    public void updateConfig(ClusterConfig config) {
        // Проверяем, есть ли такой ID в списке, и заменяем
        for (int i = 0; i < configs.size(); i++) {
            if (configs.get(i).getId().equals(config.getId())) {
                configs.set(i, config);
                saveConfigs();
                return;
            }
        }
        // Если не нашли, добавляем как новую
        addConfig(config);
    }

    public void removeConfig(String id) {
        if (configs.removeIf(config -> config.getId().equals(id))) {
            saveConfigs();
            log.info("Removed configuration with ID: {}", id);
        }
    }

    // Вспомогательный класс для Jackson, чтобы корректно сериализовать/десериализовать Properties
    // поскольку Properties не является прямым Map<String, String>
    private static class MapContainer {
        public String id;
        public String name;
        public java.util.Map<String, String> properties = new java.util.HashMap<>();
    }
}