package com.mycompany.kafkaadmin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.util.*;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicsPanel extends JPanel {

    private static final Logger log = LoggerFactory.getLogger(TopicsPanel.class);

    private AdminClient adminClient;
    private DefaultTableModel topicsTableModel;
    private JTable topicsTable;
    private JButton refreshTopicsButton;
    private JButton createTopicButton;
    private JButton deleteTopicButton;
    private JButton alterTopicConfigsButton;
    private JButton viewPartitionsButton; // Кнопка для просмотра партиций
    private JTextField topicFilterField; // Поле фильтрации топиков
    private JLabel filterStatusLabel; // Счетчик отфильтрованных топиков

    // Панель для детальной информации/действий с топиками
    private JPanel topicDetailsPanel;
    private JTextArea topicConfigArea; // Для отображения настроек топика
    private DefaultTableModel partitionsTableModel; // Модель для таблицы партиций
    private JTable partitionsTable; // Таблица для партиций

    // Данные для фильтрации
    private List<Object[]> allTopicsData = new ArrayList<>(); // Храним все данные топиков

    public TopicsPanel() {
        setLayout(new BorderLayout(10, 10)); // Добавляем отступы между компонентами

        // --- Верхняя панель с кнопками и фильтром ---
        JPanel buttonPanel = new JPanel(new BorderLayout(5, 5));

        // Панель с кнопками
        JPanel buttonsPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        refreshTopicsButton = new JButton("Refresh Topics");
        createTopicButton = new JButton("Create Topic");
        deleteTopicButton = new JButton("Delete Topic");
        alterTopicConfigsButton = new JButton("Alter Configs");
        viewPartitionsButton = new JButton("View Partitions");

        buttonsPanel.add(refreshTopicsButton);
        buttonsPanel.add(createTopicButton);
        buttonsPanel.add(deleteTopicButton);
        buttonsPanel.add(alterTopicConfigsButton);
        buttonsPanel.add(viewPartitionsButton);

        // Панель с фильтром
        JPanel filterPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        filterPanel.add(new JLabel("Фильтр топиков:"));
        topicFilterField = new JTextField(20);
        topicFilterField.setToolTipText("Введите часть имени топика для фильтрации");

        // Кнопка очистки фильтра
        JButton clearFilterButton = new JButton("✕");
        clearFilterButton.setToolTipText("Очистить фильтр");
        clearFilterButton.setPreferredSize(new Dimension(25, 25));
        clearFilterButton.addActionListener(e -> {
            topicFilterField.setText("");
            filterTopics();
        });

        // Счетчик отфильтрованных топиков
        filterStatusLabel = new JLabel("Всего: 0");
        filterStatusLabel.setForeground(Color.GRAY);

        // Добавляем слушатель для автоматической фильтрации
        topicFilterField.getDocument().addDocumentListener(new javax.swing.event.DocumentListener() {
            @Override
            public void insertUpdate(javax.swing.event.DocumentEvent e) {
                filterTopics();
            }

            @Override
            public void removeUpdate(javax.swing.event.DocumentEvent e) {
                filterTopics();
            }

            @Override
            public void changedUpdate(javax.swing.event.DocumentEvent e) {
                filterTopics();
            }
        });

        filterPanel.add(topicFilterField);
        filterPanel.add(clearFilterButton);
        filterPanel.add(filterStatusLabel);

        buttonPanel.add(buttonsPanel, BorderLayout.WEST);
        buttonPanel.add(filterPanel, BorderLayout.EAST);
        add(buttonPanel, BorderLayout.NORTH);

        // --- Таблица топиков ---
        String[] topicColumnNames = { "Topic Name", "Partitions", "Replicas" };
        topicsTableModel = new DefaultTableModel(topicColumnNames, 0) {
            @Override
            public boolean isCellEditable(int row, int column) {
                return false; // Запрещаем редактирование ячеек таблицы
            }
        };
        topicsTable = new JTable(topicsTableModel);
        // Добавляем слушатель выбора строки, чтобы активировать кнопки
        topicsTable.getSelectionModel().addListSelectionListener(e -> updateButtonStates());
        JScrollPane topicsScrollPane = new JScrollPane(topicsTable);
        add(topicsScrollPane, BorderLayout.CENTER);

        // --- Панель для деталей топика и партиций (снизу) ---
        topicDetailsPanel = new JPanel(new BorderLayout(5, 5));
        topicDetailsPanel.setBorder(BorderFactory.createTitledBorder("Topic Details & Partitions"));
        topicDetailsPanel.setPreferredSize(new Dimension(getWidth(), 250)); // Фиксируем высоту

        // Область для настроек топика
        JPanel configPanel = new JPanel(new BorderLayout());
        configPanel.setBorder(BorderFactory.createTitledBorder("Topic Configurations"));
        topicConfigArea = new JTextArea(5, 40);
        topicConfigArea.setEditable(false);
        JScrollPane configScrollPane = new JScrollPane(topicConfigArea);
        configPanel.add(configScrollPane, BorderLayout.CENTER);
        topicDetailsPanel.add(configPanel, BorderLayout.WEST); // Настройки слева

        // Таблица для партиций
        JPanel partitionsPanel = new JPanel(new BorderLayout());
        partitionsPanel.setBorder(BorderFactory.createTitledBorder("Partitions Info"));
        String[] partitionColumnNames = { "Partition", "Leader", "Replicas", "In-Sync Replicas (ISR)" };
        partitionsTableModel = new DefaultTableModel(partitionColumnNames, 0) {
            @Override
            public boolean isCellEditable(int row, int column) {
                return false;
            }
        };
        partitionsTable = new JTable(partitionsTableModel);
        JScrollPane partitionsScrollPane = new JScrollPane(partitionsTable);
        partitionsPanel.add(partitionsScrollPane, BorderLayout.CENTER);
        topicDetailsPanel.add(partitionsPanel, BorderLayout.CENTER); // Партиции по центру

        add(topicDetailsPanel, BorderLayout.SOUTH); // Панель деталей снизу

        // --- Добавление слушателей кнопок ---
        refreshTopicsButton.addActionListener(e -> fetchTopics());
        createTopicButton.addActionListener(e -> showCreateTopicDialog());
        deleteTopicButton.addActionListener(e -> deleteSelectedTopic());
        alterTopicConfigsButton.addActionListener(e -> showAlterTopicConfigsDialog());
        viewPartitionsButton.addActionListener(e -> viewSelectedTopicPartitions());

        // Изначально кнопки управления топиками должны быть неактивны, пока топик не
        // выбран
        updateButtonStates();
    }

    /**
     * Устанавливает AdminClient для этой панели. Вызывается из KafkaAdminPanel.
     * 
     * @param adminClient
     */
    public void setAdminClient(AdminClient adminClient) {
        this.adminClient = adminClient;
        // После установки AdminClient, можно сразу попробовать загрузить топики
        fetchTopics();
    }

    /**
     * Обновляет состояние кнопок в зависимости от того, выбрана ли строка в таблице
     * топиков.
     */
    private void updateButtonStates() {
        boolean topicSelected = topicsTable.getSelectedRow() != -1;
        deleteTopicButton.setEnabled(topicSelected);
        alterTopicConfigsButton.setEnabled(topicSelected);
        viewPartitionsButton.setEnabled(topicSelected);

        // Очищаем детали топика при снятии выбора
        if (!topicSelected) {
            topicConfigArea.setText("");
            partitionsTableModel.setRowCount(0); // Очищаем таблицу партиций
        }
    }

    /**
     * Фильтрует топики по введенному тексту в поле фильтрации.
     */
    private void filterTopics() {
        String filterText = topicFilterField.getText().toLowerCase().trim();

        // Очищаем таблицу
        topicsTableModel.setRowCount(0);

        // Фильтруем данные
        int filteredCount = 0;
        for (Object[] topicData : allTopicsData) {
            String topicName = (String) topicData[0];
            if (filterText.isEmpty() || topicName.toLowerCase().contains(filterText)) {
                topicsTableModel.addRow(topicData);
                filteredCount++;
            }
        }

        // Обновляем счетчик
        if (filterText.isEmpty()) {
            filterStatusLabel.setText("Всего: " + allTopicsData.size());
        } else {
            filterStatusLabel.setText("Найдено: " + filteredCount + " из " + allTopicsData.size());
        }

        // Обновляем состояние кнопок
        updateButtonStates();
    }

    /**
     * Загружает список топиков из Kafka и отображает их в таблице.
     */
    public void fetchTopics() {
        if (adminClient == null) {
            log.warn("AdminClient is null. Cannot fetch topics.");
            statusMessage("Not connected to Kafka.", JOptionPane.WARNING_MESSAGE);
            return;
        }

        // Очищаем таблицу и данные перед загрузкой новых данных
        topicsTableModel.setRowCount(0);
        partitionsTableModel.setRowCount(0);
        topicConfigArea.setText("");
        allTopicsData.clear(); // Очищаем данные для фильтрации

        refreshTopicsButton.setEnabled(false); // Отключаем кнопку на время загрузки
        statusMessage("Loading topics...", JOptionPane.INFORMATION_MESSAGE);

        new SwingWorker<Map<String, TopicDescription>, Void>() {
            @Override
            protected Map<String, TopicDescription> doInBackground() throws Exception {
                // Получаем список топиков
                Set<String> topicNames = adminClient.listTopics().names().get();
                // Получаем подробное описание каждого топика
                return adminClient.describeTopics(topicNames).all().get();
            }

            @Override
            protected void done() {
                try {
                    Map<String, TopicDescription> topicDescriptions = get();

                    // Сохраняем все данные для фильтрации
                    for (TopicDescription description : topicDescriptions.values()) {
                        String topicName = description.name();
                        int partitions = description.partitions().size();
                        // Коэффициент репликации - это размер списка реплик для первой партиции
                        // (предполагаем, что он одинаков для всех партиций)
                        int replicas = 0;
                        if (!description.partitions().isEmpty()) {
                            replicas = description.partitions().get(0).replicas().size();
                        }

                        Object[] topicData = new Object[] { topicName, partitions, replicas };
                        allTopicsData.add(topicData);
                    }

                    // Применяем текущий фильтр (если есть)
                    filterTopics();

                    statusMessage("Topics loaded successfully.", JOptionPane.INFORMATION_MESSAGE);
                } catch (InterruptedException | ExecutionException ex) {
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    log.error("Error fetching topics: {}", cause.getMessage(), cause);
                    statusMessage("Error loading topics: " + cause.getMessage(), JOptionPane.ERROR_MESSAGE);
                } finally {
                    refreshTopicsButton.setEnabled(true);
                    updateButtonStates(); // Обновляем состояние кнопок после загрузки
                }
            }
        }.execute();
    }

    /**
     * Показывает диалоговое окно для создания нового топика.
     */
    private void showCreateTopicDialog() {
        if (adminClient == null) {
            statusMessage("Not connected to Kafka.", JOptionPane.WARNING_MESSAGE);
            return;
        }

        // Получаем родительское окно
        Window window = SwingUtilities.getWindowAncestor(this);
        Frame ownerFrame = null;
        if (window instanceof Frame) {
            ownerFrame = (Frame) window;
        }

        // Используем новый диалог создания топика с передачей AdminClient
        com.mycompany.kafkaadmin.dialog.CreateTopicDialog dialog = new com.mycompany.kafkaadmin.dialog.CreateTopicDialog(
                ownerFrame, adminClient);
        dialog.setVisible(true);

        if (dialog.isConfirmed()) {
            NewTopic newTopic = dialog.createNewTopic();
            createTopic(newTopic);
        }
    }

    /**
     * Отправляет запрос на создание топика в Kafka.
     */
    private void createTopic(String topicName, int partitions, short replicationFactor) {
        NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
        createTopic(newTopic);
    }

    /**
     * Отправляет запрос на создание топика в Kafka с конфигурацией.
     */
    private void createTopic(NewTopic newTopic) {
        String topicName = newTopic.name();
        statusMessage("Creating topic: " + topicName + "...", JOptionPane.INFORMATION_MESSAGE);
        createTopicButton.setEnabled(false);

        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                return null;
            }

            @Override
            protected void done() {
                try {
                    get();
                    statusMessage("Topic '" + topicName + "' created successfully!", JOptionPane.INFORMATION_MESSAGE);
                    fetchTopics();
                } catch (InterruptedException | ExecutionException ex) {
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    log.error("Error creating topic '{}': {}", topicName, cause.getMessage(), cause);
                    // Прямое отображение ошибки пользователю
                    JOptionPane.showMessageDialog(TopicsPanel.this,
                            "Failed to create topic '" + topicName + "':\n" + cause.getMessage(),
                            "Topic Creation Error",
                            JOptionPane.ERROR_MESSAGE);
                } finally {
                    createTopicButton.setEnabled(true);
                }
            }
        }.execute();
    }

    /**
     * Удаляет выбранный топик.
     */
    private void deleteSelectedTopic() {
        if (adminClient == null) {
            statusMessage("Not connected to Kafka.", JOptionPane.WARNING_MESSAGE);
            return;
        }

        int selectedRow = topicsTable.getSelectedRow();
        if (selectedRow == -1) {
            JOptionPane.showMessageDialog(this, "Please select a topic to delete.", "No Topic Selected",
                    JOptionPane.WARNING_MESSAGE);
            return;
        }

        String topicName = (String) topicsTableModel.getValueAt(selectedRow, 0);
        int confirm = JOptionPane.showConfirmDialog(this,
                "Are you sure you want to delete topic '" + topicName + "'?",
                "Confirm Delete", JOptionPane.YES_NO_OPTION);

        if (confirm == JOptionPane.YES_OPTION) {
            statusMessage("Deleting topic: " + topicName + "...", JOptionPane.INFORMATION_MESSAGE);
            deleteTopicButton.setEnabled(false); // Отключаем кнопку

            new SwingWorker<Void, Void>() {
                @Override
                protected Void doInBackground() throws Exception {
                    adminClient.deleteTopics(Collections.singletonList(topicName)).all().get();
                    return null;
                }

                @Override
                protected void done() {
                    try {
                        get();
                        statusMessage("Topic '" + topicName + "' deleted successfully!",
                                JOptionPane.INFORMATION_MESSAGE);
                        fetchTopics(); // Обновляем список
                    } catch (InterruptedException | ExecutionException ex) {
                        Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                        log.error("Error deleting topic '{}': {}", topicName, cause.getMessage(), cause);
                        statusMessage("Error deleting topic '" + topicName + "': " + cause.getMessage(),
                                JOptionPane.ERROR_MESSAGE);
                    } finally {
                        deleteTopicButton.setEnabled(true); // Включаем кнопку
                    }
                }
            }.execute();
        }
    }

    /**
     * Показывает диалог для изменения настроек выбранного топика.
     */
    private void showAlterTopicConfigsDialog() {
        if (adminClient == null) {
            statusMessage("Not connected to Kafka.", JOptionPane.WARNING_MESSAGE);
            return;
        }

        int selectedRow = topicsTable.getSelectedRow();
        if (selectedRow == -1) {
            JOptionPane.showMessageDialog(this, "Please select a topic to alter its configurations.",
                    "No Topic Selected", JOptionPane.WARNING_MESSAGE);
            return;
        }

        String topicName = (String) topicsTableModel.getValueAt(selectedRow, 0);

        // Сначала получаем текущие настройки топика
        statusMessage("Loading configurations for topic: " + topicName + "...", JOptionPane.INFORMATION_MESSAGE);
        alterTopicConfigsButton.setEnabled(false);

        new SwingWorker<Map<String, ConfigEntry>, Void>() {
            @Override
            protected Map<String, ConfigEntry> doInBackground() throws Exception {
                DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(
                        Collections.singletonList(new ConfigResource(ConfigResource.Type.TOPIC, topicName)));
                return describeConfigsResult.all().get().get(new ConfigResource(ConfigResource.Type.TOPIC, topicName))
                        .entries().stream().collect(Collectors.toMap(ConfigEntry::name, entry -> entry));
            }

            @Override
            protected void done() {
                try {
                    Map<String, ConfigEntry> currentConfigs = get();
                    // Создаем JTable для отображения и редактирования настроек
                    String[] configColumnNames = { "Configuration Name", "Current Value", "New Value" };
                    DefaultTableModel configTableModel = new DefaultTableModel(configColumnNames, 0) {
                        @Override
                        public boolean isCellEditable(int row, int column) {
                            return column == 2; // Разрешаем редактирование только столбца "New Value"
                        }
                    };

                    // for (ConfigEntry entry : currentConfigs.values()) {
                    // // Исключаем настройки, которые нельзя изменять
                    // if (entry.source() != ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG && // Не
                    // статические настройки брокера
                    // entry.source() != ConfigEntry.ConfigSource.DEFAULT_CONFIG && // Не дефолтные
                    // !entry.isReadOnly()) { // Не только для чтения
                    // configTableModel.addRow(new Object[]{entry.name(), entry.value(),
                    // entry.value()}); // По умолчанию "New Value" равен "Current Value"
                    // }
                    // }
                    for (ConfigEntry entry : currentConfigs.values()) {
                        // Убираем фильтрацию временно
                        configTableModel.addRow(new Object[] {
                                entry.name(),
                                entry.value() != null ? entry.value() : "null", // Обработка null-значений
                                entry.value() != null ? entry.value() : "null"
                        });
                    }

                    JTable configTable = new JTable(configTableModel);
                    JScrollPane configScrollPane = new JScrollPane(configTable);
                    configScrollPane.setPreferredSize(new Dimension(500, 300));

                    int result = JOptionPane.showConfirmDialog(TopicsPanel.this, configScrollPane,
                            "Alter Configurations for Topic: " + topicName,
                            JOptionPane.OK_CANCEL_OPTION, JOptionPane.PLAIN_MESSAGE);

                    if (result == JOptionPane.OK_OPTION) {
                        List<ConfigEntry> configsToAlter = new ArrayList<>();
                        for (int i = 0; i < configTableModel.getRowCount(); i++) {
                            String configName = (String) configTableModel.getValueAt(i, 0);
                            String currentValue = (String) configTableModel.getValueAt(i, 1);
                            String newValue = (String) configTableModel.getValueAt(i, 2);

                            // Добавляем только измененные настройки
                            if (!Objects.equals(currentValue, newValue)) {
                                configsToAlter.add(new ConfigEntry(configName, newValue));
                            }
                        }
                        if (!configsToAlter.isEmpty()) {
                            alterTopicConfigs(topicName, configsToAlter);
                        } else {
                            statusMessage("No configuration changes detected for topic '" + topicName + "'.",
                                    JOptionPane.INFORMATION_MESSAGE);
                        }
                    }
                } catch (InterruptedException | ExecutionException ex) {
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    log.error("Error loading configurations for topic '{}': {}", topicName, cause.getMessage(), cause);
                    statusMessage("Error loading configurations for topic '" + topicName + "': " + cause.getMessage(),
                            JOptionPane.ERROR_MESSAGE);
                } finally {
                    alterTopicConfigsButton.setEnabled(true);
                }
            }
        }.execute();
    }

    /**
     * Отправляет запрос на изменение настроек топика в Kafka.
     */
    private void alterTopicConfigs(String topicName, List<ConfigEntry> configsToAlter) {
        Map<ConfigResource, Collection<AlterConfigOp>> configsMap = new HashMap<>();
        ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        List<AlterConfigOp> ops = configsToAlter.stream()
                .map(entry -> new AlterConfigOp(entry, AlterConfigOp.OpType.SET))
                .collect(Collectors.toList());
        configsMap.put(topicResource, ops);

        statusMessage("Altering configurations for topic: " + topicName + "...", JOptionPane.INFORMATION_MESSAGE);
        alterTopicConfigsButton.setEnabled(false);

        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                adminClient.incrementalAlterConfigs(configsMap).all().get();
                return null;
            }

            @Override
            protected void done() {
                try {
                    get();
                    statusMessage("Configurations for topic '" + topicName + "' altered successfully!",
                            JOptionPane.INFORMATION_MESSAGE);
                    // После изменения настроек, можно обновить список топиков или хотя бы их
                    // конфиги
                    describeTopicConfigs(topicName); // Обновляем отображение настроек
                } catch (InterruptedException | ExecutionException ex) {
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    log.error("Error altering configurations for topic '{}': {}", topicName, cause.getMessage(), cause);
                    statusMessage("Error altering configurations for topic '" + topicName + "': " + cause.getMessage(),
                            JOptionPane.ERROR_MESSAGE);
                } finally {
                    alterTopicConfigsButton.setEnabled(true);
                }
            }
        }.execute();
    }

    /**
     * Отображает настройки выбранного топика в текстовой области.
     */
    private void describeTopicConfigs(String topicName) {
        if (adminClient == null) {
            topicConfigArea.setText("Not connected.");
            return;
        }

        topicConfigArea.setText("Loading configurations...");

        new SwingWorker<Map<String, ConfigEntry>, Void>() {
            @Override
            protected Map<String, ConfigEntry> doInBackground() throws Exception {
                DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(
                        Collections.singletonList(new ConfigResource(ConfigResource.Type.TOPIC, topicName)));
                return describeConfigsResult.all().get().get(new ConfigResource(ConfigResource.Type.TOPIC, topicName))
                        .entries().stream().collect(Collectors.toMap(ConfigEntry::name, entry -> entry));
            }

            @Override
            protected void done() {
                try {
                    Map<String, ConfigEntry> configs = get();
                    StringBuilder sb = new StringBuilder();
                    configs.forEach((name, entry) -> {
                        sb.append(name).append(" = ").append(entry.value());
                        if (entry.isDefault()) {
                            sb.append(" (default)");
                        }
                        if (entry.isReadOnly()) {
                            sb.append(" (read-only)");
                        }
                        if (entry.source() != null) {
                            sb.append(" [Source: ").append(entry.source()).append("]");
                        }
                        sb.append("\n");
                    });
                    topicConfigArea.setText(sb.toString());
                } catch (InterruptedException | ExecutionException ex) {
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    log.error("Error describing configurations for topic '{}': {}", topicName, cause.getMessage(),
                            cause);
                    topicConfigArea.setText("Error loading configs: " + cause.getMessage());
                }
            }
        }.execute();
    }

    /**
     * Просмотр партиций выбранного топика.
     */
    private void viewSelectedTopicPartitions() {
        if (adminClient == null) {
            statusMessage("Not connected to Kafka.", JOptionPane.WARNING_MESSAGE);
            return;
        }

        int selectedRow = topicsTable.getSelectedRow();
        if (selectedRow == -1) {
            JOptionPane.showMessageDialog(this, "Please select a topic to view partitions.", "No Topic Selected",
                    JOptionPane.WARNING_MESSAGE);
            return;
        }

        String topicName = (String) topicsTableModel.getValueAt(selectedRow, 0);
        statusMessage("Loading partitions for topic: " + topicName + "...", JOptionPane.INFORMATION_MESSAGE);
        viewPartitionsButton.setEnabled(false);
        partitionsTableModel.setRowCount(0); // Очищаем таблицу партиций

        new SwingWorker<TopicDescription, Void>() {
            @Override
            protected TopicDescription doInBackground() throws Exception {
                // Получаем подробное описание только для выбранного топика
                return adminClient.describeTopics(Collections.singletonList(topicName)).all().get().get(topicName);
            }

            @Override
            protected void done() {
                try {
                    TopicDescription description = get();
                    if (description != null) {
                        for (TopicPartitionInfo partition : description.partitions()) {
                            String leader = "N/A";
                            if (partition.leader() != null) {
                                leader = String.valueOf(partition.leader().id());
                            }
                            String replicas = partition.replicas().stream()
                                    .map(Node::id)
                                    .map(String::valueOf)
                                    .collect(Collectors.joining(", "));
                            String isr = partition.isr().stream()
                                    .map(Node::id)
                                    .map(String::valueOf)
                                    .collect(Collectors.joining(", "));

                            partitionsTableModel.addRow(new Object[] {
                                    partition.partition(),
                                    leader,
                                    replicas,
                                    isr
                            });
                        }
                        // Автоматически загружаем настройки топика при просмотре партиций
                        describeTopicConfigs(topicName);
                        statusMessage("Partitions loaded for topic: " + topicName, JOptionPane.INFORMATION_MESSAGE);
                    } else {
                        statusMessage("Topic description not found for: " + topicName, JOptionPane.WARNING_MESSAGE);
                    }
                } catch (InterruptedException | ExecutionException ex) {
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    log.error("Error loading partitions for topic '{}': {}", topicName, cause.getMessage(), cause);
                    statusMessage("Error loading partitions: " + cause.getMessage(), JOptionPane.ERROR_MESSAGE);
                } finally {
                    viewPartitionsButton.setEnabled(true);
                }
            }
        }.execute();
    }

    /**
     * Вспомогательный метод для отображения сообщений статуса в всплывающем окне и
     * в логах.
     */
    private void statusMessage(String message, int messageType) {
        // Мы могли бы использовать JLabel для статуса, но для демонстрации JOptionPane
        // достаточно.
        // JOptionPane.showMessageDialog(this, message, "Info", messageType); // Слишком
        // много всплывающих окон
        log.info("Status Update: {}", message);
    }
}