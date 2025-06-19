package com.mycompany.kafkaadmin;

import com.mycompany.kafkaadmin.cluster.ClusterConfig;
import com.mycompany.kafkaadmin.cluster.ClusterConfigManager;
import com.mycompany.kafkaadmin.dialog.ConnectionSettingsDialog;
import org.apache.kafka.clients.admin.AdminClient;

import javax.swing.*;
import java.awt.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

public class KafkaAdminPanel extends JPanel {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminPanel.class);

    private JButton connectButton;
    private JButton addConfigButton; // Новая кнопка
    private JButton editConfigButton; // Новая кнопка
    private JButton removeConfigButton; // Новая кнопка
    private JComboBox<ClusterConfig> configSelector; // Компонент для выбора конфига
    private JLabel statusLabel;
    private AdminClient adminClient;

    private JTabbedPane mainTabbedPane;

    private TopicsPanel topicsPanel;
    private AclPanel aclPanel;

    private ClusterConfigManager configManager; // Менеджер конфигураций
    private ClusterConfig selectedConfig; // Текущая выбранная конфигурация

    public KafkaAdminPanel() {
        setLayout(new BorderLayout());

        configManager = new ClusterConfigManager(); // Инициализируем менеджер

        // --- Панель подключения ---
        JPanel connectionPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        connectionPanel.setBorder(BorderFactory.createTitledBorder("Kafka Connection"));

        // Селектор конфигураций
        configSelector = new JComboBox<>();
        configSelector.setPreferredSize(new Dimension(200, 25)); // Устанавливаем предпочтительный размер
        configSelector.addActionListener(e -> {
            selectedConfig = (ClusterConfig) configSelector.getSelectedItem();
            if (selectedConfig != null) {
                statusLabel.setText("Selected: " + selectedConfig.getName() + " (" + selectedConfig.getConnectionProperties().getProperty("bootstrap.servers", "") + ")");
            } else {
                statusLabel.setText("No configuration selected.");
            }
        });
        connectionPanel.add(configSelector);

        // Кнопки управления конфигурациями
        addConfigButton = new JButton("Add");
        addConfigButton.addActionListener(e -> addOrEditConfig(null)); // Добавление нового
        connectionPanel.add(addConfigButton);

        editConfigButton = new JButton("Edit");
        editConfigButton.addActionListener(e -> {
            ClusterConfig selected = (ClusterConfig) configSelector.getSelectedItem();
            if (selected != null) {
                addOrEditConfig(selected); // Редактирование выбранного
            } else {
                JOptionPane.showMessageDialog(this, "Please select a configuration to edit.", "No Selection", JOptionPane.INFORMATION_MESSAGE);
            }
        });
        connectionPanel.add(editConfigButton);

        removeConfigButton = new JButton("Remove");
        removeConfigButton.addActionListener(e -> removeSelectedConfig());
        connectionPanel.add(removeConfigButton);

        connectButton = new JButton("Connect");
        connectButton.addActionListener(e -> connectToKafka());
        connectionPanel.add(connectButton);

        statusLabel = new JLabel("Status: Disconnected");
        connectionPanel.add(statusLabel);

        add(connectionPanel, BorderLayout.NORTH);

        // --- Основное содержимое: JTabbedPane ---
        mainTabbedPane = new JTabbedPane();
        mainTabbedPane.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));

        topicsPanel = new TopicsPanel();
        mainTabbedPane.addTab("Topics", topicsPanel);

        aclPanel = new AclPanel();
        mainTabbedPane.addTab("ACLs", aclPanel);

        setTabsEnabled(false);

        add(mainTabbedPane, BorderLayout.CENTER);

        loadAndDisplayConfigs(); // Загружаем и отображаем конфигурации при запуске
    }

    /**
     * Загружает конфигурации из менеджера и обновляет JComboBox.
     */
    private void loadAndDisplayConfigs() {
        configSelector.removeAllItems();
        List<ClusterConfig> configs = configManager.getConfigs();
        for (ClusterConfig config : configs) {
            configSelector.addItem(config);
        }
        if (!configs.isEmpty()) {
            configSelector.setSelectedIndex(0); // Выбираем первую конфигурацию по умолчанию
        } else {
            statusLabel.setText("No saved configurations. Add a new one.");
        }
    }

    /**
     * Открывает диалог для добавления новой или редактирования существующей конфигурации.
     * @param configToEdit Конфигурация для редактирования, или null для добавления новой.
     */
    private void addOrEditConfig(ClusterConfig configToEdit) {
        Window window = SwingUtilities.getWindowAncestor(this);
        Frame ownerFrame = null;
        if (window instanceof Frame) {
            ownerFrame = (Frame) window;
        }

        ConnectionSettingsDialog dialog = new ConnectionSettingsDialog(ownerFrame, configToEdit);
        dialog.setVisible(true);

        if (dialog.isConfirmed()) {
            ClusterConfig newOrUpdatedConfig = dialog.getClusterConfig();
            if (newOrUpdatedConfig != null) {
                if (configToEdit == null) { // Добавление новой
                    configManager.addConfig(newOrUpdatedConfig);
                    JOptionPane.showMessageDialog(this, "Configuration '" + newOrUpdatedConfig.getName() + "' added.", "Success", JOptionPane.INFORMATION_MESSAGE);
                } else { // Обновление существующей
                    configManager.updateConfig(newOrUpdatedConfig);
                    JOptionPane.showMessageDialog(this, "Configuration '" + newOrUpdatedConfig.getName() + "' updated.", "Success", JOptionPane.INFORMATION_MESSAGE);
                }
                loadAndDisplayConfigs(); // Перезагружаем список в JComboBox
                configSelector.setSelectedItem(newOrUpdatedConfig); // Выбираем добавленную/обновленную
            }
        }
    }

    /**
     * Удаляет выбранную конфигурацию.
     */
    private void removeSelectedConfig() {
        ClusterConfig selected = (ClusterConfig) configSelector.getSelectedItem();
        if (selected == null) {
            JOptionPane.showMessageDialog(this, "Please select a configuration to remove.", "No Selection", JOptionPane.INFORMATION_MESSAGE);
            return;
        }

        int confirm = JOptionPane.showConfirmDialog(this,
                "Are you sure you want to remove configuration '" + selected.getName() + "'?",
                "Confirm Removal", JOptionPane.YES_NO_OPTION);

        if (confirm == JOptionPane.YES_OPTION) {
            configManager.removeConfig(selected.getId());
            loadAndDisplayConfigs(); // Обновляем список
            if (adminClient != null && selectedConfig != null && selectedConfig.equals(selected)) {
                // Если удаляем текущую активную конфигурацию, отключаемся
                closeAdminClient();
                statusLabel.setText("Status: Disconnected. Active configuration removed.");
                setTabsEnabled(false);
            } else {
                statusLabel.setText("Configuration '" + selected.getName() + "' removed.");
            }
        }
    }


    /**
     * Метод для подключения к Kafka кластеру.
     */
    private void connectToKafka() {
        if (selectedConfig == null) {
            JOptionPane.showMessageDialog(this, "Please select a cluster configuration or add a new one.", "Connection Error", JOptionPane.ERROR_MESSAGE);
            return;
        }

        Properties connectionProps = selectedConfig.getConnectionProperties();

        String bootstrapServers = connectionProps.getProperty("bootstrap.servers");
        statusLabel.setText("Status: Connecting to " + bootstrapServers + "...");
        connectButton.setEnabled(false);
        addConfigButton.setEnabled(false);
        editConfigButton.setEnabled(false);
        removeConfigButton.setEnabled(false);
        configSelector.setEnabled(false);
        setTabsEnabled(false);

        new SwingWorker<Boolean, Void>() {
            @Override
            protected Boolean doInBackground() throws Exception {
                try {
                    if (adminClient != null) {
                        adminClient.close();
                        adminClient = null;
                    }
                    adminClient = AdminClient.create(connectionProps);
                    adminClient.describeCluster().nodes().get();
                    log.info("Successfully connected to Kafka cluster: {}", selectedConfig.getName());
                    return true;
                } catch (Exception e) {
                    log.error("Failed to connect to Kafka cluster: {}", e.getMessage(), e);
                    if (adminClient != null) {
                        adminClient.close();
                        adminClient = null;
                    }
                    throw e;
                }
            }

            @Override
            protected void done() {
                try {
                    boolean success = get();
                    if (success) {
                        statusLabel.setText("Status: Connected to " + selectedConfig.getName());
                        JOptionPane.showMessageDialog(KafkaAdminPanel.this, "Successfully connected to Kafka!", "Connection Success", JOptionPane.INFORMATION_MESSAGE);
                        setTabsEnabled(true);
                        topicsPanel.setAdminClient(adminClient);
                        aclPanel.setAdminClient(adminClient);
                        mainTabbedPane.setSelectedComponent(topicsPanel);
                    }
                } catch (InterruptedException | ExecutionException ex) {
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    statusLabel.setText("Status: Connection Failed!");
                    JOptionPane.showMessageDialog(KafkaAdminPanel.this, "Failed to connect to Kafka: " + cause.getMessage(), "Connection Error", JOptionPane.ERROR_MESSAGE);
                    setTabsEnabled(false);
                } finally {
                    connectButton.setEnabled(true);
                    addConfigButton.setEnabled(true);
                    editConfigButton.setEnabled(true);
                    removeConfigButton.setEnabled(true);
                    configSelector.setEnabled(true);
                }
            }
        }.execute();
    }

    private void setTabsEnabled(boolean enabled) {
        for (int i = 0; i < mainTabbedPane.getTabCount(); i++) {
            mainTabbedPane.setEnabledAt(i, enabled);
        }
        if (enabled && adminClient != null) {
            if (topicsPanel != null) {
                topicsPanel.fetchTopics();
            }
            if (aclPanel != null) {
                aclPanel.fetchAcls();
            }
        }
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public void closeAdminClient() {
        if (adminClient != null) {
            adminClient.close();
            log.info("AdminClient closed.");
            adminClient = null;
        }
    }
}