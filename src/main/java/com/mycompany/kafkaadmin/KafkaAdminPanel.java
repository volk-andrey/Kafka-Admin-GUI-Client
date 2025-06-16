package com.mycompany.kafkaadmin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import javax.swing.*;
import java.awt.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaAdminPanel extends JPanel {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminPanel.class);

    private JTextField bootstrapServersField;
    private JButton connectButton;
    private JLabel statusLabel;
    private AdminClient adminClient;

    private JTabbedPane mainTabbedPane; // Добавляем JTabbedPane

    // Панели для различных функций (пока объявим, реализуем позже)
    private JPanel topicsPanel;
    private JPanel aclPanel;
    private JPanel partitionsPanel; // Для просмотра партиций, возможно, интегрируем в topicsPanel

    public KafkaAdminPanel() {
        setLayout(new BorderLayout());

        // --- Панель подключения ---
        JPanel connectionPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        connectionPanel.setBorder(BorderFactory.createTitledBorder("Kafka Connection"));

        connectionPanel.add(new JLabel("Bootstrap Servers:"));
        bootstrapServersField = new JTextField("localhost:9092", 25);
        connectionPanel.add(bootstrapServersField);

        connectButton = new JButton("Connect");
        // При подключении, мы хотим активировать вкладки
        connectButton.addActionListener(e -> connectToKafka());
        connectionPanel.add(connectButton);

        statusLabel = new JLabel("Status: Disconnected");
        connectionPanel.add(statusLabel);

        add(connectionPanel, BorderLayout.NORTH);

        // --- Основное содержимое: JTabbedPane ---
        mainTabbedPane = new JTabbedPane();
        mainTabbedPane.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5)); // Небольшие отступы

        // Инициализируем панели вкладок (пока они пустые, но позже будут содержать функционал)
        topicsPanel = new JPanel();
        topicsPanel.add(new JLabel("Topic management goes here")); // Заглушка
        mainTabbedPane.addTab("Topics", topicsPanel);

        aclPanel = new JPanel();
        aclPanel.add(new JLabel("ACL management goes here")); // Заглушка
        mainTabbedPane.addTab("ACLs", aclPanel);

        // По умолчанию вкладки будут отключены, пока не будет успешного подключения
        setTabsEnabled(false);

        add(mainTabbedPane, BorderLayout.CENTER); // Добавляем JTabbedPane в центр панели
    }

    /**
     * Метод для подключения к Kafka кластеру.
     */
    private void connectToKafka() {
        String bootstrapServers = bootstrapServersField.getText().trim();
        if (bootstrapServers.isEmpty()) {
            JOptionPane.showMessageDialog(this, "Bootstrap Servers cannot be empty!", "Connection Error", JOptionPane.ERROR_MESSAGE);
            return;
        }

        statusLabel.setText("Status: Connecting...");
        connectButton.setEnabled(false); // Отключаем кнопку во время попытки подключения
        setTabsEnabled(false); // Отключаем вкладки во время попытки подключения

        new SwingWorker<Boolean, Void>() {
            @Override
            protected Boolean doInBackground() throws Exception {
                Properties props = new Properties();
                props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

                try {
                    // Если AdminClient уже существует и подключен, закроем его перед переподключением
                    if (adminClient != null) {
                        adminClient.close();
                        adminClient = null;
                    }
                    adminClient = AdminClient.create(props);
                    // Проверяем подключение
                    adminClient.describeCluster().nodes().get();
                    log.info("Successfully connected to Kafka cluster at: {}", bootstrapServers);
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
                        statusLabel.setText("Status: Connected to " + bootstrapServers);
                        JOptionPane.showMessageDialog(KafkaAdminPanel.this, "Successfully connected to Kafka!", "Connection Success", JOptionPane.INFORMATION_MESSAGE);
                        setTabsEnabled(true); // Включаем вкладки после успешного подключения
                        // TODO: Здесь будем загружать данные в панели (например, список топиков)
                    }
                } catch (InterruptedException | ExecutionException ex) {
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    statusLabel.setText("Status: Connection Failed!");
                    JOptionPane.showMessageDialog(KafkaAdminPanel.this, "Failed to connect to Kafka: " + cause.getMessage(), "Connection Error", JOptionPane.ERROR_MESSAGE);
                    setTabsEnabled(false); // В случае ошибки оставляем вкладки отключенными
                } finally {
                    connectButton.setEnabled(true); // Включаем кнопку обратно
                }
            }
        }.execute();
    }

    /**
     * Метод для включения/выключения вкладок.
     * @param enabled true для включения, false для выключения.
     */
    private void setTabsEnabled(boolean enabled) {
        for (int i = 0; i < mainTabbedPane.getTabCount(); i++) {
            mainTabbedPane.setEnabledAt(i, enabled);
        }
    }

    /**
     * Метод для получения текущего AdminClient.
     * Другие панели будут использовать его для взаимодействия с Kafka.
     * @return AdminClient или null, если не подключено.
     */
    public AdminClient getAdminClient() {
        return adminClient;
    }

    /**
     * Метод для закрытия AdminClient при завершении работы приложения.
     */
    public void closeAdminClient() {
        if (adminClient != null) {
            adminClient.close();
            log.info("AdminClient closed.");
            adminClient = null;
        }
    }
}