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

    private JTabbedPane mainTabbedPane;

    // Объявляем нашу панель топиков
    private TopicsPanel topicsPanel;
    private JPanel aclPanel; // Пока оставим как JPanel, реализуем позже

    public KafkaAdminPanel() {
        setLayout(new BorderLayout());

        // --- Панель подключения ---
        JPanel connectionPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        connectionPanel.setBorder(BorderFactory.createTitledBorder("Kafka Connection"));

        connectionPanel.add(new JLabel("Bootstrap Servers:"));
        bootstrapServersField = new JTextField("localhost:9092", 25);
        connectionPanel.add(bootstrapServersField);

        connectButton = new JButton("Connect");
        connectButton.addActionListener(e -> connectToKafka());
        connectionPanel.add(connectButton);

        statusLabel = new JLabel("Status: Disconnected");
        connectionPanel.add(statusLabel);

        add(connectionPanel, BorderLayout.NORTH);

        // --- Основное содержимое: JTabbedPane ---
        mainTabbedPane = new JTabbedPane();
        mainTabbedPane.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));

        // Инициализируем TopicsPanel
        topicsPanel = new TopicsPanel();
        mainTabbedPane.addTab("Topics", topicsPanel);

        aclPanel = new JPanel();
        aclPanel.add(new JLabel("ACL management goes here"));
        mainTabbedPane.addTab("ACLs", aclPanel);

        setTabsEnabled(false); // По умолчанию вкладки отключены

        add(mainTabbedPane, BorderLayout.CENTER);
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
        connectButton.setEnabled(false);
        setTabsEnabled(false);

        new SwingWorker<Boolean, Void>() {
            @Override
            protected Boolean doInBackground() throws Exception {
                Properties props = new Properties();
                props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

                try {
                    if (adminClient != null) {
                        adminClient.close();
                        adminClient = null;
                    }
                    adminClient = AdminClient.create(props);
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
                        setTabsEnabled(true);
                        // После успешного подключения передаем AdminClient в TopicsPanel
                        topicsPanel.setAdminClient(adminClient);
                        // Можем переключиться на вкладку топиков по умолчанию
                        mainTabbedPane.setSelectedComponent(topicsPanel);
                    }
                } catch (InterruptedException | ExecutionException ex) {
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    statusLabel.setText("Status: Connection Failed!");
                    JOptionPane.showMessageDialog(KafkaAdminPanel.this, "Failed to connect to Kafka: " + cause.getMessage(), "Connection Error", JOptionPane.ERROR_MESSAGE);
                    setTabsEnabled(false);
                } finally {
                    connectButton.setEnabled(true);
                }
            }
        }.execute();
    }

    private void setTabsEnabled(boolean enabled) {
        for (int i = 0; i < mainTabbedPane.getTabCount(); i++) {
            mainTabbedPane.setEnabledAt(i, enabled);
        }
        // Когда вкладки включаются, вызываем fetchTopics() на TopicsPanel
        if (enabled && topicsPanel != null && adminClient != null) {
            topicsPanel.fetchTopics();
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