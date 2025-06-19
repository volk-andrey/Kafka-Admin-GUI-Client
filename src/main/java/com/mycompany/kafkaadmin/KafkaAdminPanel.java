package com.mycompany.kafkaadmin;

import com.mycompany.kafkaadmin.dialog.ConnectionSettingsDialog;
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

    private JButton connectButton;
    private JButton connectionSettingsButton;
    private JLabel statusLabel;
    private AdminClient adminClient;

    private JTabbedPane mainTabbedPane;

    private TopicsPanel topicsPanel;
    private AclPanel aclPanel;

    private Properties currentConnectionProps;

    public KafkaAdminPanel() {
        setLayout(new BorderLayout());

        // --- Панель подключения ---
        JPanel connectionPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        connectionPanel.setBorder(BorderFactory.createTitledBorder("Kafka Connection")); // ИСПРАВЛЕНО: BorderBorderFactory -> BorderFactory

        connectionSettingsButton = new JButton("Connection Settings");
        connectionSettingsButton.addActionListener(e -> showConnectionSettingsDialog());
        connectionPanel.add(connectionSettingsButton);

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

        currentConnectionProps = new Properties();
        currentConnectionProps.put("bootstrap.servers", "localhost:9092");
        statusLabel.setText("Ready to connect to " + currentConnectionProps.getProperty("bootstrap.servers"));
    }

    /**
     * Показывает диалоговое окно для настройки подключения.
     */
    private void showConnectionSettingsDialog() {
        // ИСПРАВЛЕНО: Преобразование Window в Frame
        Window window = SwingUtilities.getWindowAncestor(this);
        Frame ownerFrame = null;
        if (window instanceof Frame) {
            ownerFrame = (Frame) window;
        }
        // Если родительское окно не Frame, можно передать null или найти Frame выше
        ConnectionSettingsDialog dialog = new ConnectionSettingsDialog(ownerFrame);


        String currentBootstrap = currentConnectionProps.getProperty("bootstrap.servers", "");
        if (!currentBootstrap.isEmpty()) dialog.bootstrapServersField.setText(currentBootstrap);

        String securityProtocol = currentConnectionProps.getProperty("security.protocol", "PLAINTEXT");
        dialog.securityProtocolComboBox.setSelectedItem(securityProtocol);

        if (securityProtocol.startsWith("SASL_")) {
            String jaasConfig = currentConnectionProps.getProperty("sasl.jaas.config", "");
            if (jaasConfig.contains("username=\"") && jaasConfig.contains("password=\"")) {
                int userStart = jaasConfig.indexOf("username=\"") + "username=\"".length();
                int userEnd = jaasConfig.indexOf("\"", userStart);
                String username = jaasConfig.substring(userStart, userEnd);
                dialog.saslUsernameField.setText(username);

                int passStart = jaasConfig.indexOf("password=\"") + "password=\"".length();
                int passEnd = jaasConfig.indexOf("\"", passStart);
                String password = jaasConfig.substring(passStart, passEnd);
                dialog.saslPasswordField.setText(password);
            }
        }

        if (securityProtocol.endsWith("_SSL") || securityProtocol.equals("SSL")) {
            dialog.sslTruststoreLocationField.setText(currentConnectionProps.getProperty("ssl.truststore.location", ""));
            dialog.sslTruststorePasswordField.setText(currentConnectionProps.getProperty("ssl.truststore.password", ""));
            dialog.sslKeystoreLocationField.setText(currentConnectionProps.getProperty("ssl.keystore.location", ""));
            dialog.sslKeystorePasswordField.setText(currentConnectionProps.getProperty("ssl.keystore.password", ""));
        }

        dialog.setVisible(true);

        if (dialog.isConfirmed()) {
            Properties newProps = dialog.getKafkaConnectionProperties();
            if (newProps != null) {
                this.currentConnectionProps = newProps;
                statusLabel.setText("Connection settings updated. Ready to connect to " + currentConnectionProps.getProperty("bootstrap.servers"));
            }
        }
    }


    /**
     * Метод для подключения к Kafka кластеру.
     */
    private void connectToKafka() {
        if (currentConnectionProps == null || currentConnectionProps.isEmpty() || !currentConnectionProps.containsKey("bootstrap.servers")) {
            JOptionPane.showMessageDialog(this, "Please configure connection settings first!", "Connection Error", JOptionPane.ERROR_MESSAGE);
            showConnectionSettingsDialog();
            return;
        }

        String bootstrapServers = currentConnectionProps.getProperty("bootstrap.servers");
        statusLabel.setText("Status: Connecting to " + bootstrapServers + "...");
        connectButton.setEnabled(false);
        connectionSettingsButton.setEnabled(false);
        setTabsEnabled(false);

        new SwingWorker<Boolean, Void>() {
            @Override
            protected Boolean doInBackground() throws Exception {
                try {
                    if (adminClient != null) {
                        adminClient.close();
                        adminClient = null;
                    }
                    adminClient = AdminClient.create(currentConnectionProps);
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
                    connectionSettingsButton.setEnabled(true);
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