package com.mycompany.kafkaadmin.dialog;

import com.mycompany.kafkaadmin.cluster.ClusterConfig;

import javax.swing.*;
import java.awt.*;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConnectionSettingsDialog extends JDialog {

    private JTextField configNameField; // Новое поле для имени конфигурации
    JTextField bootstrapServersField;
    JComboBox<String> securityProtocolComboBox;
    JTextField saslUsernameField;
    JPasswordField saslPasswordField;
    JTextField sslTruststoreLocationField;
    JPasswordField sslTruststorePasswordField;
    JTextField sslKeystoreLocationField;
    JPasswordField sslKeystorePasswordField;

    private boolean confirmed = false;
    private ClusterConfig currentConfig; // Для редактирования существующей конфигурации

    public ConnectionSettingsDialog(Frame owner) {
        this(owner, null); // Вызов основного конструктора без начальной конфигурации
    }

    public ConnectionSettingsDialog(Frame owner, ClusterConfig configToEdit) {
        super(owner, "Kafka Connection Settings", true);
        setLayout(new BorderLayout(10, 10));
        setResizable(false);

        this.currentConfig = configToEdit;

        JPanel inputPanel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.fill = GridBagConstraints.HORIZONTAL;

        int row = 0;

        // Config Name (новое поле)
        gbc.gridx = 0; gbc.gridy = row; gbc.anchor = GridBagConstraints.WEST;
        inputPanel.add(new JLabel("Configuration Name:"), gbc);
        gbc.gridx = 1; gbc.gridy = row++; gbc.weightx = 1.0;
        configNameField = new JTextField(30);
        inputPanel.add(configNameField, gbc);

        // Bootstrap Servers
        gbc.gridx = 0; gbc.gridy = row; gbc.anchor = GridBagConstraints.WEST;
        inputPanel.add(new JLabel("Bootstrap Servers:"), gbc);
        gbc.gridx = 1; gbc.gridy = row++; gbc.weightx = 1.0;
        bootstrapServersField = new JTextField(30);
        inputPanel.add(bootstrapServersField, gbc);

        // Security Protocol
        gbc.gridx = 0; gbc.gridy = row;
        inputPanel.add(new JLabel("Security Protocol:"), gbc);
        gbc.gridx = 1; gbc.gridy = row++;
        securityProtocolComboBox = new JComboBox<>(new String[]{"PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL", "SSL"});
        securityProtocolComboBox.addActionListener(e -> updateFieldsVisibility());
        inputPanel.add(securityProtocolComboBox, gbc);

        // SASL Username
        gbc.gridx = 0; gbc.gridy = row;
        inputPanel.add(new JLabel("SASL Username:"), gbc);
        gbc.gridx = 1; gbc.gridy = row++;
        saslUsernameField = new JTextField(30);
        inputPanel.add(saslUsernameField, gbc);

        // SASL Password
        gbc.gridx = 0; gbc.gridy = row;
        inputPanel.add(new JLabel("SASL Password:"), gbc);
        gbc.gridx = 1; gbc.gridy = row++;
        saslPasswordField = new JPasswordField(30);
        inputPanel.add(saslPasswordField, gbc);

        // SSL Truststore Location
        gbc.gridx = 0; gbc.gridy = row;
        inputPanel.add(new JLabel("SSL Truststore Location:"), gbc);
        gbc.gridx = 1; gbc.gridy = row++;
        sslTruststoreLocationField = new JTextField(30);
        inputPanel.add(sslTruststoreLocationField, gbc);

        // SSL Truststore Password
        gbc.gridx = 0; gbc.gridy = row;
        inputPanel.add(new JLabel("SSL Truststore Password:"), gbc);
        gbc.gridx = 1; gbc.gridy = row++;
        sslTruststorePasswordField = new JPasswordField(30);
        inputPanel.add(sslTruststorePasswordField, gbc);

        // SSL Keystore Location (Optional for client auth)
        gbc.gridx = 0; gbc.gridy = row;
        inputPanel.add(new JLabel("SSL Keystore Location (Client Auth):"), gbc);
        gbc.gridx = 1; gbc.gridy = row++;
        sslKeystoreLocationField = new JTextField(30);
        inputPanel.add(sslKeystoreLocationField, gbc);

        // SSL Keystore Password (Optional for client auth)
        gbc.gridx = 0; gbc.gridy = row;
        inputPanel.add(new JLabel("SSL Keystore Password (Client Auth):"), gbc);
        gbc.gridx = 1; gbc.gridy = row++;
        sslKeystorePasswordField = new JPasswordField(30);
        inputPanel.add(sslKeystorePasswordField, gbc);


        add(inputPanel, BorderLayout.CENTER);

        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton okButton = new JButton("OK");
        okButton.addActionListener(e -> {
            confirmed = true;
            setVisible(false);
            dispose();
        });
        JButton cancelButton = new JButton("Cancel");
        cancelButton.addActionListener(e -> {
            confirmed = false;
            setVisible(false);
            dispose();
        });
        buttonPanel.add(okButton);
        buttonPanel.add(cancelButton);

        add(buttonPanel, BorderLayout.SOUTH);

        // Предзаполнение полей, если редактируется существующая конфигурация
        if (currentConfig != null) {
            populateFields(currentConfig);
        } else {
            // Если создаем новую, можно задать начальные значения
            bootstrapServersField.setText("localhost:9092");
            securityProtocolComboBox.setSelectedItem("PLAINTEXT");
        }

        updateFieldsVisibility(); // Изначально обновляем видимость полей
        pack();
        setLocationRelativeTo(owner);
    }

    private void populateFields(ClusterConfig config) {
        configNameField.setText(config.getName());
        Properties props = config.getConnectionProperties();

        bootstrapServersField.setText(props.getProperty("bootstrap.servers", ""));
        securityProtocolComboBox.setSelectedItem(props.getProperty("security.protocol", "PLAINTEXT"));

        String securityProtocol = (String) securityProtocolComboBox.getSelectedItem();

        // Populate SASL fields
        if (securityProtocol != null && securityProtocol.startsWith("SASL_")) {
            String jaasConfig = props.getProperty("sasl.jaas.config", "");
            if (jaasConfig.contains("username=\"") && jaasConfig.contains("password=\"")) {
                int userStart = jaasConfig.indexOf("username=\"") + "username=\"".length();
                int userEnd = jaasConfig.indexOf("\"", userStart);
                String username = jaasConfig.substring(userStart, userEnd);
                saslUsernameField.setText(username);

                int passStart = jaasConfig.indexOf("password=\"") + "password=\"".length();
                int passEnd = jaasConfig.indexOf("\"", passStart);
                String password = jaasConfig.substring(passStart, passEnd);
                saslPasswordField.setText(password);
            }
        }

        // Populate SSL fields
        if (securityProtocol != null && (securityProtocol.endsWith("_SSL") || securityProtocol.equals("SSL"))) {
            sslTruststoreLocationField.setText(props.getProperty("ssl.truststore.location", ""));
            sslTruststorePasswordField.setText(props.getProperty("ssl.truststore.password", ""));
            sslKeystoreLocationField.setText(props.getProperty("ssl.keystore.location", ""));
            sslKeystorePasswordField.setText(props.getProperty("ssl.keystore.password", ""));
        }
    }

    private void updateFieldsVisibility() {
        String selectedProtocol = (String) securityProtocolComboBox.getSelectedItem();

        boolean isSasl = selectedProtocol != null && (selectedProtocol.startsWith("SASL_"));
        boolean isSsl = selectedProtocol != null && (selectedProtocol.endsWith("_SSL") || selectedProtocol.equals("SSL"));

        saslUsernameField.setEnabled(isSasl);
        saslPasswordField.setEnabled(isSasl);

        sslTruststoreLocationField.setEnabled(isSsl);
        sslTruststorePasswordField.setEnabled(isSsl);
        sslKeystoreLocationField.setEnabled(isSsl);
        sslKeystorePasswordField.setEnabled(isSsl);

        if (!isSasl) {
            saslUsernameField.setText("");
            saslPasswordField.setText("");
        }
        if (!isSsl) {
            sslTruststoreLocationField.setText("");
            sslTruststorePasswordField.setText("");
            sslKeystoreLocationField.setText("");
            sslKeystorePasswordField.setText("");
        }
    }

    public boolean isConfirmed() {
        return confirmed;
    }

    /**
     * Создает или обновляет ClusterConfig из введенных данных.
     * Возвращает null, если есть ошибки валидации.
     */
    public ClusterConfig getClusterConfig() {
        String configName = configNameField.getText().trim();
        if (configName.isEmpty()) {
            JOptionPane.showMessageDialog(this, "Configuration Name cannot be empty.", "Input Error", JOptionPane.ERROR_MESSAGE);
            return null;
        }

        Properties props = new Properties();

        String bootstrapServers = bootstrapServersField.getText().trim();
        if (bootstrapServers.isEmpty()) {
            JOptionPane.showMessageDialog(this, "Bootstrap Servers cannot be empty.", "Input Error", JOptionPane.ERROR_MESSAGE);
            return null;
        }
        props.put("bootstrap.servers", bootstrapServers);

        String securityProtocol = (String) securityProtocolComboBox.getSelectedItem();
        if (securityProtocol != null && !securityProtocol.equals("PLAINTEXT")) {
            props.put("security.protocol", securityProtocol);
        }

        if (securityProtocol != null && securityProtocol.startsWith("SASL_")) {
            String username = saslUsernameField.getText().trim();
            String password = new String(saslPasswordField.getPassword()).trim();
            if (!username.isEmpty() && !password.isEmpty()) {
                props.put("sasl.mechanism", "PLAIN"); // По умолчанию PLAIN
                props.put("sasl.jaas.config", String.format(
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                        username, password
                ));
            } else {
                JOptionPane.showMessageDialog(this, "SASL username and password are required for " + securityProtocol + ".", "Input Error", JOptionPane.ERROR_MESSAGE);
                return null;
            }
        }

        if (securityProtocol != null && (securityProtocol.endsWith("_SSL") || securityProtocol.equals("SSL"))) {
            String truststoreLocation = sslTruststoreLocationField.getText().trim();
            String truststorePassword = new String(sslTruststorePasswordField.getPassword()).trim();
            String keystoreLocation = sslKeystoreLocationField.getText().trim();
            String keystorePassword = new String(sslKeystorePasswordField.getPassword()).trim();

            if (!truststoreLocation.isEmpty()) {
                props.put("ssl.truststore.location", truststoreLocation);
                props.put("ssl.truststore.password", truststorePassword);

                // Дополнительная проверка на существование файла Truststore
                try (InputStream is = new FileInputStream(truststoreLocation)) {
                    // Просто пытаемся открыть, чтобы убедиться, что файл существует и доступен
                } catch (FileNotFoundException e) {
                    JOptionPane.showMessageDialog(this, "SSL Truststore file not found: " + truststoreLocation, "File Error", JOptionPane.ERROR_MESSAGE);
                    return null;
                } catch (IOException e) {
                    JOptionPane.showMessageDialog(this, "Error accessing SSL Truststore file: " + truststoreLocation + "\n" + e.getMessage(), "File Error", JOptionPane.ERROR_MESSAGE);
                    return null;
                }

            } else {
                JOptionPane.showMessageDialog(this, "SSL Truststore Location is required for " + securityProtocol + ".", "Input Error", JOptionPane.ERROR_MESSAGE);
                return null;
            }

            if (!keystoreLocation.isEmpty()) {
                props.put("ssl.keystore.location", keystoreLocation);
                props.put("ssl.keystore.password", keystorePassword);
                try (InputStream is = new FileInputStream(keystoreLocation)) {
                    // Просто пытаемся открыть, чтобы убедиться, что файл существует и доступен
                } catch (FileNotFoundException e) {
                    JOptionPane.showMessageDialog(this, "SSL Keystore file not found: " + keystoreLocation, "File Error", JOptionPane.ERROR_MESSAGE);
                    return null;
                } catch (IOException e) {
                    JOptionPane.showMessageDialog(this, "Error accessing SSL Keystore file: " + keystoreLocation + "\n" + e.getMessage(), "File Error", JOptionPane.ERROR_MESSAGE);
                    return null;
                }
            }
        }

        if (currentConfig != null) {
            // Обновляем существующую конфигурацию
            currentConfig.setName(configName);
            currentConfig.setConnectionProperties(props);
            return currentConfig;
        } else {
            // Создаем новую конфигурацию
            return new ClusterConfig(configName, props);
        }
    }
}