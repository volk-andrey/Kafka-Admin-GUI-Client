package com.mycompany.kafkaadmin.dialog;

import javax.swing.*;
import java.awt.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConnectionSettingsDialog extends JDialog {

    public JTextField bootstrapServersField;
    public JComboBox<String> securityProtocolComboBox;
    public JTextField saslUsernameField;
    public JPasswordField saslPasswordField;
    public JTextField sslTruststoreLocationField;
    public JPasswordField sslTruststorePasswordField;
    public JTextField sslKeystoreLocationField;
    public JPasswordField sslKeystorePasswordField;
    private JLabel statusLabel;

    private boolean confirmed = false; // Флаг, был ли диалог подтвержден (OK)

    public ConnectionSettingsDialog(Frame owner) {
        super(owner, "Kafka Connection Settings", true); // Модальное окно
        setLayout(new BorderLayout(10, 10));
        setResizable(false);

        // --- Главная панель для ввода данных ---
        JPanel inputPanel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.fill = GridBagConstraints.HORIZONTAL;

        int row = 0;

        // Bootstrap Servers
        gbc.gridx = 0; gbc.gridy = row; gbc.anchor = GridBagConstraints.WEST;
        inputPanel.add(new JLabel("Bootstrap Servers:"), gbc);
        gbc.gridx = 1; gbc.gridy = row++; gbc.weightx = 1.0;
        bootstrapServersField = new JTextField("localhost:9092", 30);
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

        // --- Панель с кнопками ОК и Отмена ---
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

        // Изначально обновляем видимость полей
        updateFieldsVisibility();

        pack(); // Подгоняем размер окна под содержимое
        setLocationRelativeTo(owner); // Центрируем по отношению к родительскому окну
    }

    /**
     * Обновляет видимость полей в зависимости от выбранного протокола безопасности.
     */
    private void updateFieldsVisibility() {
        String selectedProtocol = (String) securityProtocolComboBox.getSelectedItem();

        boolean isSasl = selectedProtocol != null && (selectedProtocol.startsWith("SASL_"));
        boolean isSsl = selectedProtocol != null && (selectedProtocol.endsWith("_SSL") || selectedProtocol.equals("SSL"));

        saslUsernameField.setEnabled(isSasl);
        saslPasswordField.setEnabled(isSasl);

        sslTruststoreLocationField.setEnabled(isSsl);
        sslTruststorePasswordField.setEnabled(isSsl);
        sslKeystoreLocationField.setEnabled(isSsl); // Keystore нужен только для клиентской аутентификации
        sslKeystorePasswordField.setEnabled(isSsl); // Keystore нужен только для клиентской аутентификации

        // Очищаем поля, если они отключены
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

    /**
     * Возвращает true, если диалог был подтвержден кнопкой OK.
     */
    public boolean isConfirmed() {
        return confirmed;
    }

    /**
     * Собирает все введенные пользователем параметры в объект Properties,
     * который можно передать в AdminClientConfig.
     */
    public Properties getKafkaConnectionProperties() {
        Properties props = new Properties();

        String bootstrapServers = bootstrapServersField.getText().trim();
        if (!bootstrapServers.isEmpty()) {
            props.put("bootstrap.servers", bootstrapServers);
        }

        String securityProtocol = (String) securityProtocolComboBox.getSelectedItem();
        if (securityProtocol != null && !securityProtocol.equals("PLAINTEXT")) { // PLAINTEXT - это по умолчанию, можно не указывать
            props.put("security.protocol", securityProtocol);
        }

        // SASL Properties
        if (securityProtocol != null && securityProtocol.startsWith("SASL_")) {
            String username = saslUsernameField.getText().trim();
            String password = new String(saslPasswordField.getPassword()).trim();
            if (!username.isEmpty() && !password.isEmpty()) {
                // Это минимальные настройки для SASL/PLAIN. Для других механизмов могут понадобиться дополнительные.
                props.put("sasl.mechanism", "PLAIN"); // Или SCRAM-SHA-256, SCRAM-SHA-512
                props.put("sasl.jaas.config", String.format(
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                        username, password
                ));
            } else {
                // Если выбран SASL, но поля пустые, это ошибка
                JOptionPane.showMessageDialog(this, "SASL username and password are required for " + securityProtocol + ".", "Input Error", JOptionPane.ERROR_MESSAGE);
                return null; // Возвращаем null, чтобы сигнализировать об ошибке
            }
        }

        // SSL Properties
        if (securityProtocol != null && (securityProtocol.endsWith("_SSL") || securityProtocol.equals("SSL"))) {
            String truststoreLocation = sslTruststoreLocationField.getText().trim();
            String truststorePassword = new String(sslTruststorePasswordField.getPassword()).trim();
            String keystoreLocation = sslKeystoreLocationField.getText().trim();
            String keystorePassword = new String(sslKeystorePasswordField.getPassword()).trim();

            if (!truststoreLocation.isEmpty()) {
                props.put("ssl.truststore.location", truststoreLocation);
                props.put("ssl.truststore.password", truststorePassword);
            } else {
                JOptionPane.showMessageDialog(this, "SSL Truststore Location is required for " + securityProtocol + ".", "Input Error", JOptionPane.ERROR_MESSAGE);
                return null;
            }

            // Client authentication (optional)
            if (!keystoreLocation.isEmpty()) {
                props.put("ssl.keystore.location", keystoreLocation);
                props.put("ssl.keystore.password", keystorePassword);
            }
            // else, если keystoreLocation пуст, это не ошибка, просто нет клиентской аутентификации
        }
        return props;
    }
}