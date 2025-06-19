package com.mycompany.kafkaadmin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.*;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.util.Collection; // Изменено на Collection
import java.util.Collections;
import java.util.List;
import java.util.Set; // Хотя мы будем использовать Collection, Set для других целей можно оставить
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AclPanel extends JPanel {

    private static final Logger log = LoggerFactory.getLogger(AclPanel.class);

    private AdminClient adminClient;
    private DefaultTableModel aclTableModel;
    private JTable aclTable;
    private JButton refreshAclsButton;
    private JButton addAclButton;
    private JButton deleteAclButton;

    public AclPanel() {
        setLayout(new BorderLayout(10, 10));

        // --- Верхняя панель с кнопками ---
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        refreshAclsButton = new JButton("Refresh ACLs");
        addAclButton = new JButton("Add ACL");
        deleteAclButton = new JButton("Delete ACL");

        buttonPanel.add(refreshAclsButton);
        buttonPanel.add(addAclButton);
        buttonPanel.add(deleteAclButton);
        add(buttonPanel, BorderLayout.NORTH);

        // --- Таблица ACL ---
        String[] aclColumnNames = {"Principal", "Host", "Operation", "Permission Type", "Resource Type", "Resource Name", "Pattern Type"};
        aclTableModel = new DefaultTableModel(aclColumnNames, 0) {
            @Override
            public boolean isCellEditable(int row, int column) {
                return false; // Запрещаем редактирование ячеек таблицы
            }
        };
        aclTable = new JTable(aclTableModel);
        aclTable.getSelectionModel().addListSelectionListener(e -> updateButtonStates());
        JScrollPane aclScrollPane = new JScrollPane(aclTable);
        add(aclScrollPane, BorderLayout.CENTER);

        // --- Добавление слушателей кнопок ---
        refreshAclsButton.addActionListener(e -> fetchAcls());
        addAclButton.addActionListener(e -> showAddAclDialog());
        deleteAclButton.addActionListener(e -> deleteSelectedAcls());

        updateButtonStates(); // Изначально кнопки удаления должны быть неактивны
    }

    /**
     * Устанавливает AdminClient для этой панели. Вызывается из KafkaAdminPanel.
     * @param adminClient
     */
    public void setAdminClient(AdminClient adminClient) {
        this.adminClient = adminClient;
        fetchAcls(); // После установки AdminClient, сразу загружаем ACL
    }

    /**
     * Обновляет состояние кнопок в зависимости от того, выбрана ли строка в таблице ACL.
     */
    private void updateButtonStates() {
        boolean aclSelected = aclTable.getSelectedRow() != -1;
        deleteAclButton.setEnabled(aclSelected);
    }

    /**
     * Загружает список ACL из Kafka и отображает их в таблице.
     */
    public void fetchAcls() {
        if (adminClient == null) {
            log.warn("AdminClient is null. Cannot fetch ACLs.");
            statusMessage("Not connected to Kafka.", JOptionPane.WARNING_MESSAGE);
            return;
        }

        aclTableModel.setRowCount(0); // Очищаем таблицу перед загрузкой
        refreshAclsButton.setEnabled(false);
        statusMessage("Loading ACLs...", JOptionPane.INFORMATION_MESSAGE);

        new SwingWorker<Collection<AclBinding>, Void>() { // Изменено на Collection<AclBinding>
            @Override
            protected Collection<AclBinding> doInBackground() throws Exception { // Изменено на Collection<AclBinding>
                // Пустой фильтр возвращает все ACL
                DescribeAclsResult result = adminClient.describeAcls(AclBindingFilter.ANY);
                return result.values().get(); // Возвращает Collection
            }

            @Override
            protected void done() {
                try {
                    Collection<AclBinding> acls = get(); // Получаем Collection
                    for (AclBinding acl : acls) {
                        aclTableModel.addRow(new Object[]{
                                acl.entry().principal(), // Корректно
                                acl.entry().host(),     // Корректно
                                acl.entry().operation().name(), // Корректно
                                acl.entry().permissionType().name(), // Корректно
                                acl.pattern().resourceType().name(),
                                acl.pattern().name(),
                                acl.pattern().patternType().name()
                        });
                    }
                    statusMessage("ACLs loaded successfully.", JOptionPane.INFORMATION_MESSAGE);
                } catch (InterruptedException | ExecutionException ex) {
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    log.error("Error fetching ACLs: {}", cause.getMessage(), cause);
                    JOptionPane.showMessageDialog(AclPanel.this,
                            "Error loading ACLs: " + cause.getMessage(),
                            "ACL Loading Error",
                            JOptionPane.ERROR_MESSAGE);
                } finally {
                    refreshAclsButton.setEnabled(true);
                    updateButtonStates();
                }
            }
        }.execute();
    }

    /**
     * Показывает диалоговое окно для добавления нового ACL.
     */
    private void showAddAclDialog() {
        if (adminClient == null) {
            statusMessage("Not connected to Kafka.", JOptionPane.WARNING_MESSAGE);
            return;
        }

        JTextField principalField = new JTextField(20);
        JTextField hostField = new JTextField("*" , 20); // По умолчанию '*'
        JComboBox<AclOperation> operationComboBox = new JComboBox<>(AclOperation.values());
        JComboBox<AclPermissionType> permissionTypeComboBox = new JComboBox<>(AclPermissionType.values());
        JComboBox<ResourceType> resourceTypeComboBox = new JComboBox<>(ResourceType.values());
        JTextField resourceNameField = new JTextField("*" , 20); // По умолчанию '*'
        JComboBox<PatternType> patternTypeComboBox = new JComboBox<>(PatternType.values());

        // Удаляем UNSUPPORTED, ANY, UNKNOWN из выпадающих списков, если они есть
        operationComboBox.removeItem(AclOperation.ANY);
        operationComboBox.removeItem(AclOperation.UNKNOWN);
        // operationComboBox.removeItem(AclOperation.UNSUPPORTED); // ИСПРАВЛЕНО: UNSUPPORTED не существует

        permissionTypeComboBox.removeItem(AclPermissionType.ANY);
        permissionTypeComboBox.removeItem(AclPermissionType.UNKNOWN);

        resourceTypeComboBox.removeItem(ResourceType.ANY);
        resourceTypeComboBox.removeItem(ResourceType.UNKNOWN);

        patternTypeComboBox.removeItem(PatternType.UNKNOWN);

        // Устанавливаем значения по умолчанию для удобства
        operationComboBox.setSelectedItem(AclOperation.READ);
        permissionTypeComboBox.setSelectedItem(AclPermissionType.ALLOW);
        resourceTypeComboBox.setSelectedItem(ResourceType.TOPIC);
        patternTypeComboBox.setSelectedItem(PatternType.LITERAL);

        JPanel panel = new JPanel(new GridLayout(0, 2, 5, 5));
        panel.add(new JLabel("Principal (e.g., User:testuser):"));
        panel.add(principalField);
        panel.add(new JLabel("Host (e.g., * or 192.168.1.1):"));
        panel.add(hostField);
        panel.add(new JLabel("Operation:"));
        panel.add(operationComboBox);
        panel.add(new JLabel("Permission Type:"));
        panel.add(permissionTypeComboBox);
        panel.add(new JLabel("Resource Type:"));
        panel.add(resourceTypeComboBox);
        panel.add(new JLabel("Resource Name (e.g., my-topic or *):"));
        panel.add(resourceNameField);
        panel.add(new JLabel("Pattern Type:"));
        panel.add(patternTypeComboBox);


        int result = JOptionPane.showConfirmDialog(this, panel, "Add New ACL",
                JOptionPane.OK_CANCEL_OPTION, JOptionPane.PLAIN_MESSAGE);

        if (result == JOptionPane.OK_OPTION) {
            String principal = principalField.getText().trim();
            String host = hostField.getText().trim();
            AclOperation operation = (AclOperation) operationComboBox.getSelectedItem();
            AclPermissionType permissionType = (AclPermissionType) permissionTypeComboBox.getSelectedItem();
            ResourceType resourceType = (ResourceType) resourceTypeComboBox.getSelectedItem();
            String resourceName = resourceNameField.getText().trim();
            PatternType patternType = (PatternType) patternTypeComboBox.getSelectedItem();

            if (principal.isEmpty() || host.isEmpty() || resourceName.isEmpty()) {
                JOptionPane.showMessageDialog(this, "All fields must be filled out!", "Input Error", JOptionPane.ERROR_MESSAGE);
                return;
            }

            addAcl(principal, host, operation, permissionType, resourceType, resourceName, patternType);
        }
    }

    /**
     * Отправляет запрос на добавление ACL в Kafka.
     */
    private void addAcl(String principal, String host, AclOperation operation,
                        AclPermissionType permissionType, ResourceType resourceType,
                        String resourceName, PatternType patternType) {

        ResourcePattern resourcePattern = new ResourcePattern(resourceType, resourceName, patternType);
        AccessControlEntry accessControlEntry = new AccessControlEntry(principal, host, operation, permissionType);
        AclBinding aclBinding = new AclBinding(resourcePattern, accessControlEntry);

        statusMessage("Adding ACL for " + principal + " on " + resourceName + "...", JOptionPane.INFORMATION_MESSAGE);
        addAclButton.setEnabled(false);

        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                adminClient.createAcls(Collections.singletonList(aclBinding)).all().get();
                return null;
            }

            @Override
            protected void done() {
                try {
                    get();
                    statusMessage("ACL added successfully!", JOptionPane.INFORMATION_MESSAGE);
                    fetchAcls(); // Обновляем список ACL
                } catch (InterruptedException | ExecutionException ex) {
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    log.error("Error adding ACL: {}", cause.getMessage(), cause);
                    JOptionPane.showMessageDialog(AclPanel.this,
                            "Error adding ACL: " + cause.getMessage(),
                            "ACL Add Error",
                            JOptionPane.ERROR_MESSAGE);
                } finally {
                    addAclButton.setEnabled(true);
                }
            }
        }.execute();
    }

    /**
     * Удаляет выбранные ACL из таблицы.
     */
    private void deleteSelectedAcls() {
        if (adminClient == null) {
            statusMessage("Not connected to Kafka.", JOptionPane.WARNING_MESSAGE);
            return;
        }

        int[] selectedRows = aclTable.getSelectedRows();
        if (selectedRows.length == 0) {
            JOptionPane.showMessageDialog(this, "Please select one or more ACLs to delete.", "No ACL Selected", JOptionPane.WARNING_MESSAGE);
            return;
        }

        int confirm = JOptionPane.showConfirmDialog(this,
                "Are you sure you want to delete " + selectedRows.length + " selected ACL(s)?",
                "Confirm Delete", JOptionPane.YES_NO_OPTION);

        if (confirm == JOptionPane.YES_OPTION) {
            List<AclBindingFilter> filtersToDelete = new java.util.ArrayList<>();
            for (int i : selectedRows) {
                // Создаем AclBindingFilter на основе выбранной строки
                String principal = (String) aclTableModel.getValueAt(i, 0);
                String host = (String) aclTableModel.getValueAt(i, 1);
                AclOperation operation = AclOperation.valueOf((String) aclTableModel.getValueAt(i, 2));
                AclPermissionType permissionType = AclPermissionType.valueOf((String) aclTableModel.getValueAt(i, 3));
                ResourceType resourceType = ResourceType.valueOf((String) aclTableModel.getValueAt(i, 4));
                String resourceName = (String) aclTableModel.getValueAt(i, 5);
                PatternType patternType = PatternType.valueOf((String) aclTableModel.getValueAt(i, 6));

                // ИСПРАВЛЕНО: Создаем ResourcePatternFilter и AccessControlEntryFilter для удаления
                ResourcePatternFilter resourcePatternFilter = new ResourcePatternFilter(resourceType, resourceName, patternType);
                AccessControlEntryFilter accessControlEntryFilter = new AccessControlEntryFilter(principal, host, operation, permissionType);

                filtersToDelete.add(new AclBindingFilter(resourcePatternFilter, accessControlEntryFilter));
            }

            statusMessage("Deleting selected ACLs...", JOptionPane.INFORMATION_MESSAGE);
            deleteAclButton.setEnabled(false);

            new SwingWorker<Void, Void>() {
                @Override
                protected Void doInBackground() throws Exception {
                    adminClient.deleteAcls(filtersToDelete).all().get();
                    return null;
                }

                @Override
                protected void done() {
                    try {
                        get();
                        statusMessage("Selected ACL(s) deleted successfully!", JOptionPane.INFORMATION_MESSAGE);
                        fetchAcls(); // Обновляем список
                    } catch (InterruptedException | ExecutionException ex) {
                        Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                        log.error("Error deleting ACLs: {}", cause.getMessage(), cause);
                        JOptionPane.showMessageDialog(AclPanel.this,
                                "Error deleting ACL(s): " + cause.getMessage(),
                                "ACL Delete Error",
                                JOptionPane.ERROR_MESSAGE);
                    } finally {
                        deleteAclButton.setEnabled(true);
                    }
                }
            }.execute();
        }
    }

    /**
     * Вспомогательный метод для отображения сообщений статуса в всплывающем окне и в логах.
     */
    private void statusMessage(String message, int messageType) {
        log.info("Status Update: {}", message);
        // Для ERROR и WARNING все равно используем JOptionPane
        if (messageType == JOptionPane.ERROR_MESSAGE) {
            JOptionPane.showMessageDialog(this, message, "Error", messageType);
        } else if (messageType == JOptionPane.WARNING_MESSAGE) {
            JOptionPane.showMessageDialog(this, message, "Warning", messageType);
        }
    }
}