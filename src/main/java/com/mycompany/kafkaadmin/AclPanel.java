package com.mycompany.kafkaadmin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.*;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableRowSorter;
import java.awt.*;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AclPanel extends JPanel {

    private static final Logger log = LoggerFactory.getLogger(AclPanel.class);

    private AdminClient adminClient;
    private DefaultTableModel aclTableModel;
    private JTable aclTable;
    private TableRowSorter<DefaultTableModel> tableSorter;
    private JButton refreshAclsButton;
    private JButton addAclButton;
    private JButton deleteAclButton;

    // Фильтры
    private JTextField principalFilterField;
    private JTextField hostFilterField;
    private JComboBox<String> operationFilterComboBox;
    private JComboBox<String> permissionTypeFilterComboBox;
    private JComboBox<String> resourceTypeFilterComboBox;
    private JTextField resourceNameFilterField;
    private JComboBox<String> patternTypeFilterComboBox;
    private JButton applyFilterButton;
    private JButton clearFilterButton;
    private JButton exportFilteredButton;

    // Опции фильтрации
    private JCheckBox caseSensitiveCheckBox;
    private JCheckBox regexCheckBox;

    // Хранилище всех ACL для фильтрации
    private List<AclBinding> allAcls = Collections.emptyList();

    public AclPanel() {
        setLayout(new BorderLayout(10, 10));

        // --- Панель фильтров ---
        JPanel filterPanel = createFilterPanel();

        // --- Верхняя панель с кнопками ---
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        refreshAclsButton = new JButton("Refresh ACLs");
        addAclButton = new JButton("Add ACL");
        deleteAclButton = new JButton("Delete ACL");

        buttonPanel.add(refreshAclsButton);
        buttonPanel.add(addAclButton);
        buttonPanel.add(deleteAclButton);

        // Создаем панель для верхней части (фильтры + кнопки)
        JPanel topPanel = new JPanel(new BorderLayout(5, 5));
        topPanel.add(filterPanel, BorderLayout.NORTH);
        topPanel.add(buttonPanel, BorderLayout.SOUTH);
        add(topPanel, BorderLayout.NORTH);

        // --- Таблица ACL ---
        String[] aclColumnNames = { "Principal", "Host", "Operation", "Permission Type", "Resource Type",
                "Resource Name", "Pattern Type" };
        aclTableModel = new DefaultTableModel(aclColumnNames, 0) {
            @Override
            public boolean isCellEditable(int row, int column) {
                return false; // Запрещаем редактирование ячеек таблицы
            }
        };
        aclTable = new JTable(aclTableModel);

        // Настраиваем сортировщик таблицы
        tableSorter = new TableRowSorter<>(aclTableModel);
        aclTable.setRowSorter(tableSorter);

        aclTable.getSelectionModel().addListSelectionListener(e -> updateButtonStates());
        JScrollPane aclScrollPane = new JScrollPane(aclTable);
        add(aclScrollPane, BorderLayout.CENTER);

        // --- Добавление слушателей кнопок ---
        refreshAclsButton.addActionListener(e -> fetchAcls());
        addAclButton.addActionListener(e -> showAddAclDialog());
        deleteAclButton.addActionListener(e -> deleteSelectedAcls());
        applyFilterButton.addActionListener(e -> applyFilters());
        clearFilterButton.addActionListener(e -> clearFilters());
        exportFilteredButton.addActionListener(e -> exportFilteredAcls());

        updateButtonStates(); // Изначально кнопки удаления должны быть неактивны
    }

    /**
     * Создает панель фильтров
     */
    private JPanel createFilterPanel() {
        JPanel filterPanel = new JPanel(new BorderLayout(5, 5));
        filterPanel.setBorder(BorderFactory.createTitledBorder("ACL Filters"));

        // Основная панель фильтров с вертикальным расположением
        JPanel filtersPanel = new JPanel();
        filtersPanel.setLayout(new BoxLayout(filtersPanel, BoxLayout.Y_AXIS));

        // Первая строка фильтров
        JPanel row1Panel = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 5));
        row1Panel.add(new JLabel("Principal:"));
        principalFilterField = new JTextField(15);
        principalFilterField
                .setToolTipText("Filter by principal (e.g., User:testuser). Use regex for complex patterns.");
        row1Panel.add(principalFilterField);

        row1Panel.add(new JLabel("Host:"));
        hostFilterField = new JTextField(15);
        hostFilterField.setToolTipText("Filter by host (e.g., *, 192.168.1.1). Use regex for IP ranges.");
        row1Panel.add(hostFilterField);

        row1Panel.add(new JLabel("Operation:"));
        operationFilterComboBox = new JComboBox<>();
        operationFilterComboBox.addItem("All");
        operationFilterComboBox.setToolTipText("Filter by operation type (READ, WRITE, etc.)");
        for (AclOperation op : AclOperation.values()) {
            if (op != AclOperation.ANY && op != AclOperation.UNKNOWN) {
                operationFilterComboBox.addItem(op.name());
            }
        }
        row1Panel.add(operationFilterComboBox);

        // Вторая строка фильтров
        JPanel row2Panel = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 5));
        row2Panel.add(new JLabel("Permission:"));
        permissionTypeFilterComboBox = new JComboBox<>();
        permissionTypeFilterComboBox.addItem("All");
        permissionTypeFilterComboBox.setToolTipText("Filter by permission type (ALLOW, DENY)");
        for (AclPermissionType perm : AclPermissionType.values()) {
            if (perm != AclPermissionType.ANY && perm != AclPermissionType.UNKNOWN) {
                permissionTypeFilterComboBox.addItem(perm.name());
            }
        }
        row2Panel.add(permissionTypeFilterComboBox);

        row2Panel.add(new JLabel("Resource Type:"));
        resourceTypeFilterComboBox = new JComboBox<>();
        resourceTypeFilterComboBox.addItem("All");
        resourceTypeFilterComboBox.setToolTipText("Filter by resource type (TOPIC, GROUP, CLUSTER, etc.)");
        for (ResourceType resType : ResourceType.values()) {
            if (resType != ResourceType.ANY && resType != ResourceType.UNKNOWN) {
                resourceTypeFilterComboBox.addItem(resType.name());
            }
        }
        row2Panel.add(resourceTypeFilterComboBox);

        row2Panel.add(new JLabel("Resource Name:"));
        resourceNameFilterField = new JTextField(15);
        resourceNameFilterField.setToolTipText("Filter by resource name (e.g., my-topic, *). Use regex for patterns.");
        row2Panel.add(resourceNameFilterField);

        // Третья строка фильтров
        JPanel row3Panel = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 5));
        row3Panel.add(new JLabel("Pattern Type:"));
        patternTypeFilterComboBox = new JComboBox<>();
        patternTypeFilterComboBox.addItem("All");
        patternTypeFilterComboBox.setToolTipText("Filter by pattern type (LITERAL, PREFIX, etc.)");
        for (PatternType patType : PatternType.values()) {
            if (patType != PatternType.UNKNOWN) {
                patternTypeFilterComboBox.addItem(patType.name());
            }
        }
        row3Panel.add(patternTypeFilterComboBox);

        // Опции фильтрации
        caseSensitiveCheckBox = new JCheckBox("Case Sensitive");
        caseSensitiveCheckBox.setToolTipText("Enable case-sensitive search");
        row3Panel.add(caseSensitiveCheckBox);

        regexCheckBox = new JCheckBox("Use Regex");
        regexCheckBox
                .setToolTipText("Enable regular expression search (e.g., .*test.* for any string containing 'test')");
        row3Panel.add(regexCheckBox);

        // Добавляем строки в основную панель
        filtersPanel.add(row1Panel);
        filtersPanel.add(Box.createVerticalStrut(5)); // Небольшой отступ
        filtersPanel.add(row2Panel);
        filtersPanel.add(Box.createVerticalStrut(5)); // Небольшой отступ
        filtersPanel.add(row3Panel);

        // Кнопки фильтрации
        JPanel filterButtonsPanel = new JPanel(new FlowLayout(FlowLayout.CENTER, 10, 5));
        applyFilterButton = new JButton("Apply Filters");
        clearFilterButton = new JButton("Clear Filters");
        exportFilteredButton = new JButton("Export Filtered ACLs");
        filterButtonsPanel.add(applyFilterButton);
        filterButtonsPanel.add(clearFilterButton);
        filterButtonsPanel.add(exportFilteredButton);

        filterPanel.add(filtersPanel, BorderLayout.CENTER);
        filterPanel.add(filterButtonsPanel, BorderLayout.SOUTH);

        return filterPanel;
    }

    /**
     * Применяет фильтры к таблице ACL
     */
    private void applyFilters() {
        if (allAcls.isEmpty()) {
            return;
        }

        // Очищаем таблицу
        aclTableModel.setRowCount(0);

        // Получаем значения фильтров
        String principalFilter = principalFilterField.getText().trim();
        String hostFilter = hostFilterField.getText().trim();
        String operationFilter = (String) operationFilterComboBox.getSelectedItem();
        String permissionFilter = (String) permissionTypeFilterComboBox.getSelectedItem();
        String resourceTypeFilter = (String) resourceTypeFilterComboBox.getSelectedItem();
        String resourceNameFilter = resourceNameFilterField.getText().trim();
        String patternTypeFilter = (String) patternTypeFilterComboBox.getSelectedItem();

        // Получаем опции фильтрации
        boolean caseSensitive = caseSensitiveCheckBox.isSelected();
        boolean useRegex = regexCheckBox.isSelected();

        // Компилируем регулярные выражения, если включены
        final Pattern principalPattern;
        final Pattern hostPattern;
        final Pattern resourceNamePattern;

        if (useRegex) {
            try {
                if (!principalFilter.isEmpty()) {
                    principalPattern = Pattern.compile(principalFilter, caseSensitive ? 0 : Pattern.CASE_INSENSITIVE);
                } else {
                    principalPattern = null;
                }
                if (!hostFilter.isEmpty()) {
                    hostPattern = Pattern.compile(hostFilter, caseSensitive ? 0 : Pattern.CASE_INSENSITIVE);
                } else {
                    hostPattern = null;
                }
                if (!resourceNameFilter.isEmpty()) {
                    resourceNamePattern = Pattern.compile(resourceNameFilter,
                            caseSensitive ? 0 : Pattern.CASE_INSENSITIVE);
                } else {
                    resourceNamePattern = null;
                }
            } catch (Exception e) {
                JOptionPane.showMessageDialog(this,
                        "Invalid regular expression: " + e.getMessage(),
                        "Regex Error",
                        JOptionPane.ERROR_MESSAGE);
                return;
            }
        } else {
            principalPattern = null;
            hostPattern = null;
            resourceNamePattern = null;
        }

        // Фильтруем ACL
        List<AclBinding> filteredAcls = allAcls.stream()
                .filter(acl -> {
                    // Фильтр по Principal
                    if (!principalFilter.isEmpty()) {
                        String principal = acl.entry().principal();
                        if (useRegex) {
                            if (principalPattern != null && !principalPattern.matcher(principal).find()) {
                                return false;
                            }
                        } else {
                            String principalToCheck = caseSensitive ? principal : principal.toLowerCase();
                            String filterToCheck = caseSensitive ? principalFilter : principalFilter.toLowerCase();
                            if (!principalToCheck.contains(filterToCheck)) {
                                return false;
                            }
                        }
                    }

                    // Фильтр по Host
                    if (!hostFilter.isEmpty()) {
                        String host = acl.entry().host();
                        if (useRegex) {
                            if (hostPattern != null && !hostPattern.matcher(host).find()) {
                                return false;
                            }
                        } else {
                            String hostToCheck = caseSensitive ? host : host.toLowerCase();
                            String filterToCheck = caseSensitive ? hostFilter : hostFilter.toLowerCase();
                            if (!hostToCheck.contains(filterToCheck)) {
                                return false;
                            }
                        }
                    }

                    // Фильтр по Operation
                    if (!"All".equals(operationFilter) &&
                            !acl.entry().operation().name().equals(operationFilter)) {
                        return false;
                    }

                    // Фильтр по Permission Type
                    if (!"All".equals(permissionFilter) &&
                            !acl.entry().permissionType().name().equals(permissionFilter)) {
                        return false;
                    }

                    // Фильтр по Resource Type
                    if (!"All".equals(resourceTypeFilter) &&
                            !acl.pattern().resourceType().name().equals(resourceTypeFilter)) {
                        return false;
                    }

                    // Фильтр по Resource Name
                    if (!resourceNameFilter.isEmpty()) {
                        String resourceName = acl.pattern().name();
                        if (useRegex) {
                            if (resourceNamePattern != null && !resourceNamePattern.matcher(resourceName).find()) {
                                return false;
                            }
                        } else {
                            String resourceToCheck = caseSensitive ? resourceName : resourceName.toLowerCase();
                            String filterToCheck = caseSensitive ? resourceNameFilter
                                    : resourceNameFilter.toLowerCase();
                            if (!resourceToCheck.contains(filterToCheck)) {
                                return false;
                            }
                        }
                    }

                    // Фильтр по Pattern Type
                    if (!"All".equals(patternTypeFilter) &&
                            !acl.pattern().patternType().name().equals(patternTypeFilter)) {
                        return false;
                    }

                    return true;
                })
                .collect(Collectors.toList());

        // Добавляем отфильтрованные ACL в таблицу
        for (AclBinding acl : filteredAcls) {
            aclTableModel.addRow(new Object[] {
                    acl.entry().principal(),
                    acl.entry().host(),
                    acl.entry().operation().name(),
                    acl.entry().permissionType().name(),
                    acl.pattern().resourceType().name(),
                    acl.pattern().name(),
                    acl.pattern().patternType().name()
            });
        }

        statusMessage("Filtered " + filteredAcls.size() + " ACL(s) from " + allAcls.size() + " total",
                JOptionPane.INFORMATION_MESSAGE);
    }

    /**
     * Очищает все фильтры и показывает все ACL
     */
    private void clearFilters() {
        principalFilterField.setText("");
        hostFilterField.setText("");
        operationFilterComboBox.setSelectedItem("All");
        permissionTypeFilterComboBox.setSelectedItem("All");
        resourceTypeFilterComboBox.setSelectedItem("All");
        resourceNameFilterField.setText("");
        patternTypeFilterComboBox.setSelectedItem("All");

        // Сбрасываем опции фильтрации
        caseSensitiveCheckBox.setSelected(false);
        regexCheckBox.setSelected(false);

        // Показываем все ACL
        displayAllAcls();
    }

    /**
     * Отображает все ACL в таблице
     */
    private void displayAllAcls() {
        aclTableModel.setRowCount(0);
        for (AclBinding acl : allAcls) {
            aclTableModel.addRow(new Object[] {
                    acl.entry().principal(),
                    acl.entry().host(),
                    acl.entry().operation().name(),
                    acl.entry().permissionType().name(),
                    acl.pattern().resourceType().name(),
                    acl.pattern().name(),
                    acl.pattern().patternType().name()
            });
        }
        statusMessage("Showing all " + allAcls.size() + " ACL(s)", JOptionPane.INFORMATION_MESSAGE);
    }

    /**
     * Устанавливает AdminClient для этой панели. Вызывается из KafkaAdminPanel.
     * 
     * @param adminClient
     */
    public void setAdminClient(AdminClient adminClient) {
        this.adminClient = adminClient;
        fetchAcls(); // После установки AdminClient, сразу загружаем ACL
    }

    /**
     * Обновляет состояние кнопок в зависимости от того, выбрана ли строка в таблице
     * ACL.
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

        new SwingWorker<Collection<AclBinding>, Void>() {
            @Override
            protected Collection<AclBinding> doInBackground() throws Exception {
                // Пустой фильтр возвращает все ACL
                DescribeAclsResult result = adminClient.describeAcls(AclBindingFilter.ANY);
                return result.values().get();
            }

            @Override
            protected void done() {
                try {
                    Collection<AclBinding> acls = get();
                    allAcls = new java.util.ArrayList<>(acls); // Сохраняем все ACL

                    // Отображаем все ACL
                    for (AclBinding acl : acls) {
                        aclTableModel.addRow(new Object[] {
                                acl.entry().principal(),
                                acl.entry().host(),
                                acl.entry().operation().name(),
                                acl.entry().permissionType().name(),
                                acl.pattern().resourceType().name(),
                                acl.pattern().name(),
                                acl.pattern().patternType().name()
                        });
                    }
                    statusMessage("ACLs loaded successfully. Total: " + acls.size(), JOptionPane.INFORMATION_MESSAGE);
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
        JTextField hostField = new JTextField("*", 20); // По умолчанию '*'
        JComboBox<AclOperation> operationComboBox = new JComboBox<>(AclOperation.values());
        JComboBox<AclPermissionType> permissionTypeComboBox = new JComboBox<>(AclPermissionType.values());
        JComboBox<ResourceType> resourceTypeComboBox = new JComboBox<>(ResourceType.values());
        JTextField resourceNameField = new JTextField("*", 20); // По умолчанию '*'
        JComboBox<PatternType> patternTypeComboBox = new JComboBox<>(PatternType.values());

        // Удаляем UNSUPPORTED, ANY, UNKNOWN из выпадающих списков, если они есть
        operationComboBox.removeItem(AclOperation.ANY);
        operationComboBox.removeItem(AclOperation.UNKNOWN);
        // operationComboBox.removeItem(AclOperation.UNSUPPORTED); // ИСПРАВЛЕНО:
        // UNSUPPORTED не существует

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
                JOptionPane.showMessageDialog(this, "All fields must be filled out!", "Input Error",
                        JOptionPane.ERROR_MESSAGE);
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
            JOptionPane.showMessageDialog(this, "Please select one or more ACLs to delete.", "No ACL Selected",
                    JOptionPane.WARNING_MESSAGE);
            return;
        }

        int confirm = JOptionPane.showConfirmDialog(this,
                "Are you sure you want to delete " + selectedRows.length + " selected ACL(s)?",
                "Confirm Delete", JOptionPane.YES_NO_OPTION);

        if (confirm == JOptionPane.YES_OPTION) {
            List<AclBindingFilter> filtersToDelete = new java.util.ArrayList<>();
            for (int viewRow : selectedRows) {
                // Конвертируем индекс представления в индекс модели для работы с сортировщиком
                int modelRow = aclTable.convertRowIndexToModel(viewRow);

                // Создаем AclBindingFilter на основе выбранной строки
                String principal = (String) aclTableModel.getValueAt(modelRow, 0);
                String host = (String) aclTableModel.getValueAt(modelRow, 1);
                AclOperation operation = AclOperation.valueOf((String) aclTableModel.getValueAt(modelRow, 2));
                AclPermissionType permissionType = AclPermissionType
                        .valueOf((String) aclTableModel.getValueAt(modelRow, 3));
                ResourceType resourceType = ResourceType.valueOf((String) aclTableModel.getValueAt(modelRow, 4));
                String resourceName = (String) aclTableModel.getValueAt(modelRow, 5);
                PatternType patternType = PatternType.valueOf((String) aclTableModel.getValueAt(modelRow, 6));

                // Создаем ResourcePatternFilter и AccessControlEntryFilter для удаления
                ResourcePatternFilter resourcePatternFilter = new ResourcePatternFilter(resourceType, resourceName,
                        patternType);
                AccessControlEntryFilter accessControlEntryFilter = new AccessControlEntryFilter(principal, host,
                        operation, permissionType);

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
     * Вспомогательный метод для отображения сообщений статуса в всплывающем окне и
     * в логах.
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

    /**
     * Экспортирует отфильтрованные ACL в CSV файл
     */
    private void exportFilteredAcls() {
        if (aclTableModel.getRowCount() == 0) {
            JOptionPane.showMessageDialog(this,
                    "No ACLs to export. Please apply filters first.",
                    "No Data",
                    JOptionPane.WARNING_MESSAGE);
            return;
        }

        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Export Filtered ACLs");
        fileChooser.setFileFilter(new javax.swing.filechooser.FileNameExtensionFilter("CSV Files", "csv"));
        fileChooser.setSelectedFile(new java.io.File("filtered_acls.csv"));

        if (fileChooser.showSaveDialog(this) == JFileChooser.APPROVE_OPTION) {
            java.io.File file = fileChooser.getSelectedFile();
            if (!file.getName().toLowerCase().endsWith(".csv")) {
                file = new java.io.File(file.getAbsolutePath() + ".csv");
            }

            try (java.io.PrintWriter writer = new java.io.PrintWriter(file, "UTF-8")) {
                // Записываем заголовки
                writer.println("Principal,Host,Operation,Permission Type,Resource Type,Resource Name,Pattern Type");

                // Записываем данные
                for (int i = 0; i < aclTableModel.getRowCount(); i++) {
                    StringBuilder line = new StringBuilder();
                    for (int j = 0; j < aclTableModel.getColumnCount(); j++) {
                        if (j > 0)
                            line.append(",");
                        String value = (String) aclTableModel.getValueAt(i, j);
                        // Экранируем запятые и кавычки в CSV
                        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
                            value = "\"" + value.replace("\"", "\"\"") + "\"";
                        }
                        line.append(value);
                    }
                    writer.println(line.toString());
                }

                statusMessage("Exported " + aclTableModel.getRowCount() + " ACL(s) to " + file.getName(),
                        JOptionPane.INFORMATION_MESSAGE);

            } catch (Exception e) {
                log.error("Error exporting ACLs: {}", e.getMessage(), e);
                JOptionPane.showMessageDialog(this,
                        "Error exporting ACLs: " + e.getMessage(),
                        "Export Error",
                        JOptionPane.ERROR_MESSAGE);
            }
        }
    }
}