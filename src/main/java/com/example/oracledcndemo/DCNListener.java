package com.example.oracledcndemo;

import lombok.extern.slf4j.Slf4j;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleDriver;
import oracle.jdbc.OracleStatement;
import oracle.jdbc.dcn.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

@Component
@Slf4j
public class DCNListener implements DatabaseChangeListener {
    @Autowired
    private DriverManagerDataSource ds;

    private DatabaseChangeRegistration dcr = null;
    private OracleConnection oracleConnection;
    private String listenerQuery = "select * from book";
    PreparedStatement statement = null;

    @PostConstruct
    public void init() throws SQLException {
        initDB();
        registerListener();
        statement = oracleConnection.prepareStatement(listenerQuery);
    }

    private void registerListener() {
        Properties properties = new Properties();
        properties.put(OracleConnection.DCN_NOTIFY_ROWIDS, "true");
        properties.put(OracleConnection.NTF_QOS_RELIABLE, "true");

        Statement stmt = null;
        ResultSet rs = null;
        try {

            System.out.println("Creating DatabaseChangeRegistration ");

            dcr = oracleConnection.registerDatabaseChangeNotification(properties);
            System.out.println("Associate the statement with the registration ");
            stmt = oracleConnection.createStatement();
            // Associate the statement with the registration.
            ((OracleStatement) stmt).setDatabaseChangeRegistration(dcr);
            rs = stmt.executeQuery(listenerQuery);

            while (rs.next()) {
                // Do Nothing
            }
            System.out.println("Attaching the listener to the processor");
            dcr.addListener(this);
            System.out.println("Attached listener for " + listenerQuery);
            String[] tableNames = dcr.getTables();
            for (int i = 0; i < tableNames.length; i++) {
                System.out.println(tableNames[i] + " Successfully registered.");
            }

        } catch (SQLException e) {
            System.out.println("Error during listener registartion " + e);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    System.out.println("Error closing Statement " + e);
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    System.out.println("Error closing ResultSet " + e);
                }
            }
        }
    }

    private void initDB() throws SQLException {
        OracleDriver dr = new OracleDriver();
        Properties prop = new Properties();
        prop.setProperty("user", ds.getUsername());
        prop.setProperty("password", ds.getPassword());
        oracleConnection = (OracleConnection) dr.connect(ds.getUrl(), prop);
    }

    @Override
    public void onDatabaseChangeNotification(DatabaseChangeEvent databaseChangeEvent) {
        System.out.println("Event handler received event");
        TableChangeDescription[] tableChanges = databaseChangeEvent.getTableChangeDescription();
        for (TableChangeDescription tableChange : tableChanges) {
            RowChangeDescription[] rcds = tableChange.getRowChangeDescription();
            for (RowChangeDescription rcd : rcds) {
                System.out.println("Affected row : "
                        + rcd.getRowid().stringValue() + rcd
                        + " operation :");

                RowChangeDescription.RowOperation ro = rcd.getRowOperation();
                String rowId = rcd.getRowid().stringValue();

                if (ro.equals(RowChangeDescription.RowOperation.INSERT)) {
                    System.out.println("insert: " + rowId);
                } else if (ro.equals(RowChangeDescription.RowOperation.UPDATE)) {
                    System.out.println("update: " + rowId);
                } else if (ro.equals(RowChangeDescription.RowOperation.DELETE)) {
                    System.out.println("delete: " + rowId);
                } else {
                    System.out.println("Event Not Replicated - Only INSERT/DELETE/UPDATE are handled.");
                }
            }
        }
    }
}
