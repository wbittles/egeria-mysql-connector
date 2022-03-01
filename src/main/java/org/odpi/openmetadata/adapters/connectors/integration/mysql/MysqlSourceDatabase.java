/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */

package org.odpi.openmetadata.adapters.connectors.integration.mysql;

import org.odpi.openmetadata.adapters.connectors.integration.mysql.properties.MysqlColumn;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.properties.MysqlDatabase;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.properties.MysqlForeignKeyLinks;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.properties.MysqlSchema;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.properties.MysqlTable;
import org.odpi.openmetadata.frameworks.connectors.properties.ConnectionProperties;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/*
The mysqlSourceDatabase class abstracts away the connection to the database host system which is needed to gain a list of databases
 */
public class MysqlSourceDatabase
{
    /* used to cache the resilts of the getDatabaseInstance() */
    String instance = null;

    Properties mysqlProps = new Properties();

    public MysqlSourceDatabase(ConnectionProperties egeriaProps )
    {

        //TODO Can the configuration properties be <String,String> to avoid the conversion
        Map<String, Object> objProps = egeriaProps.getConfigurationProperties();

        if( objProps !=null )
        {
            for(Map.Entry<String,Object> obj : objProps.entrySet())
            {
                if(obj.getValue() instanceof String)
                {
                    mysqlProps.put(obj.getKey(), String.valueOf(obj.getValue()));
                }
            }
            //TODO YIKES and YUK
            mysqlProps.setProperty("user", egeriaProps.getUserId());
            mysqlProps.setProperty("password", egeriaProps.getClearPassword());
            }
    }

    /*
    Generates a Database Instance identifier from system tables
    @return usr@server_addr@port
     */
/*    private String getDatabaseInstance( ) throws SQLException
    {

        if( this.instance != null )
            return this.instance;


        String sql = "SELECT CURRENT_USER usr ,inet_server_addr() host, inet_server_port() port;";
        try( Connection connection  = DriverManager.getConnection( mysqlProps.getProperty("url"), mysqlProps.getProperty("user"), mysqlProps.getProperty("password") );
             PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()
        )
        {

            while (rs.next())
            {
                instance = rs.getString("usr") + "@" +
                        rs.getString("host") + "@" +
                        rs.getString("port");
            }
        }

        return instance;
    }

 */
    /*
    returns a list of databases served ny a particular server
    connection url MUST be in format dbc:mysqlql://host:port

     * @param props contains the database connection properties
     * @return A list of datbase attributes hosted by the host serever
     * @throws SQLException thrown by the JDBC Driver

     */

    public List<MysqlDatabase> getDabases( ) throws SQLException
    {
        ArrayList<MysqlDatabase> databaseNames = new ArrayList<MysqlDatabase>();
        /*
         */
        String sql = "SELECT * FROM information_schema.schemata;";
        try( Connection connection  = DriverManager.getConnection( mysqlProps.getProperty("url"), mysqlProps.getProperty("user"), mysqlProps.getProperty("password") );
             PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()
        )
        {

            while (rs.next()) {
                databaseNames.add( new MysqlDatabase(           rs.getString("CATALOG_NAME"),
                                                                rs.getString("SCHEMA_NAME"),
                                                                rs.getString("DEFAULT_CHARACTER_SET_NAME"),
                                                                rs.getString("DEFAULT_COLLATION_NAME"),
                                                                rs.getString ( "DEFAULT_COLLATION_NAME" )));
            }
        }

        return databaseNames;
    }

    /**
     * Lists the schemas for a given database
     * @param databaseName the name of the database to find the schemas
     * @return A list of schemas for the given database
     * @throws SQLException thrown by the JDBC Driver
     */
    public List<MysqlSchema> getDatabaseSchema(String databaseName ) throws SQLException
    {
        String sql = "SELECT *  FROM information_schema.schemata where catalog_name = '%s' ;";

        sql = String.format( sql, databaseName );

        /* list of the attributes of the schemas */
        List<MysqlSchema> schemas = new ArrayList<>();

        try( Connection conn  = DriverManager.getConnection( mysqlProps.getProperty("url"), mysqlProps.getProperty("user"), mysqlProps.getProperty("password") );
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql);
        ) {
            while (rs.next()) {
                MysqlSchema attributes = new MysqlSchema(
                        rs.getString("catalog_name"),
                        rs.getString("schema_name"),
                        rs.getString("default_character_set_schema"),
                        rs.getString("default_collation_name"),
                        rs.getString("sql_path"),
                        rs.getString("default_encryption"));

                if ((attributes.getSchema_name().equals("public"))) {
                    if (isSchemaInUse("public") == true) {
                        schemas.add(attributes);
                    }
                } else {
                    schemas.add(attributes);
                }

            }
        }

        return schemas;
    }
    /**
     * Checks to see if a named schema has any tables
     * @param schema the name of the schema
     * @return A boolean, true if a table is found in schema
     * @throws SQLException thrown by the JDBC Driver
     */
    private boolean isSchemaInUse(String schema) throws SQLException {

        boolean result = false;
        String sql = "SELECT count(table_schema) AS rowcount " +
                "FROM INFORMATION_SCHEMA.TABLES " +
                "WHERE table_schema = '%s' ;";

        sql = String.format(sql, schema);

        try( Connection conn  = DriverManager.getConnection( mysqlProps.getProperty("url"), mysqlProps.getProperty("user"), mysqlProps.getProperty("password") );
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql);
        ) {
            rs.next();
            if (rs.getInt("rowcount") == 0) {
                result = true;
            }
        }

        return result;
    }

    /**
     * Lists the mysql attributes for all tables for a given schema
     * @param schemaName the name of the database to find the schemas
     * @param type in mysql views and tables are treated the same type = "VIEW" or "BASE TABLE"
     * @return A list of tables for the given schema
     * @throws SQLException thrown by the JDBC Driver
     */
    private List<MysqlTable> getTables(String schemaName, String type) throws SQLException {
        String sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '%s' AND table_type = '%s';";
        sql = String.format(sql, schemaName,type);
        List<MysqlTable> attributes = new ArrayList<>();

        try( Connection conn  = DriverManager.getConnection( mysqlProps.getProperty("url"), mysqlProps.getProperty("user"), mysqlProps.getProperty("password") );
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql);
        ) {
            while (rs.next()) {
                MysqlTable attr = new MysqlTable(
                        rs.getString("table_catalog"),
                        rs.getString("table_schema"),
                        rs.getString("table_name"),
                        rs.getString("table_type"),
                        rs.getString("self_referencing_column_name"),
                        rs.getString("reference_generation"),
                        rs.getString("user_defined_type_catalog"),
                        rs.getString("user_defined_type_schema"),
                        rs.getString("user_defined_type_name"),
                        rs.getString("user_defined_type_name"),
                        rs.getString("is_insertable_into"),
                        rs.getString("commit_action")
                );
                attributes.add(attr);
            }

        }

        return attributes;
    }

    /**
     * Lists the mysql column attributes for a given table
     * @param tableName the name of the database to find the schemas
     * @return A list of columns for the given table
     * @throws SQLException thrown by the JDBC Driver
     */
    List<MysqlColumn> getColumns(String tableName) throws SQLException {
        String sql = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s';";
        sql = String.format(sql, tableName);
        List<MysqlColumn> cols = new ArrayList<MysqlColumn>();

        try( Connection conn  = DriverManager.getConnection( mysqlProps.getProperty("url"), mysqlProps.getProperty("user"), mysqlProps.getProperty("password") );
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql);
        ) {
            while (rs.next()) {

                MysqlColumn attr = new MysqlColumn(
                        rs.getString("table_catalog"),
                        rs.getString("table_schema"),
                        rs.getString("table_name"),
                        rs.getString("column_name"),
                        rs.getString("ordinal_position"),
                        rs.getString("column_default"),
                        rs.getString("is_nullable"),
                        rs.getString("data_type"),
                        rs.getString("character_maximum_length"),
                        rs.getString("character_octet_length"),
                        rs.getString("numeric_precision"),
                        rs.getString("numeric_precision_radix"),
                        rs.getString("numeric_scale"),
                        rs.getString("datetime_precision"),
                        rs.getString("interval_type"),
                        rs.getString("interval_precision"),
                        rs.getString("character_set_catalog"),
                        rs.getString("character_set_schema"),
                        rs.getString("character_set_name"),
                        rs.getString("collation_catalog"),
                        rs.getString("collation_schema"),
                        rs.getString("collation_name"),
                        rs.getString("domain_catalog"),
                        rs.getString("domain_schema"),
                        rs.getString("domain_name"),
                        rs.getString("udt_catalog"),
                        rs.getString("udt_schema"),
                        rs.getString("udt_name"),
                        rs.getString("scope_catalog"),
                        rs.getString("scope_schema"),
                        rs.getString("scope_name"),
                        rs.getString("maximum_cardinality"),
                        rs.getString("dtd_identifier"),
                        rs.getString("is_self_referencing"),
                        rs.getString("is_identity"),
                        rs.getString("identity_generation"),
                        rs.getString("identity_start"),
                        rs.getString("identity_increment"),
                        rs.getString("identity_maximum"),
                        rs.getString("identity_minimum"),
                        rs.getString("identity_cycle"),
                        rs.getString("is_generated"),
                        rs.getString("generation_expression"),
                        rs.getString("is_updatable")
                );
                cols.add(attr);
            }

        }

        return cols;


    }

    /**
     * Wrapper function which lists the mysql attributes for views for a given schema
     * @param schemaName the name of the database to find the schemas
     * @return A list of schemas for the given database
     * @throws SQLException thrown by the JDBC Driver
     */
    public List<MysqlTable> getViews(String schemaName) throws SQLException {

        return getTables(schemaName, "VIEW");

    }

    /**
     * Wrapper function which lists the mysql attributes for tables for a given schema
     * @param schemaName the name of the database to find the schemas
     * @return A list of tables for the given database
     * @throws SQLException thrown by the JDBC Driver
     */
    public List<MysqlTable> getTables(String schemaName) throws SQLException {
        return getTables(schemaName, "BASE TABLE");

    }

    /**
     * Wrapper function which lists the mysql primary key attributes for a given table name
     * @param tableName the name of the database to find the schemas
     * @return A list of primary keys for the given database
     * @throws SQLException thrown by the JDBC Driver
     */
    public List<String> getPrimaryKeyColumnNamesForTable(String tableName) throws SQLException {
        return getKeyNamesForTable(tableName, "PRIMARY KEY");
    }

    /**
     * Wrapper function which lists the mysql foreign key attributes for views for a given table
     * @param tableName the name of the table
     * @return A list of foreign keys for the given table
     * @throws SQLException thrown by the JDBC Driver
     */
    public List<String> getForeignKeyColumnNamesForTable(String tableName) throws SQLException {
        return getKeyNamesForTable(tableName, "FOREIGN KEY");
    }

    /**
     * Primary keys and foregin keys are treated the same in mysql
     * @param tableName the name of the database to find the schemas
     * @param type "PRIMARY KEY" or "FOREGIN KEY"
     * @return A list of keys for the given table
     * @throws SQLException thrown by the JDBC Driver
     */
    private List<String> getKeyNamesForTable(String tableName, String type) throws SQLException {
        List<String> names = new ArrayList<>();

        String sql = "SELECT c.column_name AS name FROM information_schema.table_constraints tc JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name WHERE constraint_type = '%s' and tc.table_name = '%s';";
        sql = String.format(sql, type, tableName);

        try( Connection conn  = DriverManager.getConnection( mysqlProps.getProperty("url"), mysqlProps.getProperty("user"), mysqlProps.getProperty("password") );
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql);
        ) {
            while (rs.next()) {

                String name = rs.getString("name");
                names.add(name);
            }

        }

        return names;
    }

    /**
     * lists the foreign key attributes needed to create an enetity relationship between the database columns
     * @param tableName the name of the table containimng the foregin keys
     * @return A list of foregin key links attributes for the given table
     * @throws SQLException thrown by the JDBC Driver
     */
    public List<MysqlForeignKeyLinks> getForeginKeyLinksForTable(String tableName) throws SQLException {

        String sql = "SELECT\n" +
                "    tc.table_schema, \n" +
                "    tc.constraint_name, \n" +
                "    tc.table_name, \n" +
                "    kcu.column_name, \n" +
                "    ccu.table_schema AS ftschema,\n" +
                "    ccu.table_name AS ftname,\n" +
                "    ccu.column_name AS fcolumn \n" +
                "FROM \n" +
                "    information_schema.table_constraints AS tc \n" +
                "    JOIN information_schema.key_column_usage AS kcu\n" +
                "      ON tc.constraint_name = kcu.constraint_name\n" +
                "      AND tc.table_schema = kcu.table_schema\n" +
                "    JOIN information_schema.constraint_column_usage AS ccu\n" +
                "      ON ccu.constraint_name = tc.constraint_name\n" +
                "      AND ccu.table_schema = tc.table_schema\n" +
                "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='%s';\n";


        sql = String.format(sql, tableName);

        List<MysqlForeignKeyLinks> results = new ArrayList<>();

        try( Connection conn  = DriverManager.getConnection( mysqlProps.getProperty("url"), mysqlProps.getProperty("user"), mysqlProps.getProperty("password") );
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql);
        )
        {
            while (rs.next()) {
                MysqlForeignKeyLinks link = new MysqlForeignKeyLinks(
                        rs.getString("table_schema"),
                        rs.getString("constraint_name"),
                        rs.getString("table_name"),
                        rs.getString("column_name"),
                        rs.getString("ftschema"),
                        rs.getString("ftname"),
                        rs.getString("fcolumn"));

                results.add(link);
            }

            return results;

        }
    }
}
