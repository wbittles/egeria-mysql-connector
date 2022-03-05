/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.mysql.mapper;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseColumnElement;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseColumnProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseSchemaProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseTableProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseViewProperties;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.properties.MysqlColumn;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.properties.MysqlDatabase;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.properties.MysqlSchema;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.properties.MysqlTable;

/**
 * Utility class that provides bean mapping functions
 */
public class MysqlMapper
{

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from mysql and adds the data to Egeria
     *
     * @param db     the mysql database attributes to be
     * @return       the Egeria database properties
     */
    public static DatabaseProperties getDatabaseProperties(MysqlDatabase db )
    {

            DatabaseProperties dbProps = new DatabaseProperties();
            dbProps.setDisplayName(db.getQualifiedName());
            dbProps.setQualifiedName(db.getQualifiedName());
            dbProps.setDatabaseType("mysql");
            dbProps.setDatabaseVersion(db.getVersion());
            dbProps.setEncodingType(db.getEncoding());
            dbProps.setEncodingLanguage(db.getCtype());
            dbProps.setAdditionalProperties(db.getProperties());

            return dbProps;
    }

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from mysql and adds the data to Egeria
     *
     * @param sch     the mysql database attributes to be
     * @return        the Egeria schema properties
     */
    public static DatabaseSchemaProperties getSchemaProperties(MysqlSchema sch)
    {
        DatabaseSchemaProperties schemaProps = new DatabaseSchemaProperties();
        schemaProps.setDisplayName(sch.getQualifiedName());
        schemaProps.setQualifiedName(sch.getQualifiedName());
        //schemaProps.setOwner(sch.getSchema_owner());
        schemaProps.setAdditionalProperties(sch.getProperties());

        return schemaProps;
    }

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from mysql and adds the data to Egeria
     *
     * @param table     the mysql table properties
     * @return          the Egeria table properties
     */
    public static DatabaseTableProperties getTableProperties(MysqlTable table)
    {
        DatabaseTableProperties tableProps = new DatabaseTableProperties();
        tableProps.setDisplayName(table.getTable_name());
        tableProps.setQualifiedName(table.getQualifiedName());
        tableProps.setAdditionalProperties( table.getProperties());

        return tableProps;
    }

    /**
     * mapping function that reads converts a mysql table properties to an Egeria DatabaseViewProperties
     * for a schema from mysql and adds the data to Egeria
     *
     * @param table     the mysql table properties
     * @return          the Egeria view properties
     */
    public static DatabaseViewProperties getViewProperties(MysqlTable table)
    {
        DatabaseViewProperties tableProps = new DatabaseViewProperties();
        tableProps.setDisplayName(table.getTable_name());
        tableProps.setQualifiedName(table.getQualifiedName());
        tableProps.setAdditionalProperties( table.getProperties());

        return tableProps;
    }
    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from mysql and adds the data to Egeria
     *
     * @param col    the mysql column properties
     * @return          the Egeria column properties
     */
    public static DatabaseColumnProperties getColumnProperties(MysqlColumn col)
    {
        DatabaseColumnProperties colProps = new DatabaseColumnProperties();
        colProps.setDisplayName(col.getColumn_name());
        colProps.setQualifiedName(col.getQualifiedName());
        colProps.setDataType(col.getData_type());
        colProps.setAdditionalProperties(col.getProperties());

        return colProps;
    }

}
