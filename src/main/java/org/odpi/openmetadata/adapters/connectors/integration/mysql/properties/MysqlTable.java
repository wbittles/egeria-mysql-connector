/* SPDX-License-Identifier: Apache 2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.mysql.properties;

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseTableElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseViewElement;

import java.util.HashMap;
import java.util.Map;

public class MysqlTable {

    private final String table_catalog;
    private final String table_schema;
    private final String table_name;
    private final String table_type;
    private final String engine;
    private final String version;
    private final String row_format;
    private final String table_rows;
    private final String avg_row_length;
    private final String data_length;
    private final String max_data_length;
    private final String index_length;
    private final String data_free;
    private final String auto_increment;
    private final String create_time;
    private final String update_time;
    private final String check_time;
    private final String table_collation;
    private final String checksum;
    private final String create_options;
    private final String table_comment;

    public MysqlTable(String table_catalog, String table_schema, String table_name, String table_type, String engine, String version, String row_format, String table_rows, String avg_row_length, String data_length, String max_data_length, String index_length, String data_free, String auto_increment, String create_time, String update_time, String check_time, String table_collation, String checksum, String create_options, String table_comment) {

        this.table_catalog = table_catalog;
        this.table_schema = table_schema;
        this.table_name = table_name;
        this.table_type = table_type;
        this.engine = engine;
        this.version = version;
        this.row_format = row_format;
        this.table_rows = table_rows;
        this.avg_row_length = avg_row_length;
        this.data_length = data_length;
        this.max_data_length = max_data_length;
        this.index_length = index_length;
        this.data_free = data_free;
        this.auto_increment = auto_increment;
        this.create_time = create_time;
        this.update_time = update_time;
        this.check_time = check_time;
        this.table_collation = table_collation;
        this.checksum = checksum;
        this.create_options = create_options;
        this.table_comment = table_comment;

    }


    public String getEngine() {
        return engine;
    }

    public String getVersion() {
        return version;
    }

    public String getRow_format() {
        return row_format;
    }

    public String getTable_rows() {
        return table_rows;
    }

    public String getAvg_row_length() {
        return avg_row_length;
    }

    public String getData_length() {
        return data_length;
    }

    public String getMax_data_length() {
        return max_data_length;
    }

    public String getIndex_length() {
        return index_length;
    }

    public String getData_free() {
        return data_free;
    }

    public String getAuto_increment() {
        return auto_increment;
    }

    public String getCreate_time() {
        return create_time;
    }

    public String getUpdate_time() {
        return update_time;
    }

    public String getCheck_time() {
        return check_time;
    }

    public String getTable_collation() {
        return table_collation;
    }

    public String getChecksum() {
        return checksum;
    }

    public String getCreate_options() {
        return create_options;
    }

    public String getTable_comment() {
        return table_comment;
    }

    public String getTable_catalog()
    {
        return table_catalog;
    }

    public String getTable_schema()
    {
        return table_schema;
    }
    public String getTable_name()
    {
        return table_name;
    }
    public String getTable_type()
    {
        return table_type;
    }

    public Map<String,String> getProperties()
    {
        HashMap<String,String> props = new HashMap<>();
        props.put("table_catalog", this.table_catalog);
        props.put("table_schema", this.table_schema);
        props.put("table_name", table_name);
        props.put("table_type", table_type);
        props.put("engine", engine);
        props.put("version", version );
        props.put("row_format", row_format );
        props.put("table_rows", table_rows );
        props.put("avg_row_length", avg_row_length );
        props.put("data_length", data_length );
        props.put( "max_data_length", max_data_length );
        props.put( "index_length", index_length );
        props.put( "data_free", data_free );
        props.put( "auto_increment", auto_increment );
        props.put( "create_time", create_time );
        props.put( "update_time", update_time );
        props.put( "check_time", check_time );
        props.put( "table_collation", table_collation );
        props.put( "checksum", checksum );
        props.put( "create_options", create_options );
        props.put( "table_comment", table_comment );

        return props;
    }

    public String getQualifiedName ( ) {
        return table_catalog + "." + table_schema + "." + table_type.substring(0,4) + "." + table_name;
    }

    public boolean isEquivalent(DatabaseTableElement element)
    {
        boolean result = false;
        Map<String, String> mysqlProps = this.getProperties();
        Map<String, String> egeriaProps = element.getDatabaseTableProperties().getAdditionalProperties();

        if ( egeriaProps.equals( mysqlProps))
        {
            result = true;
        }
        return result;
    }


    public boolean isEquivalent(DatabaseViewElement element)
    {
        boolean result = false;
        Map<String, String> mysqlProps = this.getProperties();
        Map<String, String> egeriaProps = element.getDatabaseViewProperties().getAdditionalProperties();

        if ( egeriaProps.equals( mysqlProps))
        {
            result = true;
        }
        return result;
    }

}
