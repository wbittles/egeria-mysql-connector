/* SPDX-License-Identifier: Apache 2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.mysql.properties;

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseColumnElement;

import java.util.HashMap;
import java.util.Map;

public class MysqlColumn
{
    private final String table_catalog;
    private final String table_schema;
    private final String table_name;
    private final String column_name;
    private final String ordinal_position;
    private final String column_default;
    private final String is_nullable;
    private final String data_type;
    private final String character_maximum_length;
    private final String character_octet_length;
    private final String numeric_precision;
    private final String numeric_scale;
    private final String datetime_precision;
    private final String character_set_name;
    private final String collation_name;
    private final String generation_expression;

    /*    private final String column_type;
    private final String column_key;
    private final String extra;
    private final String privileges;
    private final String column_comment;
    private final String srs_id;
*/

    public MysqlColumn(String table_catalog, String table_schema, String table_name, String column_name, String ordinal_position, String column_default, String is_nullable, String data_type, String character_maximum_length, String character_octet_length, String numeric_precision, String numeric_scale, String datetime_precision, String character_set_name, String collation_name, String generation_expression) {
        this.table_catalog = table_catalog;
        this.table_schema = table_schema;
        this.table_name = table_name;
        this.column_name = column_name;
        this.ordinal_position = ordinal_position;
        this.column_default = column_default;
        this.is_nullable = is_nullable;
        this.data_type = data_type;
        this.character_maximum_length = character_maximum_length;
        this.character_octet_length = character_octet_length;
        this.numeric_precision = numeric_precision;
        this.numeric_scale = numeric_scale;
        this.datetime_precision = datetime_precision;
        this.character_set_name = character_set_name;
        this.collation_name = collation_name;
        this.generation_expression = generation_expression;
    }


    public Map<String, String> getProperties()
    {

        Map<String, String> props = new HashMap<>();
            props.put("table_catalog", getTable_catalog());
            props.put("table_schema", getTable_schema());
            props.put("table_name", getTable_name());
            props.put("column_name", getColumn_name());
            props.put("ordinal_position", getOrdinal_position());
            props.put("column_default", getColumn_default());
            props.put("is_nullable", getIs_nullable());
            props.put("data_type", getData_type());
            props.put("character_maximum_length", getCharacter_maximum_length());
            props.put("character_octet_length", getCharacter_octet_length());
            props.put("numeric_precision", getNumeric_precision());
            props.put("numeric_scale", getNumeric_scale());
            props.put("datetime_precision", getDatetime_precision());
            props.put("collation_name", getCollation_name());
            props.put("generation_expression", getGeneration_expression() );
        return props;
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

    public String getColumn_name() {
        return column_name;
    }

    public String getOrdinal_position() {
        return ordinal_position;
    }

    public String getColumn_default() {
        return column_default;
    }

    public String getIs_nullable() {
        return is_nullable;
    }

    public String getData_type() {
        return data_type;
    }

    public String getCharacter_maximum_length() {
        return character_maximum_length;
    }

    public String getCharacter_octet_length() {
        return character_octet_length;
    }

    public String getNumeric_precision() {
        return numeric_precision;
    }


    public String getNumeric_scale() {
        return numeric_scale;
    }

    public String getDatetime_precision() {
        return datetime_precision;
    }

    public String getCollation_name() {
        return collation_name;
    }

    public String getGeneration_expression() {
        return generation_expression;
    }


    public String getQualifiedName ( ) {

        return getTable_catalog () + "::" + getTable_schema () + "::" + getTable_name () + "::" + getColumn_name ();
    }

    public boolean isEquivalent(DatabaseColumnElement element)
    {
        boolean result = false;
        Map<String, String> props = element.getDatabaseColumnProperties().getAdditionalProperties();
        if ( props.equals( this.getProperties()))
        {
            result = true;
        }
        return result;
    }

}
