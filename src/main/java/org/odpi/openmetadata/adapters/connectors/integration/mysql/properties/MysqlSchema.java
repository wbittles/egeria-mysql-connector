/* SPDX-License-Identifier: Apache 2.0 */
/* Copyright Contributors to the ODPi Egeria project. */

package org.odpi.openmetadata.adapters.connectors.integration.mysql.properties;

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseSchemaElement;

import java.util.HashMap;
import java.util.Map;

public class MysqlSchema {

    final private String catalog_name;
    final private String schema_name;
    final private String default_character_set_name;
    final private String default_collation_name;
    final private String sql_path;
    private final String default_encryption;


    public String getDefault_collation_name() {
        return default_collation_name;
    }


    public String getDefault_encryption() {
        return default_encryption;
    }


    public MysqlSchema(String catalog_name, String schema_name, String default_character_set_name, String default_collation_name, String sql_path, String default_encryption) {

        this.catalog_name = catalog_name;
        this.schema_name = schema_name;
        this.default_character_set_name = default_character_set_name;
        this.default_collation_name = default_collation_name;
        this.sql_path = sql_path;
        this.default_encryption = default_encryption;
    }

    public String getCatalog_name() {
        return catalog_name;
    }
    public String getSchema_name() {
        return schema_name;
    }
    public String getDefault_character_set_name() {
        return default_character_set_name;
    }
    public String getSql_path() {
        return sql_path;
    }



    public Map<String, String> getProperties()
    {
        Map<String,String> props = new HashMap<>();

            props.put("catalog_name", getCatalog_name());
            props.put("schema_name", getSchema_name());
            props.put("default_character_set_name", getDefault_character_set_name());
            props.put("default_collation_name", getDefault_collation_name());
            props.put("default_encryption", getDefault_encryption());
            props.put("sql_path", getSql_path());
        return props;
    }

    public String getQualifiedName ( )
    {
        return catalog_name + "::" + schema_name;
    }

    public boolean isEquivalent(DatabaseSchemaElement element)
    {
        boolean result = false;
        Map<String, String> props = element.getDatabaseSchemaProperties().getAdditionalProperties();
        if ( props.equals( this.getProperties()))
        {
            result = true;
        }
        return result;
    }
}
