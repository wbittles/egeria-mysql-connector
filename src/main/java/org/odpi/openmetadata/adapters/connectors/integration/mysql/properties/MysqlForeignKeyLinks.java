/* SPDX-License-Identifier: Apache 2.0 */
/* Copyright Contributors to the ODPi Egeria project. */

package org.odpi.openmetadata.adapters.connectors.integration.mysql.properties;

import java.util.HashMap;
import java.util.Map;

public class MysqlForeignKeyLinks
{

    private final String constraint_name;
    private final String table_name;
    private final String column_name;
    private final String referenced_table_name;



    public MysqlForeignKeyLinks(String table_name, String column_name, String constraint_name, String referenced_table_name) {
        this.table_name = table_name;
        this.column_name = column_name;
        this.constraint_name = constraint_name;
        this.referenced_table_name = referenced_table_name;
    }

    public String getImportedColumnQualifiedName()
    {
        return  referenced_table_name + "." +
                column_name ;
    }

    public String getExportedColumnQualifiedName()
    {
        return  table_name + "." +
                column_name;
    }

    public Map< String, String> getProperties ()
    {
        Map<String, String> props = new HashMap<>();

        props.put("constraint_name", constraint_name );
        props.put("table_name", table_name );
        props.put("column_name", column_name );
        props.put("referenced_table_name", referenced_table_name);
        return props;
    }
    
}
