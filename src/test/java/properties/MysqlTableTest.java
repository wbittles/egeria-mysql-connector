package properties;

import org.junit.jupiter.api.Test;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.properties.MysqlTable;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MysqlTableTest {


    @Test
    void getTable_catalog() {
        MysqlTable table = new MysqlTable(
        "table_catalog",
        "table_schema",
        "table_name",
        "table_type",
        "engine",
        "version",
        "row_format",
        "table_rows",
        "avg_row_length",
        "data_length",
        "max_data_length",
        "data_free",
        "data_free",
        "auto_increment",
        "create_time",
        "update_time",
        "check_time",
        "table_collation",
        "checksum",
        "create_options",
        "table_comment"
        );

        assertEquals( table.getTable_catalog(), "table_catalog");
    }

    @Test
    void getTable_schema() {

        MysqlTable table = new MysqlTable(
                "table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "engine",
                "version",
                "row_format",
                "table_rows",
                "avg_row_length",
                "data_length",
                "max_data_length",
                "data_free",
                "data_free",
                "auto_increment",
                "create_time",
                "update_time",
                "check_time",
                "table_collation",
                "checksum",
                "create_options",
                "table_comment"
        );

        assertEquals( table.getTable_schema(), "table_schema");

    }

    @Test
    void getTable_name() {

        MysqlTable table = new MysqlTable(
                "table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "engine",
                "version",
                "row_format",
                "table_rows",
                "avg_row_length",
                "data_length",
                "max_data_length",
                "data_free",
                "data_free",
                "auto_increment",
                "create_time",
                "update_time",
                "check_time",
                "table_collation",
                "checksum",
                "create_options",
                "table_comment"
        );
        assertEquals( table.getTable_name(), "table_name");
    }

    @Test
    void getTable_type() {
        MysqlTable table = new MysqlTable(
                "table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "engine",
                "version",
                "row_format",
                "table_rows",
                "avg_row_length",
                "data_length",
                "max_data_length",
                "data_free",
                "data_free",
                "auto_increment",
                "create_time",
                "update_time",
                "check_time",
                "table_collation",
                "checksum",
                "create_options",
                "table_comment"
        );

        assertEquals( table.getTable_type(), "table_type");
    }





    @Test
    void getProperties() {
        MysqlTable table = new MysqlTable(
                "table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "engine",
                "version",
                "row_format",
                "table_rows",
                "avg_row_length",
                "data_length",
                "max_data_length",
                "index_length",
                "data_free",
                "auto_increment",
                "create_time",
                "update_time",
                "check_time",
                "table_collation",
                "checksum",
                "create_options",
                "table_comment"
        );

        Map<String,String> testProps = new HashMap<>();
        testProps.put("table_catalog","table_catalog");
        testProps.put("table_schema","table_schema");
        testProps.put("table_name", "table_name" );
        testProps.put("table_type", "table_type");
        testProps.put("engine", "engine");
        testProps.put("version", "version" );
        testProps.put("row_format","row_format");
        testProps.put("table_rows", "table_rows" );
        testProps.put("avg_row_length", "avg_row_length" );
        testProps.put("data_length", "data_length" );
        testProps.put("max_data_length", "max_data_length" );
        testProps.put("index_length", "index_length" );
        testProps.put("data_free", "data_free" );
        testProps.put("auto_increment", "auto_increment" );
        testProps.put("create_time", "create_time" );
        testProps.put("update_time", "update_time" );
        testProps.put("check_time", "check_time" );
        testProps.put("table_collation", "table_collation" );
        testProps.put("checksum", "checksum" );
        testProps.put("create_options", "create_options" );
        testProps.put("table_comment", "table_comment" );

        Map<String, String> props = table.getProperties();

        assertTrue( props.entrySet().stream()
                .allMatch(e -> e.getValue().equals(testProps.get(e.getKey()))) );

    }

    @Test
    void getQualifiedName() {

        MysqlTable table = new MysqlTable(
                "table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "engine",
                "version",
                "row_format",
                "table_rows",
                "avg_row_length",
                "data_length",
                "max_data_length",
                "data_free",
                "data_free",
                "auto_increment",
                "create_time",
                "update_time",
                "check_time",
                "table_collation",
                "checksum",
                "create_options",
                "table_comment"
        );

        String qName = new StringBuilder().append(table.getTable_catalog()).append(".")
                                            .append(table.getTable_schema()).append(".")
                                            .append(table.getTable_type().substring(0,4)).append(".")
                                            .append(table.getTable_name()).toString();

        assertEquals( table.getQualifiedName(), qName);

    }
}