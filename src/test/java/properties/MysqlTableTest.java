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
        MysqlTable table = new MysqlTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getTable_catalog(), "table_catalog");
    }

    @Test
    void getTable_schema() {

        MysqlTable table = new MysqlTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getTable_schema(), "table_schema");

    }

    @Test
    void getTable_name() {
        MysqlTable table = new MysqlTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getTable_name(), "table_name");
    }

    @Test
    void getTable_type() {
        MysqlTable table = new MysqlTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getTable_type(), "table_type");
    }

    @Test
    void getSelf_referencing_column_name() {
        MysqlTable table = new MysqlTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getSelf_referencing_column_name(), "self_referencing_column_name");
    }

    @Test
    void getReference_generation() {
        MysqlTable table = new MysqlTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getReference_generation(), "reference_generation");

    }

    @Test
    void getUser_defined_type_catalog() {
        MysqlTable table = new MysqlTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getUser_defined_type_catalog(), "user_defined_type_catalog");
    }

    @Test
    void getUser_defined_type_schema() {
        MysqlTable table = new MysqlTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getUser_defined_type_schema(), "user_defined_type_schema");

    }

    @Test
    void getUser_defined_type_name() {
        MysqlTable table = new MysqlTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getUser_defined_type_name(), "user_defined_type_name");
    }

    @Test
    void getIs_insertable_into() {
        MysqlTable table = new MysqlTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getIs_insertable_into(), "is_insertable_into");
    }

    @Test
    void getIs_typed() {
        MysqlTable table = new MysqlTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getIs_typed(), "is_typed");
    }

    @Test
    void getCommit_action() {

        MysqlTable table = new MysqlTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getCommit_action(), "commit_action");

    }

    @Test
    void getProperties() {
        MysqlTable table = new MysqlTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        Map<String,String> testProps = new HashMap<>();
        testProps.put("table_catalog","table_catalog");
        testProps.put("table_schema","table_schema");
        testProps.put("table_name", "table_name" );
        testProps.put("table_type", "table_type");
        testProps.put("self_referencing_column_name", "self_referencing_column_name");
        testProps.put("reference_generation", "reference_generation" );
        testProps.put("user_defined_type_catalog","user_defined_type_catalog");
        testProps.put("user_defined_type_schema", "user_defined_type_schema" );
        testProps.put("user_defined_type_name", "user_defined_type_name" );
        testProps.put("is_insertable_into", "is_insertable_into" );
        testProps.put("is_typed", "is_typed" );
        testProps.put("commit_action", "commit_action" );

        Map<String, String> props = table.getProperties();
        assertEquals( props.size(), 12);

        assertTrue( props.entrySet().stream()
                .allMatch(e -> e.getValue().equals(testProps.get(e.getKey()))) );

    }

    @Test
    void getQualifiedName() {

        MysqlTable table = new MysqlTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        String qName = new StringBuilder().append(table.getTable_catalog()).append(".")
                                            .append(table.getTable_schema()).append(".")
                                            .append(table.getTable_type().substring(0,4)).append(".")
                                            .append(table.getTable_name()).toString();

        assertEquals( table.getQualifiedName(), qName);

    }
}