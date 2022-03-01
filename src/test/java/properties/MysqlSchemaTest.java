package properties;

import org.junit.jupiter.api.Test;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.properties.MysqlSchema;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MysqlSchemaTest {


    void mysqlSchemaTest()
    {
    }


    @Test
    void getCatalog_name() {
        MysqlSchema schema =  new MysqlSchema(   "catalog_name",
                "schema_name",
                "default_character_set_name",
                "default_collation_name",
                "sql_path",
                "default_encryption");

        assertEquals( schema.getCatalog_name(), "catalog_name" );
    }

    @Test
    void getSchema_name() {
        MysqlSchema schema =  new MysqlSchema(   "catalog_name",
                "schema_name",
                "default_character_set_name",
                "default_collation_name",
                "sql_path",
                "default_encryption");

        assertEquals( schema.getSchema_name(), "schema_name" );
    }





    @Test
    void getDefault_character_set_name() {
        MysqlSchema schema =  new MysqlSchema(   "catalog_name",
                "schema_name",
                "default_character_set_name",
                "default_collation_name",
                "sql_path",
                "default_encryption");

          assertEquals(schema.getDefault_character_set_name(), "default_character_set_name");
  }

    @Test
    void getSql_path() {
        MysqlSchema schema =  new MysqlSchema(   "catalog_name",
                "schema_name",
                "default_character_set_name",
                "default_collation_name",
                "sql_path",
                "default_encryption");

           assertEquals(schema.getSql_path(), "sql_path");
   }

    @Test
    void getProperties() {
        MysqlSchema schema =  new MysqlSchema(   "catalog_name",
                "schema_name",
                "default_character_set_name",
                "default_collation_name",
                "sql_path",
                "default_encryption");

        Map<String,String> testProps = new HashMap<>();
        testProps.put("default_character_set_name", "default_character_set_name" );
        testProps.put("catalog_name","catalog_name");
        testProps.put("sql_path", "sql_path" );
        testProps.put("schema_name", "schema_name" );
        testProps.put("default_collation_name", "default_schema_character_set");

        Map<String, String> props = schema.getProperties();
        assertEquals( props.size(), 7);
        Boolean b = props.entrySet().stream()
                .allMatch(e -> e.getValue().equals(testProps.get(e.getKey())));
        assertTrue( b);

    }

    @Test
    void getQualifiedName() {
        MysqlSchema schema =  new MysqlSchema(   "catalog_name",
                "schema_name",
                "default_character_set_name",
                "default_collation_name",
                "sql_path",
                "default_encryption");

        assertEquals(schema.getQualifiedName(),  "catalog_name" + "::" + "schema_name" );
    }
}