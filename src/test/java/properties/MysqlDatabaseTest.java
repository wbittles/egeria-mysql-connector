package properties;

import org.junit.jupiter.api.Test;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.properties.MysqlDatabase;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MysqlDatabaseTest {


    @Test
    void getName() {
        MysqlDatabase database = new MysqlDatabase("name",
                "encoding",
                "collate",
                "ctype",
                "version");

        assertEquals ( database.getName(), "name");
    }


    @Test
    void getEncoding() {
        MysqlDatabase database = new MysqlDatabase("name",
                "encoding",
                "collate",
                "ctype",
                "version");
        assertEquals ( database.getEncoding(),"encoding");
    }

    @Test
    void getCollate() {
        MysqlDatabase database = new MysqlDatabase("name",
                "encoding",
                "collate",
                "ctype",
                "version");
        assertEquals ( database.getCollate(), "collate");
    }

    @Test
    void getCtype() {
        MysqlDatabase database = new MysqlDatabase("name",
                "encoding",
                "collate",
                "ctype",
                "version");
        assertEquals ( database.getCtype(), "ctype");

    }


    @Test
    void getVersion() {
        MysqlDatabase database = new MysqlDatabase("name",
                "encoding",
                "collate",
                "ctype",
                "version");
        assertEquals ( database.getVersion(), "version" );

    }

    @Test
    void getProperties() {
        MysqlDatabase database = new MysqlDatabase("name",
                "encoding",
                "collate",
                "ctype",
                "version");

        Map<String,String> testProps = new HashMap<>();
        testProps.put("name","name");
        testProps.put("ctype", "ctype" );
        testProps.put("version", "version");
        testProps.put("collate", "collate");
        testProps.put("encoding", "encoding" );

        Map<String, String> props = database.getProperties();
        assertEquals( props.size(), 5);
        assertTrue( props.entrySet().stream()
                .allMatch(e -> e.getValue().equals(testProps.get(e.getKey()))));
    }

    @Test
    void getQualifiedName() {

        MysqlDatabase database = new MysqlDatabase("name",
                "encoding",
                "collate",
                "ctype",
                "version");
        String qName =  database.getName();
        assertEquals(database.getQualifiedName(), qName );
    }
}