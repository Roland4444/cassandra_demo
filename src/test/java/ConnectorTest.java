import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.UUIDs;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNotEquals;
import static org.junit.jupiter.api.Assertions.*;

class ConnectorTest {
    Connector cnc = new Connector();
    @org.junit.jupiter.api.Test
    void getsession() {
        cnc.connect("127.0.0.1", null);
        assertNotEquals(null, cnc.getsession());
        cnc.close();

    }

    @Test
    void whenCreatingAKeyspace_thenCreated() {
        cnc.connect("127.0.0.1", null);
        String keyspaceName = "library";
        cnc.createKeyspace(keyspaceName, "SimpleStrategy", 1);

        ResultSet result = cnc.getsession().execute("SELECT * FROM system_schema.keyspaces;");

        List<String> matchedKeyspaces = result.all()
                .stream()
                .filter(r -> r.getString(0).equals(keyspaceName.toLowerCase()))
                .map(r -> r.getString(0))
                .collect(Collectors.toList());

        assertEquals(matchedKeyspaces.size(), 1);
        assertTrue(matchedKeyspaces.get(0).equals(keyspaceName.toLowerCase()));
    }


    @Test
    void createTable() {
        cnc.connect("127.0.0.1", null);
        Map<String, String> params= new HashMap();
        params.put("id", "uuid");
        params.put("title", "text");
        params.put("subject", "text");
        assertNotEquals(null, cnc.createTable("library", "books", params, "id"));
        System.out.println(cnc.createTable("library", "books", params, "id"));
        cnc.getsession().execute(cnc.createTable("library", "books", params, "id"));


    }


    @Test
    void createTableDump() {
        cnc.connect("127.0.0.1", null);
        Map<String, String> params= new HashMap();
        params.put("id", "int");
        params.put("dump", "blob");
        assertNotEquals(null, cnc.createTable("library", "dump", params, "id"));
        System.out.println(cnc.createTable("library", "dump", params, "id"));
        cnc.getsession().execute(cnc.createTable("library", "dump", params, "id"));
    }

    @Test
    void insertblob() throws IOException {

        cnc.connect("127.0.0.1", null);
        long startTime = System.currentTimeMillis();

        for (int i =0; i<100000; i++){
            Blob blob = new Blob();
            ByteBuffer bbuf = ByteBuffer.allocate(4096);
            bbuf.order(ByteOrder.BIG_ENDIAN);
            blob.id=i;
            blob.blob=Integer.toString(i).getBytes();
            System.out.println(i+">>>"+Bytes.toHexString(blob.blob));
            cnc.insertblob(blob, "library", "dump");
        }
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println("total="+elapsedTime/1000+"   Seconds");



    }

    @Test
    void insertbookByTitle() {
        cnc.connect("127.0.0.1", null);
        Book Book = new Book();
        Book.Id= UUIDs.timeBased();
        Book.Title="Остров сокровищ";
        cnc.insertbookByTitle(Book, "library", "books");
    }

    @Test
    void select() {
        cnc.connect("127.0.0.1", null);
        long startTime = System.currentTimeMillis();
        ResultSet result = cnc.getsession().execute("SELECT * FROM library.dump ;");
        List res=result.all();
        Map<Integer, byte[]> Flow = new HashMap();
        for (int i=0; i<res.size(); i++){
            Row row= (Row) res.get(i);
            byte[] restored = Bytes.getArray(row.getBytes(1));
            Integer id = row.getInt(0);
            Flow.put(id, restored);
            System.out.println(new String(restored));
        };
        Map<Integer, byte[]> treeMap = new TreeMap<Integer, byte[]>(Flow);
        printMap(treeMap);
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println("total="+elapsedTime/1000+"   Seconds");
    }

    @Test
    void sort(){
        Map<String, String> unsortMap = new HashMap<String, String>();
        unsortMap.put("Z", "z");
        unsortMap.put("B", "b");
        unsortMap.put("A", "a");
        unsortMap.put("C", "c");
        unsortMap.put("D", "d");
        unsortMap.put("E", "e");
        unsortMap.put("Y", "y");
        unsortMap.put("N", "n");
        unsortMap.put("J", "j");
        unsortMap.put("M", "m");
        unsortMap.put("F", "f");

        System.out.println("Unsort Map......");
        printMap(unsortMap);

        System.out.println("\nSorted Map......By Key");
        Map<String, String> treeMap = new TreeMap<String, String>(unsortMap);
        printMap(treeMap);

}

    //pretty print a map
    public <K, V> void printMap(Map<K, V> map) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            System.out.println("Key : " + entry.getKey()
                    + " Value : " + new String((byte[]) entry.getValue()));
        }
    }

}

