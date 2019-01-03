import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.Bytes;

import java.nio.ByteBuffer;
import java.util.Map;

public class Connector {
    private Cluster cluster;

    private Session session;

    public void connect(String node, Integer port) {
        Cluster.Builder b = Cluster.builder().addContactPoint(node);
        if (port != null) {
            b.withPort(port);
        }
        cluster = b.build();
        session = cluster.connect();
    }

    public Session getsession() {
        return this.session;
    }

    public void close() {
        session.close();
        cluster.close();
    }

    public void createKeyspace(String keyspaceName, String replicationStrategy, int replicationFactor) {
        StringBuilder sb = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                        .append(keyspaceName).append(" WITH replication = {")
                        .append("'class':'").append(replicationStrategy)
                        .append("','replication_factor':").append(replicationFactor)
                        .append("};");
        String query = sb.toString();
        session.execute(query);
    }

    public String createTable(String keyspace, String tablename, Map<String, String> parameters, String PrimaryKey) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
               .append(keyspace+"."+tablename).append("(");
        for (Map.Entry entry :parameters.entrySet()){
            sb.append(entry.getKey() +" "+entry.getValue()+",");
        };
        sb.append("primary key ("+PrimaryKey+")");
        sb.append(");");
        String query = sb.toString();
        return sb.toString();
    }

    public void insertbookByTitle(Book book, String keyspace, String tablename) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(keyspace+"."+tablename).append("(id, title) ")
                .append("VALUES (").append(book.Id)
                .append(", '").append(book.Title).append("');");
        String query = sb.toString();
        session.execute(query);
    }

    public void insertblob(Blob blob, String keyspace, String tablename) {
        PreparedStatement ps = session.prepare("insert into "+keyspace+"."+tablename+" (id, dump) values(?,?)");
        BoundStatement boundStatement = new BoundStatement(ps);
        System.out.println(Bytes.toHexString(blob.blob));
        ByteBuffer buff =  Bytes.fromHexString(Bytes.toHexString(blob.blob));
        session.execute(  boundStatement.bind(blob.id, buff));
    }

}
