import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

import java.io.IOException;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class Test {
    public static void main(String[] args) throws ConfigurationException, IOException {
        GraphTraversalSource g = null;
        try {
            g = traversal().withRemote("conf/remote-graph.properties");
            Object age = g.V().has("name", "hercules").values("age").next();
            System.out.println("age : " + age);

            g.addV("demigod").property("name", "hercules2").iterate();
            Vertex vertex = g.V().has("name", "hercules2").next();
            System.out.printf("vertex.id() : " + vertex.id());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (g != null) {
                    g.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

//        JanusGraph g = JanusGraphFactory.open(ConfigurationUtil.loadPropertiesConfig("conf/remote-graph.properties"));
//        g.openManagement();
    }
}
