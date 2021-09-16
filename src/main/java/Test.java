import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class Test {
    public static void main(String[] args) {
        GraphTraversalSource g = null;
        try {
            g = traversal().withRemote("conf/remote-graph.properties");
            Object age = g.V().has("name", "hercules").values("age").next();

            System.out.println("age : " + age);
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
    }
}
