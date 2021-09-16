import static com.sun.xml.internal.ws.spi.db.BindingContextFactory.LOGGER;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

import java.util.stream.Stream;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.Bindings;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.util.system.ConfigurationUtil;

public class RemoteGraphAppTest {

    protected static final String CONF_FILE = "conf/remote-graph.properties";
    public Configuration conf;
    protected GraphTraversalSource g;
    protected Graph graph;
    protected Cluster cluster;
    protected Client client;

    // used for bindings
    private static final String NAME = "name";
    private static final String AGE = "age";
    private static final String TIME = "time";
    private static final String REASON = "reason";
    private static final String PLACE = "place";
    private static final String LABEL = "label";
    private static final String OUT_V = "outV";
    private static final String IN_V = "inV";

    protected boolean supportsGeoshape = true;

    public static void main(String[] args) {
        RemoteGraphAppTest janusGraphAppTest = new RemoteGraphAppTest();

        try {
            janusGraphAppTest.openGraph();

            janusGraphAppTest.createSchema();
            janusGraphAppTest.createElements();
            janusGraphAppTest.printSchema();
            janusGraphAppTest.printV();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                janusGraphAppTest.closeGraph();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void openGraph() throws Exception {
        System.out.println("================== openGraph() ==================");

        conf = ConfigurationUtil.loadPropertiesConfig(CONF_FILE);

        // using the remote driver for schema
        try {
            cluster = Cluster.open(conf.getString("gremlin.remote.driver.clusterFile"));
            client = cluster.connect();
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }

        // using the remote graph for queries
        g = traversal().withRemote(DriverRemoteConnection.using(cluster));
    }

    public void createSchema() {
        System.out.println("================== creating schema ==================");
        // get the schema request as a string
        final String req = createSchemaRequest();
        // submit the request to the server
        final ResultSet resultSet = client.submit(req);

        // drain the results completely
        Stream<Result> futureList = resultSet.stream();
        futureList.map(Result::toString).forEach(System.out::println);
    }

    protected String createSchemaRequest() {
        final StringBuilder s = new StringBuilder();

        s.append("JanusGraphManagement management = graph.openManagement(); ");
        s.append("boolean created = false; ");

        // naive check if the schema was previously created
        s.append("if (management.getRelationTypes(RelationType.class).iterator().hasNext()) { management.rollback(); created = false; } else { ");

        // properties
        s.append("PropertyKey name = management.makePropertyKey(\"name\").dataType(String.class).make(); ");
        s.append("PropertyKey age = management.makePropertyKey(\"age\").dataType(Integer.class).make(); ");
        s.append("PropertyKey time = management.makePropertyKey(\"time\").dataType(Integer.class).make(); ");
        s.append("PropertyKey reason = management.makePropertyKey(\"reason\").dataType(String.class).make(); ");
        s.append("PropertyKey place = management.makePropertyKey(\"place\").dataType(Geoshape.class).make(); ");
        s.append("PropertyKey nickName = management.makePropertyKey(\"nickName\").dataType(String.class).make(); ");

        // vertex labels
        s.append("management.makeVertexLabel(\"titan\").make(); ");
        s.append("management.makeVertexLabel(\"location\").make(); ");
        s.append("management.makeVertexLabel(\"god\").make(); ");
        s.append("management.makeVertexLabel(\"demigod\").make(); ");
        s.append("management.makeVertexLabel(\"human\").make(); ");
        s.append("management.makeVertexLabel(\"monster\").make(); ");
        s.append("management.makeVertexLabel(\"fairy\").make(); ");

        // edge labels
        s.append("management.makeEdgeLabel(\"father\").multiplicity(Multiplicity.MANY2ONE).make(); ");
        s.append("management.makeEdgeLabel(\"mother\").multiplicity(Multiplicity.MANY2ONE).make(); ");
        s.append("management.makeEdgeLabel(\"lives\").signature(reason).make(); ");
        s.append("management.makeEdgeLabel(\"pet\").make(); ");
        s.append("management.makeEdgeLabel(\"brother\").make(); ");
        s.append("management.makeEdgeLabel(\"battled\").make(); ");

        // composite indexes
        s.append("management.buildIndex(\"nameIndex\", Vertex.class).addKey(name).buildCompositeIndex(); ");

        // mixed indexes
//        if (true) {
//            s.append("management.buildIndex(\"vAge\", Vertex.class).addKey(age).buildMixedIndex(\"").append("search").append("\"); ");
//            s.append("management.buildIndex(\"eReasonPlace\", Edge.class).addKey(reason).addKey(place).buildMixedIndex(\"").append("search").append("\"); ");
//        }

        s.append("management.commit(); created = true; }");

        return s.toString();
    }

    public void createElements() {
        System.out.println("================== creating elements ================== ");

        // Use bindings to allow the Gremlin Server to cache traversals that
        // will be reused with different parameters. This minimizes the
        // number of scripts that need to be compiled and cached on the server.
        // https://tinkerpop.apache.org/docs/3.2.6/reference/#parameterized-scripts
        final Bindings b = Bindings.instance();

        // see GraphOfTheGodsFactory.java
        Vertex saturn = g.addV(b.of(LABEL, "titan")).property(NAME, b.of(NAME, "saturn")).property(AGE, b.of(AGE, 10000)).next();
        Vertex sky = g.addV(b.of(LABEL, "location")).property(NAME, b.of(NAME, "sky")).next();
        Vertex sea = g.addV(b.of(LABEL, "location")).property(NAME, b.of(NAME, "sea")).next();
        Vertex jupiter = g.addV(b.of(LABEL, "god")).property(NAME, b.of(NAME, "jupiter")).property(AGE, b.of(AGE, 5000)).next();
        Vertex neptune = g.addV(b.of(LABEL, "god")).property(NAME, b.of(NAME, "neptune")).property(AGE, b.of(AGE, 4500)).next();
        Vertex hercules = g.addV(b.of(LABEL, "demigod")).property(NAME, b.of(NAME, "hercules")).property(AGE, b.of(AGE, 30)).next();
        Vertex alcmene = g.addV(b.of(LABEL, "human")).property(NAME, b.of(NAME, "alcmene")).property(AGE, b.of(AGE, 45)).next();
        Vertex pluto = g.addV(b.of(LABEL, "god")).property(NAME, b.of(NAME, "pluto")).property(AGE, b.of(AGE, 4000)).next();
        Vertex nemean = g.addV(b.of(LABEL, "monster")).property(NAME, b.of(NAME, "nemean")).next();
        Vertex hydra = g.addV(b.of(LABEL, "monster")).property(NAME, b.of(NAME, "hydra")).next();
        Vertex cerberus = g.addV(b.of(LABEL, "monster")).property(NAME, b.of(NAME, "cerberus")).next();
        Vertex tartarus = g.addV(b.of(LABEL, "location")).property(NAME, b.of(NAME, "tartarus")).next();

        g.V(b.of(OUT_V, jupiter)).as("a").V(b.of(IN_V, saturn)).addE(b.of(LABEL, "father")).from("a").next();
        g.V(b.of(OUT_V, jupiter)).as("a").V(b.of(IN_V, sky)).addE(b.of(LABEL, "lives")).property(REASON, b.of(REASON, "loves fresh breezes")).from("a").next();
        g.V(b.of(OUT_V, jupiter)).as("a").V(b.of(IN_V, neptune)).addE(b.of(LABEL, "brother")).from("a").next();
        g.V(b.of(OUT_V, jupiter)).as("a").V(b.of(IN_V, pluto)).addE(b.of(LABEL, "brother")).from("a").next();

        g.V(b.of(OUT_V, neptune)).as("a").V(b.of(IN_V, sea)).addE(b.of(LABEL, "lives")).property(REASON, b.of(REASON, "loves waves")).from("a").next();
        g.V(b.of(OUT_V, neptune)).as("a").V(b.of(IN_V, jupiter)).addE(b.of(LABEL, "brother")).from("a").next();
        g.V(b.of(OUT_V, neptune)).as("a").V(b.of(IN_V, pluto)).addE(b.of(LABEL, "brother")).from("a").next();

        g.V(b.of(OUT_V, hercules)).as("a").V(b.of(IN_V, jupiter)).addE(b.of(LABEL, "father")).from("a").next();
        g.V(b.of(OUT_V, hercules)).as("a").V(b.of(IN_V, alcmene)).addE(b.of(LABEL, "mother")).from("a").next();

        if (supportsGeoshape) {
            g.V(b.of(OUT_V, hercules)).as("a").V(b.of(IN_V, nemean)).addE(b.of(LABEL, "battled"))
                .property(TIME, b.of(TIME, 1)).property(PLACE, b.of(PLACE, Geoshape.point(38.1f, 23.7f))).from("a")
                .next();
            g.V(b.of(OUT_V, hercules)).as("a").V(b.of(IN_V, hydra)).addE(b.of(LABEL, "battled"))
                .property(TIME, b.of(TIME, 2)).property(PLACE, b.of(PLACE, Geoshape.point(37.7f, 23.9f))).from("a")
                .next();
            g.V(b.of(OUT_V, hercules)).as("a").V(b.of(IN_V, cerberus)).addE(b.of(LABEL, "battled"))
                .property(TIME, b.of(TIME, 12)).property(PLACE, b.of(PLACE, Geoshape.point(39f, 22f))).from("a")
                .next();
        } else {
            g.V(b.of(OUT_V, hercules)).as("a").V(b.of(IN_V, nemean)).addE(b.of(LABEL, "battled"))
                .property(TIME, b.of(TIME, 1)).property(PLACE, b.of(PLACE, getGeoFloatArray(38.1f, 23.7f)))
                .from("a").next();
            g.V(b.of(OUT_V, hercules)).as("a").V(b.of(IN_V, hydra)).addE(b.of(LABEL, "battled"))
                .property(TIME, b.of(TIME, 2)).property(PLACE, b.of(PLACE, getGeoFloatArray(37.7f, 23.9f)))
                .from("a").next();
            g.V(b.of(OUT_V, hercules)).as("a").V(b.of(IN_V, cerberus)).addE(b.of(LABEL, "battled"))
                .property(TIME, b.of(TIME, 12)).property(PLACE, b.of(PLACE, getGeoFloatArray(39f, 22f))).from("a")
                .next();
        }

        g.V(b.of(OUT_V, pluto)).as("a").V(b.of(IN_V, jupiter)).addE(b.of(LABEL, "brother")).from("a").next();
        g.V(b.of(OUT_V, pluto)).as("a").V(b.of(IN_V, neptune)).addE(b.of(LABEL, "brother")).from("a").next();
        g.V(b.of(OUT_V, pluto)).as("a").V(b.of(IN_V, tartarus)).addE(b.of(LABEL, "lives")).property(REASON, b.of(REASON, "no fear of death")).from("a").next();
        g.V(b.of(OUT_V, pluto)).as("a").V(b.of(IN_V, cerberus)).addE(b.of(LABEL, "pet")).from("a").next();

        g.V(b.of(OUT_V, cerberus)).as("a").V(b.of(IN_V, tartarus)).addE(b.of(LABEL, "lives")).from("a").next();
    }

    protected float[] getGeoFloatArray(final float lat, final float lon) {
        return new float[]{ lat, lon };
    }

    public void printSchema() {

        System.out.println("==================  printSchema() ================== ");
        final StringBuilder s = new StringBuilder();

        s.append("JanusGraphManagement management = graph.openManagement(); ");
        s.append("management.printSchema(); ");

        ResultSet results = client.submit(s.toString());
        results.stream().forEach(System.out::println);
    }

    public void printV() {
        System.out.println("==================  printV() ================== ");
        final StringBuilder s = new StringBuilder();

        s.append("saturn = g.V().has('name', 'saturn').next(); ");
        s.append("g.V(saturn).valueMap(); ");
        s.append("g.V(saturn).in('father').in('father').values('name'); ");

        ResultSet results = client.submit(s.toString());
        results.stream().forEach(System.out::println);
    }

    public void closeGraph() throws Exception {
        LOGGER.info("closing graph");
        try {
            if (g != null) {
                // this closes the remote, no need to close the empty graph
                g.close();
            }
            if (cluster != null) {
                // the cluster closes all of its clients
                cluster.close();
            }
        } finally {
            g = null;
            graph = null;
            client = null;
            cluster = null;
        }
    }
}
