import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.util.system.ConfigurationUtil;

public class JanusGraphFactoryTest {

    protected static final String CONF_FILE = "conf/janusgraph-cql-es.properties";
    public JanusGraph janusGraph;
    public Configuration conf;


    public static void main(String[] args) {
        JanusGraphFactoryTest janusGraphFactoryTest = new JanusGraphFactoryTest();
        try {
            janusGraphFactoryTest.openGraph();
//            janusGraphFactoryTest.createSchema();
            janusGraphFactoryTest.viewSchema();
        } catch (ConfigurationException e) {
            e.printStackTrace();
        } finally {
            janusGraphFactoryTest.closeGraph();
        }
    }

    public void openGraph() throws ConfigurationException {
        conf = ConfigurationUtil.loadPropertiesConfig(CONF_FILE);
        janusGraph = JanusGraphFactory.open(conf);
    }

    public void closeGraph() {
        janusGraph.close();
    }

    public void createSchema() {
        JanusGraphManagement management = janusGraph.openManagement();
        boolean created = false;

        if (management.getRelationTypes(RelationType.class).iterator().hasNext()) {
            management.rollback();
            created = false;
        } else {
            // properties
            PropertyKey name = management.makePropertyKey("name").dataType(String.class).make();

            // vertex labels
            management.makeVertexLabel("titan").make();

            // edge labels
            management.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();

            management.commit();
        }
    }

    public void viewSchema() {
        System.out.println("=========================== viewSchema ===========================");
        JanusGraphManagement management = janusGraph.openManagement();
        String schema = management.printSchema();
        System.out.println(schema);
    }
}
