package io.github.pulquero.tinkerpopstar;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;

@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = SparqlStarGraphProvider.class, graph = SparqlStarGraph.class)
public class SparqlStarStructureStandardTest {

}
