package io.github.pulquero.tinkerpopstar;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.TestSupport;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.mockito.internal.util.collections.Sets;

public class SparqlStarGraphProvider extends AbstractGraphProvider {

	@Override
	public Set<Class> getImplementations() {
		return Sets.<Class>newSet(SparqlStarGraph.class, SparqlStarVertex.class, SparqlStarEdge.class, SparqlStarProperty.class, SparqlStarVertexProperty.class);
	}

	@Override
	public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName,
			GraphData loadGraphWith) {
		Map<String, Object> conf = new HashMap<>();
		conf.put(Graph.GRAPH, SparqlStarGraph.class.getName());
		conf.put(SparqlStarGraph.REPOSITORY, new SailRepository(new MemoryStore()));
		// TODO Transactions are currently broken for RDF*, see https://github.com/eclipse/rdf4j/issues/2604
		conf.put(SparqlStarGraph.ISOLATION_LEVEL, IsolationLevels.NONE);
		return conf;
	}

	@Override
	protected void readIntoGraph(final Graph graph, final String path) throws IOException {
		Path p = Paths.get("resources", path).getParent();
		String[] parts = new String[p.getNameCount()];
		for (int i=0; i<parts.length; i++) {
			parts[i] = p.getName(i).toString();
		}
		TestSupport.makeTestDataPath(graph.getClass(), parts);
		super.readIntoGraph(graph, path);
	}

    @Override
	public void clear(Graph graph, Configuration configuration)
		throws Exception {
		if (graph != null) {
			graph.close();
		}
	}
}
