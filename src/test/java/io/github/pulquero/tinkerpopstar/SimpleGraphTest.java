package io.github.pulquero.tinkerpopstar;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.util.iterators.Iterators;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimpleGraphTest {
	private SparqlStarGraph g;

	@Before
	public void init() {
		Repository repo = new SailRepository(new MemoryStore());
		Map<String, Object> conf = new HashMap<>();
		conf.put(Graph.GRAPH, SparqlStarGraph.class.getName());
		conf.put(SparqlStarGraph.REPOSITORY, repo);
		conf.put(SparqlStarGraph.ISOLATION_LEVEL, IsolationLevels.NONE);
		g = SparqlStarGraph.open(new MapConfiguration(conf));
	}

	@After
	public void close() throws Exception {
		g.close();
	}

	@Test
	public void testVariables() {
		g.variables().set("test", Boolean.TRUE);
		assertEquals(1, g.variables().asMap().size());
	}

	@Test
	public void testVertex() {
		Vertex v = g.addVertex();
		assertEquals(1, count(g.vertices()));
		v.remove();
		assertEquals(0, count(g.vertices()));
	}

	@Test
	public void testEdges() {
		Vertex v1 = g.addVertex();
		Vertex v2 = g.addVertex();
		Edge e = v1.addEdge("e", v2, "prop1", "foobar");
		assertEquals(1, count(g.edges()));
		assertEquals(0, count(g.edges("invalid")));
		e.remove();
		assertEquals(0, count(g.edges()));
	}

	@Test
	public void testEdgeProperties() {
		Vertex v1 = g.addVertex();
		Vertex v2 = g.addVertex();
		Edge e = v1.addEdge("e", v2, "prop1", "foobar");
		assertEquals(1, count(e.properties()));
	}

	@Test
	public void testMultiedges() {
		Vertex v1 = g.addVertex();
		Vertex v2 = g.addVertex();
		v1.addEdge("e", v2);
		v1.addEdge("e", v2);
		assertEquals(2, count(v1.edges(Direction.OUT)));
		assertEquals(2, count(v2.edges(Direction.IN)));
		assertEquals(2, count(g.edges()));
	}

	@Test
	public void testVertexProperties() {
		Vertex v = g.addVertex();
		v.property(Cardinality.single, "foo", "bar", "metafoo", "metabar");
		assertEquals(1, count(v.properties()));
		assertEquals(1, count(v.properties("foo")));
		v.property(Cardinality.set, "foo", "meh");
		assertEquals(2, count(v.properties()));
		assertEquals(2, count(v.properties("foo")));
		List<VertexProperty<Object>> vps = getVertexProperties(v, "foo", "bar");
		assertEquals(1, vps.size());
		assertEquals(1, count(vps.get(0).properties()));
		assertEquals(1, count(vps.get(0).properties("metafoo")));
		v.property(Cardinality.list, "foo", "meh");
		assertEquals(3, count(v.properties()));
		assertEquals(3, count(v.properties("foo"))); // set bar, meh and list meh
		v.property(Cardinality.set, "foo", "bar", "metaf00", "metabar");
		assertEquals(2, count(v.properties()));
		assertEquals(2, count(v.properties("foo"))); // set bar, meh
		vps = getVertexProperties(v, "foo", "bar");
		assertEquals(1, vps.size());
		assertEquals(2, count(vps.get(0).properties()));
		assertEquals(1, count(vps.get(0).properties("metafoo")));
		vps = getVertexProperties(v, "foo", "bar");
		assertEquals(1, vps.size());
		assertEquals(2, count(vps.get(0).properties()));
		assertEquals(1, count(vps.get(0).properties("metafoo")));
		v.property(Cardinality.single, "foo", "meh");
		assertEquals(1, count(v.properties()));
		assertEquals(1, count(v.properties("foo")));
	}

	private List<VertexProperty<Object>> getVertexProperties(Vertex v, String key, Object value) {
		return Iterators.asList(v.properties(key)).stream().filter(prop -> value.equals(prop.value())).collect(Collectors.toList());
	}

	private static int count(Iterator<?> iter) {
		int count = 0;
		for(; iter.hasNext(); iter.next()) {
			count++;
		}
		return count;
	}
}
