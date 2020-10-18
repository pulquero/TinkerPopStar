package io.github.pulquero.tinkerpopstar;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.eclipse.rdf4j.model.Triple;

public class SparqlStarEdge extends SparqlStarElement implements Edge, RdfableEdge {

	private final Triple outEdge;
	private final Triple inEdge;
	private final Vertex out;
	private final Vertex in;

	protected SparqlStarEdge(Triple outEdge, Triple inEdge, Vertex out, Vertex in, SparqlStarGraph graph) {
		super(graph);
		this.outEdge = outEdge;
		this.inEdge = inEdge;
		this.out = out;
		this.in = in;
	}

	@Override
	public Triple rdfOut() {
		return outEdge;
	}

	@Override
	public Triple rdfIn() {
		return inEdge;
	}

	@Override
	public Object id() {
		return graph.toEdgeId(this);
	}

	@Override
	public String label() {
		return graph.toEdgeLabel(this);
	}

	@Override
	public <V> Property<V> property(String key, V value) {
		return graph.addEdgeProperty(this, key, value);
	}

	@Override
	public void remove() {
		graph.removeEdge(this);
	}

	@Override
	public Iterator<Vertex> vertices(Direction direction) {
		switch (direction) {
			case OUT:
				return IteratorUtils.of(out);
			case IN:
				return IteratorUtils.of(in);
			default:
				return IteratorUtils.of(out, in);
		}
	}

	@Override
	public <V> Iterator<Property<V>> properties(String... propertyKeys) {
		return graph.getEdgeProperties(this, propertyKeys);
	}

	@Override
	public String toString() {
		return StringFactory.edgeString(this);
	}
}
