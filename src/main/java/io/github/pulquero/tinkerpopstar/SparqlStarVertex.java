package io.github.pulquero.tinkerpopstar;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;

public class SparqlStarVertex extends SparqlStarElement implements Vertex {

	private final Resource id;
	private final IRI label;

	protected SparqlStarVertex(Resource id, IRI label, SparqlStarGraph graph) {
		super(graph);
		this.id = id;
		this.label = label;
	}

	Resource rdf() {
		return id;
	}

	IRI rdfLabel() {
		return label;
	}

	@Override
	public Object id() {
		return graph.toVertexId(this);
	}

	@Override
	public String label() {
		return graph.toVertexLabel(this);
	}

	@Override
	public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
		if (inVertex == null) {
			throw Graph.Exceptions.argumentCanNotBeNull("inVertex");
		}
		return graph.addEdge(label, this, (SparqlStarVertex)inVertex, keyValues);
	}

	@Override
	public <V> VertexProperty<V> property(Cardinality cardinality, String key, V value, Object... keyValues) {
		return graph.addVertexProperty(this, cardinality, key, value, keyValues);
	}

	@Override
	public void remove() {
		graph.removeVertex(this);
	}

	@Override
	public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
		switch (direction) {
			case OUT:
				return graph.getOutEdges(this, edgeLabels);
			case IN:
				return graph.getInEdges(this, edgeLabels);
			case BOTH:
				return IteratorUtils.concat(graph.getOutEdges(this, edgeLabels), graph.getInEdges(this, edgeLabels));
			default:
				throw new AssertionError();
		}
	}

	@Override
	public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
		switch (direction) {
			case OUT:
				return graph.getOutVertices(this, edgeLabels);
			case IN:
				return graph.getInVertices(this, edgeLabels);
			case BOTH:
				return IteratorUtils.concat(graph.getOutVertices(this, edgeLabels), graph.getInVertices(this, edgeLabels));
			default:
				throw new AssertionError();
		}
	}

	@Override
	public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
		return graph.getVertexProperties(this, propertyKeys);
	}

	@Override
	public String toString() {
		return StringFactory.vertexString(this);
	}
}
