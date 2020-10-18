package io.github.pulquero.tinkerpopstar;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.eclipse.rdf4j.model.Triple;

public class SparqlStarVertexProperty<V> extends SparqlStarElement implements VertexProperty<V>, RdfableProperty, RdfableEdge {

	private final Vertex element;
	private final Triple property;
	private final Triple value;

	protected SparqlStarVertexProperty(Vertex v, Triple property, Triple value, SparqlStarGraph graph) {
		super(graph);
		this.element = v;
		this.property = property;
		this.value = value;
	}

	@Override
	public Triple rdfOut() {
		return property;
	}

	@Override
	public Triple rdfIn() {
		return value;
	}

	@Override
	public Triple rdfKey() {
		return property;
	}

	@Override
	public Triple rdfValue() {
		return value;
	}

	@Override
	public Object id() {
		return graph.toVertexPropertyId(this);
	}

	@Override
	public String key() {
		return graph.toVertexPropertyKey(this);
	}

	@Override
	public V value() throws NoSuchElementException {
		return (V) graph.toPropertyValue(this);
	}

	@Override
	public boolean isPresent() {
		return true;
	}

	@Override
	public Vertex element() {
		return element;
	}

	@Override
	public <V> Property<V> property(String key, V value) {
		return graph.addMetaProperty(this, key, value);
	}

	@Override
	public <U> Iterator<Property<U>> properties(String... propertyKeys) {
		return graph.getMetaProperties(this, propertyKeys);
	}

	@Override
	public void remove() {
		graph.removeVertexProperty(this);
	}

	@Override
	public String toString() {
		return StringFactory.propertyString(this);
	}
}
