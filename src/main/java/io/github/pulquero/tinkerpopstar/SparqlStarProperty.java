package io.github.pulquero.tinkerpopstar;

import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Triple;

public class SparqlStarProperty<V> implements Property<V>, RdfableProperty {
	private final Element element;
	private final Triple property;
	private final SparqlStarGraph graph;
	private final RdfMapper<String,IRI> propertyKeyMapper;

	protected SparqlStarProperty(Element e, Triple property, SparqlStarGraph graph, RdfMapper<String,IRI> propertyKeyMapper) {
		this.element = e;
		this.property = property;
		this.graph = graph;
		this.propertyKeyMapper = propertyKeyMapper;
	}

	@Override
	public Triple rdfKey() {
		return property;
	}

	@Override
	public Triple rdfValue() {
		return property;
	}

	@Override
	public Element element() {
		return element;
	}

	@Override
	public String key() {
		return propertyKeyMapper.fromRdf(property.getPredicate());
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
	public void remove() {
		graph.removeProperty(this);
	}

    @Override
	public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

	@Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

	@Override
	public String toString() {
		return StringFactory.propertyString(this);
	}
}
