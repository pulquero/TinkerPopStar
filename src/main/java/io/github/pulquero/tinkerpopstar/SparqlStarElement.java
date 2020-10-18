package io.github.pulquero.tinkerpopstar;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

public abstract class SparqlStarElement implements Element {

	protected final SparqlStarGraph graph;

	protected SparqlStarElement(SparqlStarGraph graph) {
		this.graph = graph;
	}

	@Override
	public Graph graph() {
		return graph;
	}

	@Override
	public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

	@Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }
}
