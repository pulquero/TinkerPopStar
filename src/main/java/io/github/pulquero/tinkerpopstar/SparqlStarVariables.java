package io.github.pulquero.tinkerpopstar;

import java.util.Optional;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Graph.Variables;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.eclipse.rdf4j.model.Resource;

class SparqlStarVariables implements Variables {
	private final SparqlStarGraph graph;
	private Resource var;

	SparqlStarVariables(SparqlStarGraph graph) {
		this.graph = graph;
	}

	Resource rdf() {
		if (var == null) {
			var = graph.getVariables();
		}
		return var;
	}

	@Override
	public void set(String key, Object value) {
		graph.setVariable(this, key, value);
	}

	@Override
	public Set<String> keys() {
		return graph.getVariables(this);
	}

	@Override
	public <R> Optional<R> get(String key) {
		return graph.getVariable(this, key);
	}

	@Override
	public void remove(String key) {
		graph.removeVariable(this, key);
	}

	@Override
	public String toString() {
		return StringFactory.graphVariablesString(this);
	}
}
