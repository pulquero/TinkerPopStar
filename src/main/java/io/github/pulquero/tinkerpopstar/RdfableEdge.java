package io.github.pulquero.tinkerpopstar;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.eclipse.rdf4j.model.Triple;

interface RdfableEdge extends Element {
	/**
	 * Returns a triple that uniquely identifies this edge.
	 * The subject should uniquely identify the out vertex.
	 * The predicate should uniquely identify the label.
	 */
	Triple rdfOut();
	/**
	 * Returns the in-going triple - the object should uniquely identify the in vertex.
	 * Note: usually this is the same triple as {@link #rdfOut()}.
	 */
	Triple rdfIn();
}
