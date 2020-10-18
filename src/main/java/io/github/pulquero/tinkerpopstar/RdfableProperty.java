package io.github.pulquero.tinkerpopstar;

import org.eclipse.rdf4j.model.Triple;

interface RdfableProperty {
	/**
	 * Returns a triple that uniquely identifies this property.
	 * The predicate should uniquely identify the key.
	 */
	Triple rdfKey();
	/**
	 * Returns the value triple - the object is the value of this property.
	 * Note: usually this is the same triple as {@link #rdfKey()}.
	 */
	Triple rdfValue();
}
