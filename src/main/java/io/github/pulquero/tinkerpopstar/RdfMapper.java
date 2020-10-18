package io.github.pulquero.tinkerpopstar;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;

/**
 * Invertible mapper.
 */
public interface RdfMapper<O,R extends Value> {
	R toRdf(O o, ValueFactory vf);
	O fromRdf(R r);
}
