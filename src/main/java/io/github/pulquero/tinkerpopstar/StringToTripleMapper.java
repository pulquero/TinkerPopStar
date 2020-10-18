package io.github.pulquero.tinkerpopstar;

import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;

/**
 * Parses a string to a Triple.
 */
public class StringToTripleMapper implements RdfMapper<String,Triple> {
	@Override
	public Triple toRdf(String o, ValueFactory vf) {
		return NTriplesUtil.parseTriple(o, vf);
	}

	@Override
	public String fromRdf(Triple t) {
		return NTriplesUtil.toNTriplesString(t);
	}
}
