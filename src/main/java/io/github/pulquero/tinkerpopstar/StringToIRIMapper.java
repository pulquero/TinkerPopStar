package io.github.pulquero.tinkerpopstar;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;

/**
 * Maps a string directly to an IRI.
 */
public class StringToIRIMapper implements RdfMapper<String,IRI> {
	@Override
	public IRI toRdf(String o, ValueFactory vf) {
		return vf.createIRI(o);
	}

	@Override
	public String fromRdf(IRI r) {
		return r.stringValue();
	}
}
