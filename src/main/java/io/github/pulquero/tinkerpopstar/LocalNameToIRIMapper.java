package io.github.pulquero.tinkerpopstar;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;

/**
 * Maps a string to an IRI by prefixing with a namespace.
 */
public class LocalNameToIRIMapper implements RdfMapper<String,IRI> {
	private final String ns;

	public LocalNameToIRIMapper(String ns) {
		this.ns = ns;
	}

	@Override
	public IRI toRdf(String s, ValueFactory vf) {
		try {
			return vf.createIRI(ns, URLEncoder.encode(s, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new AssertionError(e);
		}
	}

	@Override
	public String fromRdf(IRI r) {
		try {
			return URLDecoder.decode(r.getLocalName(), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new AssertionError(e);
		}
	}

}
