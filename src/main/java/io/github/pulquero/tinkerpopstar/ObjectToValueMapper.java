package io.github.pulquero.tinkerpopstar;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.UUID;

import javax.xml.bind.DatatypeConverter;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.XSD;

public class ObjectToValueMapper implements RdfMapper<Object,Value> {
	@Override
	public Value toRdf(Object o, ValueFactory vf) {
		if (o instanceof UUID) {
			return vf.createIRI(SparqlStarGraph.UUID_NS, ((UUID)o).toString());
		}
		try {
			return Values.literal(vf, o, true);
		} catch (IllegalArgumentException e) {
			if (o instanceof Serializable) {
				ByteArrayOutputStream bout = new ByteArrayOutputStream();
				try(ObjectOutputStream out = new ObjectOutputStream(bout)) {
					out.writeObject(o);
				} catch(IOException ioe) {
					throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(o, ioe);
				}
				String s = DatatypeConverter.printBase64Binary(bout.toByteArray());
				return vf.createLiteral(s, XSD.BASE64BINARY);
			}
			throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(o, e);
		}
	}

	@Override
	public Object fromRdf(Value v) {
		if (v instanceof Literal) {
			Literal l = (Literal) v;
			if (XSD.INT.equals(l.getDatatype())) {
				return l.intValue();
			} else if (XSD.LONG.equals(l.getDatatype())) {
				return l.longValue();
			} else if (XSD.DOUBLE.equals(l.getDatatype())) {
				return l.doubleValue();
			} else if (XSD.FLOAT.equals(l.getDatatype())) {
				return l.floatValue();
			} else if (XSD.BOOLEAN.equals(l.getDatatype())) {
				return l.booleanValue();
			} else if (XSD.BASE64BINARY.equals(l.getDatatype())) {
				byte[] bytes = DatatypeConverter.parseBase64Binary(l.getLabel());
				try(ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
					return in.readObject();
				} catch (ClassNotFoundException | IOException e) {
					throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(l, e);
				}
			}
			return l.getLabel();
		} else if (v instanceof IRI) {
			IRI iri = (IRI) v;
			if (SparqlStarGraph.UUID_NS.equals(iri.getNamespace())) {
				return UUID.fromString(iri.getLocalName());
			}
			return iri.stringValue();
		} else {
			return v.stringValue();
		}
	}
}
