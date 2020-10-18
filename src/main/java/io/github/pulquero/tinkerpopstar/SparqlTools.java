package io.github.pulquero.tinkerpopstar;

import java.util.function.Consumer;
import java.util.stream.Stream;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sparqlbuilder.core.Variable;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPattern;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPatterns;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.TriplePattern;
import org.eclipse.rdf4j.sparqlbuilder.rdf.Rdf;
import org.eclipse.rdf4j.sparqlbuilder.rdf.RdfObject;
import org.eclipse.rdf4j.sparqlbuilder.rdf.RdfPredicate;
import org.eclipse.rdf4j.sparqlbuilder.rdf.RdfResource;
import org.eclipse.rdf4j.sparqlbuilder.rdf.RdfSubject;
import org.eclipse.rdf4j.sparqlbuilder.rdf.RdfValue;

public final class SparqlTools {
	private SparqlTools() {}

	static final TriplePattern asTriplePattern(Triple t) {
		return GraphPatterns.tp(resource(t.getSubject()), t.getPredicate(), t.getObject());
	}

	static final GraphPattern values(Variable var, Stream<? extends Value> valueList) {
		return values(new Variable[] {var}, valueList.map(v -> new Value[] {v}));
	}

	static final GraphPattern values(Variable[] vars, Stream<? extends Value[]> valueList) {
		return new GraphPattern() {
			@Override
			public String getQueryString() {
				StringBuilder buf = new StringBuilder(1024);
				buf.append("VALUES ( ");
				for (Variable var : vars) {
					buf.append(var.getQueryString());
					buf.append(" ");
				}
				buf.append(") { ");
				Consumer<Value[]> valueAppender = new Consumer<Value[]>() {
					@Override
					public void accept(Value[] values) {
						buf.append("( ");
						for (Value val : values) {
							buf.append(value(val).getQueryString());
							buf.append(" ");
						}
						buf.append(") ");
					}
				};
				valueList.forEach(valueAppender);
				buf.append("}\n");
				return buf.toString();
			}
		};
	}

	static final RdfResource triple(Triple v) {
		return triple(resource(v.getSubject()), Rdf.iri(v.getPredicate()), Rdf.object(v.getObject()));
	}

	static final RdfResource triple(RdfSubject subj, RdfPredicate pred, RdfObject obj) {
		return new RdfResource() {
			@Override
			public String getQueryString() {
				StringBuilder buf = new StringBuilder(256);
				buf.append("<<");
				buf.append(subj.getQueryString());
				buf.append(" ");
				buf.append(pred.getQueryString());
				buf.append(" ");
				buf.append(obj.getQueryString());
				buf.append(">>");
				return buf.toString();
			}
		};
	}

	static final RdfValue value(Value v) {
		if (v instanceof Literal) {
			Literal l = (Literal) v;
			return Rdf.literalOfType(l.getLabel(), l.getDatatype());
		} else {
			return resource((Resource)v);
		}
	}

	static final RdfResource resource(Resource res) {
		if (res instanceof IRI) {
			return Rdf.iri((IRI)res);
		} else if (res instanceof BNode) {
			return Rdf.bNode(((BNode)res).getID());
		} else if (res instanceof Triple) {
			return triple((Triple)res);
		} else {
			throw new AssertionError();
		}
	}

	static final GraphPattern group(GraphPattern gp) {
		return () -> "{ " + gp.getQueryString() + " }";
	}

	static final GraphPattern wrap(GraphPattern gp) {
		return () -> gp.getQueryString();
	}
}
