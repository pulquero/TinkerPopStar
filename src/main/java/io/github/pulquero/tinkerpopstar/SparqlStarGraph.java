package io.github.pulquero.tinkerpopstar;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.GraphVariableHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.StoreIteratorCounter;
import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.common.iterator.CloseableIterationIterator;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.sparqlbuilder.constraint.Expressions;
import org.eclipse.rdf4j.sparqlbuilder.core.SparqlBuilder;
import org.eclipse.rdf4j.sparqlbuilder.core.Variable;
import org.eclipse.rdf4j.sparqlbuilder.core.query.DeleteDataQuery;
import org.eclipse.rdf4j.sparqlbuilder.core.query.InsertDataQuery;
import org.eclipse.rdf4j.sparqlbuilder.core.query.ModifyQuery;
import org.eclipse.rdf4j.sparqlbuilder.core.query.Queries;
import org.eclipse.rdf4j.sparqlbuilder.core.query.SelectQuery;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPatterns;
import org.eclipse.rdf4j.sparqlbuilder.rdf.Rdf;
import org.eclipse.rdf4j.util.iterators.ConvertingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldPersistDataOnClose", reason = "MemoryStore doesn't currently support RDF* persistence (and no suitable alternatives)")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.TransactionTest", method = "*", reason = "Transactions are currently broken for RDF*, see https://github.com/eclipse/rdf4j/issues/2604")
public class SparqlStarGraph implements Graph {
	public static final String REPOSITORY = "sparql*.repository";
	public static final String ENDPOINT = "sparql*.endpoint";
	public static final String CONTEXT = "sparql*.graph";
	public static final String MULTIEDGES = "sparql*.multiedges";
	/**
	 * Vertex ID (resource) namespace.
	 */
	public static final String VERTEX_ID_NS = "sparql*.namespace.vertex.id";
	/**
	 * Vertex label (class) namespace.
	 */
	public static final String VERTEX_LABEL_NS = "sparql*.namespace.vertex.label";
	/**
	 * Vertex property (property) namespace.
	 */
	public static final String VERTEX_PROPERTY_NS = "sparql*.namespace.vertex.property";
	/**
	 * Edge label (property) namespace.
	 */
	public static final String EDGE_LABEL_NS = "sparql*.namespace.edge.label";
	/**
	 * Edge property (property) namespace.
	 */
	public static final String EDGE_PROPERTY_NS = "sparql*.namespace.edge.property";
	/**
	 * Meta property (property) namespace.
	 */
	public static final String META_PROPERTY_NS = "sparql*.namespace.meta.property";
	/**
	 * Variable (property) namespace.
	 */
	public static final String VARIABLE_NS = "sparql*.namespace.variable";
	public static final String ISOLATION_LEVEL = "sparql*.isolationLevel";

	private static final Logger logger = LoggerFactory.getLogger(SparqlStarGraph.class);

	private static final IRI VARIABLES_CLASS = SimpleValueFactory.getInstance().createIRI("graph:Variables");
	private static final IRI VARIABLE_PROPERTY = SimpleValueFactory.getInstance().createIRI("graph:variable");
	private static final IRI VERTEX_CLASS = SimpleValueFactory.getInstance().createIRI("graph:Vertex");
	private static final IRI VERTEX_PROPERTY_PROPERTY = SimpleValueFactory.getInstance().createIRI("graph:vertexProperty");
	private static final IRI EDGE_PROPERTY = SimpleValueFactory.getInstance().createIRI("graph:edge");
	private static final IRI EDGE_PROPERTY_PROPERTY = SimpleValueFactory.getInstance().createIRI("graph:edgeProperty");
	private static final IRI META_PROPERTY_PROPERTY = SimpleValueFactory.getInstance().createIRI("graph:metaProperty");
	static final String UUID_NS = "urn:uuid:";

	private final Features features = new SparqlStarFeatures();
	private final Map<AtomicReference<RepositoryConnection>,Long> connHolders = new ConcurrentHashMap<>();
	// use a class from the bootstrap classloader as a holder to prevent any possible classloader leaks
	private final ThreadLocal<AtomicReference<RepositoryConnection>> connections = ThreadLocal.withInitial(() -> {
		AtomicReference<RepositoryConnection> holder = new AtomicReference<>();
		// store the thread ID for reference (e.g. debug)
		connHolders.put(holder, Thread.currentThread().getId());
		return holder;
	});
	private final SparqlStarTransaction transaction = new SparqlStarTransaction(this);
	private final SparqlStarVariables variables = new SparqlStarVariables(this);
	private final Configuration conf;
	private final Repository repo;
	private final ValueFactory vf;
	private final boolean supportsMultiedges;
	private final RdfMapper<Object,Resource> vertexIdMapper;
	private final RdfMapper<String,IRI> vertexLabelMapper;
	private final RdfMapper<String,Triple> vertexPropertyIdMapper;
	private final RdfMapper<String,IRI> vertexPropertyKeyMapper;
	private final RdfMapper<String,Triple> edgeIdMapper;
	private final RdfMapper<String,IRI> edgeLabelMapper;
	private final RdfMapper<String,IRI> edgePropertyKeyMapper;
	private final RdfMapper<String,IRI> metaPropertyKeyMapper;
	private final RdfMapper<String,IRI> variableKeyMapper;
	private final RdfMapper<Object,Value> propertyValueMapper;
	private final IRI context;
	private final IsolationLevel defaultIsolationLevel;

	public static SparqlStarGraph open(Configuration conf) {
		return new SparqlStarGraph(conf);
	}

	private static RdfMapper<String,IRI> createIRIMapper(Configuration conf, String key, String defaultNs, ValueFactory vf) {
		String ns = conf.getString(key, defaultNs);
		if (ns.isEmpty()) {
			return new StringToIRIMapper();
		} else {
			return new LocalNameToIRIMapper(ns);
		}
	}

	protected SparqlStarGraph(Configuration conf) {
		this.conf = conf;
		String endpoint = conf.getString(ENDPOINT);
		if (endpoint != null) {
			this.repo = new SPARQLRepository(endpoint);
		} else {
			this.repo = (Repository) conf.getProperty(REPOSITORY);
		}
		this.vf = repo.getValueFactory();
		this.supportsMultiedges = conf.getBoolean(MULTIEDGES, true);
		this.defaultIsolationLevel = (IsolationLevel) conf.getProperty(ISOLATION_LEVEL);

		String ctx = conf.getString(CONTEXT);
		this.context = (ctx != null) ? vf.createIRI(ctx) : null;
		String vertexIdNs = conf.getString(VERTEX_ID_NS, "graph:vertexId:");
		if (vertexIdNs.isEmpty()) {
			// IDs are IRI strings
			this.vertexIdMapper = new RdfMapper<Object,Resource>() {
				@Override
				public Resource toRdf(Object id, ValueFactory vf) {
					return vf.createIRI(id.toString());
				}
				@Override
				public Object fromRdf(Resource r) {
					return r.stringValue();
				}
			};
		} else {
			// IDs are any object
			this.vertexIdMapper = new RdfMapper<Object,Resource>() {
				@Override
				public Resource toRdf(Object id, ValueFactory vf) {
					try {
						return vf.createIRI(vertexIdNs, URLEncoder.encode(id.toString(), "UTF-8"));
					} catch (UnsupportedEncodingException e) {
						throw new AssertionError(e);
					}
				}
				@Override
				public Object fromRdf(Resource r) {
					try {
						return URLDecoder.decode(((IRI)r).getLocalName(), "UTF-8");
					} catch (UnsupportedEncodingException e) {
						throw new AssertionError(e);
					}
				}
			};
		}
		this.vertexPropertyIdMapper = new StringToTripleMapper();
		this.edgeIdMapper = new StringToTripleMapper();
		this.vertexLabelMapper = createIRIMapper(conf, VERTEX_LABEL_NS, "graph:vertexLabel:", vf);
		this.vertexPropertyKeyMapper = createIRIMapper(conf, VERTEX_PROPERTY_NS, "graph:vertexProperty:", vf);
		this.edgeLabelMapper = createIRIMapper(conf, EDGE_LABEL_NS, "graph:edgeLabel:", vf);
		this.edgePropertyKeyMapper = createIRIMapper(conf, EDGE_PROPERTY_NS, "graph:edgeProperty:", vf);
		this.metaPropertyKeyMapper = createIRIMapper(conf, META_PROPERTY_NS, "graph:metaProperty:", vf);
		this.variableKeyMapper = createIRIMapper(conf, VARIABLE_NS, "graph:metaProperty:", vf);
		this.propertyValueMapper = new ObjectToValueMapper();

		this.repo.init();
	}

	IRI uuidUrn() {
		return uuidUrn(UUID.randomUUID().toString());
	}

	private IRI uuidUrn(String uuid) {
		return vf.createIRI(UUID_NS, uuid);
	}

	RepositoryConnection getExistingConnection() {
		return connections.get().get();
	}

	RepositoryConnection ensureConnection() {
		AtomicReference<RepositoryConnection> holder = connections.get();
		RepositoryConnection conn = holder.get();
		if (conn == null) {
			logger.debug("Opening repository connection for graph {}", System.identityHashCode(this));
			conn = repo.getConnection();
			StoreIteratorCounter.INSTANCE.increment();
			conn.setIsolationLevel(defaultIsolationLevel);
			holder.set(conn);
		}
		return conn;
	}

	protected RepositoryConnection readWriteConnection() {
		RepositoryConnection conn = ensureConnection();
		transaction.readWrite();
		return conn;
	}

	protected void update(String s) {
		logger.debug("Update: {}", s);
		readWriteConnection().prepareUpdate(s).execute();
	}

	protected TupleQueryResult query(String s) {
		logger.debug("Query: {}", s);
		return readWriteConnection().prepareTupleQuery(s).evaluate();
	}

	protected InsertDataQuery intoContext(InsertDataQuery q) {
		if (context != null) {
			q.into(Rdf.iri(context));
		}
		return q;
	}

	protected DeleteDataQuery fromContext(DeleteDataQuery q) {
		if (context != null) {
			q.from(Rdf.iri(context));
		}
		return q;
	}

	protected ModifyQuery withContext(ModifyQuery q) {
		if (context != null) {
			q.with(Rdf.iri(context));
		}
		return q;
	}

	protected SelectQuery fromContext(SelectQuery q) {
		if (context != null) {
			q.from(SparqlBuilder.from(Rdf.iri(context)));
		}
		return q;
	}

	protected void closeConnection() {
		AtomicReference<RepositoryConnection> holder = connections.get();
		try {
			close(holder);
		} finally {
			connections.remove();
			connHolders.remove(holder);
		}
	}

	private void close(AtomicReference<RepositoryConnection> holder) {
		RepositoryConnection conn = holder.get();
		if (conn != null) {
			logger.debug("Closing repository connection for graph {}", System.identityHashCode(this));
			StoreIteratorCounter.INSTANCE.decrement();
			try {
				conn.close();
			} finally {
				holder.set(null);
			}
		}
	}

	@Override
	public Features features() {
		return features;
	}

	@Override
	public Variables variables() {
		return variables;
	}

	@Override
	public Configuration configuration() {
		return conf;
	}

	private Resource toRdfVertexId(Object o) {
		if (o instanceof Resource) {
			return (Resource) o;
		} else if (o instanceof UUID) {
			return uuidUrn(((UUID)o).toString());
		} else if (o instanceof SparqlStarVertex) {
			return ((SparqlStarVertex)o).rdf();
		} else if (o instanceof Vertex) {
			return toRdfVertexId(((Vertex)o).id());
		} else if (o instanceof String) {
			String s = (String) o;
			try {
				UUID.fromString(s);
				return uuidUrn(s);
			} catch(IllegalArgumentException e) {
				// ignore - not a UUID
			}
		}
		return vertexIdMapper.toRdf(o, vf);
	}

	private Triple toRdfEdgeId(Object o) {
		if (o instanceof Triple) {
			return (Triple) o;
		} else if (o instanceof String) {
			return edgeIdMapper.toRdf((String)o, vf);
		} else if (o instanceof SparqlStarEdge) {
			return ((SparqlStarEdge)o).rdfOut();
		} else if (o instanceof Edge) {
			return toRdfEdgeId(((Edge)o).id());
		}
		throw new IllegalArgumentException(String.format("Unsupported edge ID: %s", o));
	}

	Object toVertexId(SparqlStarVertex v) {
		Resource res = v.rdf();
		if (res instanceof IRI) {
			IRI iri = (IRI) res;
			if (UUID_NS.equals(iri.getNamespace())) {
				return UUID.fromString(iri.getLocalName());
			}
		}
		return vertexIdMapper.fromRdf(res);
	}

	String toVertexLabel(SparqlStarVertex v) {
		return vertexLabelMapper.fromRdf(v.rdfLabel());
	}

	Object toVertexPropertyId(SparqlStarVertexProperty<?> v) {
		return vertexPropertyIdMapper.fromRdf(v.rdfOut());
	}

	String toVertexPropertyKey(SparqlStarVertexProperty<?> property) {
		return vertexPropertyKeyMapper.fromRdf(property.rdfKey().getPredicate());
	}

	Object toEdgeId(SparqlStarEdge edge) {
		return edgeIdMapper.fromRdf(edge.rdfOut());
	}

	String toEdgeLabel(SparqlStarEdge edge) {
		return edgeLabelMapper.fromRdf(edge.rdfOut().getPredicate());
	}

	Object toPropertyValue(RdfableProperty prop) {
		return propertyValueMapper.fromRdf(prop.rdfValue().getObject());
	}

	Resource getVariables() {
		Optional<Resource> v = queryVariables();
		if (!v.isPresent()) {
			ModifyQuery insert = withContext(Queries.MODIFY().insert(
				GraphPatterns.tp(uuidUrn(), RDF.TYPE, VARIABLES_CLASS)
			).where(
				() -> "BIND(\"insert\" AS ?action)",
				GraphPatterns.filterNotExists(
					SparqlBuilder.var("s").isA(Rdf.iri(VARIABLES_CLASS))
			)));
			update(insert.getQueryString());
			v = queryVariables();
		}
		return v.get();
	}

	private Optional<Resource> queryVariables() {
		Variable subjVar = SparqlBuilder.var("s");
		SelectQuery q = fromContext(Queries.SELECT(subjVar).where(
			subjVar.isA(Rdf.iri(VARIABLES_CLASS))
		));
		return (Optional<Resource>) getSingleResult(query(q.getQueryString()), "s");
	}

	private Optional<? extends Value> getSingleResult(TupleQueryResult r, String var) {
		try {
			if (r.hasNext()) {
				Value v = r.next().getValue(var);
				if (r.hasNext()) {
					throw new IllegalStateException("Multiple results found");
				}
				return Optional.of(v);
			}
		} finally {
			r.close();
		}
		return Optional.empty();
	}

	@Override
	public Vertex addVertex(Object... keyValues) {
		logger.debug("addVertex");
		ElementHelper.legalPropertyKeyValueArray(keyValues);
		Resource id = ElementHelper.getIdValue(keyValues).map(this::toRdfVertexId).orElseGet(() -> uuidUrn(UUID.randomUUID().toString()));
		String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
		IRI labelType = vertexLabelMapper.toRdf(label, vf);
		RepositoryConnection conn = readWriteConnection();
		if (conn.hasStatement(id, RDF.TYPE, labelType, false, context)) {
			throw Graph.Exceptions.vertexWithIdAlreadyExists(id);
		}
		InsertDataQuery q = intoContext(Queries.INSERT_DATA(
			GraphPatterns.tp(id, RDF.TYPE, labelType),
			GraphPatterns.tp(labelType, RDFS.SUBCLASSOF, VERTEX_CLASS)
		));
		update(q.getQueryString());
		SparqlStarVertex v = new SparqlStarVertex(id, labelType, this);
		ElementHelper.attachProperties(v, keyValues);
		return v;
	}

	Edge addEdge(String label, SparqlStarVertex outVertex, SparqlStarVertex inVertex, Object... keyValues) {
		logger.debug("addEdge");
		ElementHelper.validateLabel(label);
		ElementHelper.legalPropertyKeyValueArray(keyValues);
		if(ElementHelper.getIdValue(keyValues).isPresent()) {
			throw Edge.Exceptions.userSuppliedIdsNotSupported();
		}
		IRI labelProperty = edgeLabelMapper.toRdf(label, vf);
		Triple outEdge;
		Triple inEdge;
		if (supportsMultiedges) {
			Resource extedge = uuidUrn();
			outEdge = vf.createTriple(outVertex.rdf(), labelProperty, extedge);
			inEdge = vf.createTriple(extedge, RDFS.SEEALSO, inVertex.rdf());
		} else {
			outEdge = vf.createTriple(outVertex.rdf(), labelProperty, inVertex.rdf());
			inEdge = outEdge;
		}
		InsertDataQuery q = intoContext(Queries.INSERT_DATA(
			SparqlTools.asTriplePattern(outEdge),
			GraphPatterns.tp(outEdge.getPredicate(), RDFS.SUBPROPERTYOF, EDGE_PROPERTY)
		));
		if (supportsMultiedges) {
			q.insertData(SparqlTools.asTriplePattern(inEdge));
		}
		update(q.getQueryString());
		SparqlStarEdge e = new SparqlStarEdge(outEdge, inEdge, outVertex, inVertex, this);
		ElementHelper.attachProperties(e, keyValues);
		return e;
	}

	<V> VertexProperty<V> addVertexProperty(SparqlStarVertex v, Cardinality cardinality, String key, V value, Object... keyValues) {
		logger.debug("addVertexProperty");
		ElementHelper.validateProperty(key, value);
		if(ElementHelper.getIdValue(keyValues).isPresent()) {
			throw Edge.Exceptions.userSuppliedIdsNotSupported();
		}
		Resource vid = v.rdf();
		IRI propKey = vertexPropertyKeyMapper.toRdf(key, vf);

		Variable valueVar = SparqlBuilder.var("value");
		Variable extVar = SparqlBuilder.var("ext");
		Variable vpkVar = SparqlBuilder.var("vpKey");
		Variable vpvVar = SparqlBuilder.var("vpValue");
		ModifyQuery deleteListQuery = withContext(Queries.MODIFY()
			.delete(
				GraphPatterns.tp(vid, propKey, valueVar),
				valueVar.has(RDF.VALUE, extVar),
				SparqlTools.triple(SparqlTools.resource(vid), Rdf.iri(propKey), valueVar).has(vpkVar, vpvVar)
			).where(
				GraphPatterns.tp(vid, propKey, valueVar),
				valueVar.has(RDF.VALUE, extVar),
				SparqlTools.triple(SparqlTools.resource(vid), Rdf.iri(propKey), valueVar).has(vpkVar, vpvVar).optional(),
				GraphPatterns.tp(propKey, RDFS.SUBPROPERTYOF, VERTEX_PROPERTY_PROPERTY)
			));

		Value propValue;
		Value ext;
		switch (cardinality) {
			case single:
				update(deleteListQuery.getQueryString());
				propValue = propertyValueMapper.toRdf(value, vf);
				ext = null;
				ModifyQuery deleteSetQuery = withContext(Queries.MODIFY().delete(
					GraphPatterns.tp(vid, propKey, valueVar),
					SparqlTools.triple(SparqlTools.resource(vid), Rdf.iri(propKey), valueVar).has(vpkVar, vpvVar)
				).where(
					GraphPatterns.tp(vid, propKey, valueVar)
					.and(SparqlTools.triple(SparqlTools.resource(vid), Rdf.iri(propKey), valueVar).has(vpkVar, vpvVar).optional())
					.filter(Expressions.notEquals(valueVar, SparqlTools.value(propValue))),
					GraphPatterns.tp(propKey, RDFS.SUBPROPERTYOF, VERTEX_PROPERTY_PROPERTY)
				));
				update(deleteSetQuery.getQueryString());
				break;
			case set:
				update(deleteListQuery.getQueryString());
				propValue = propertyValueMapper.toRdf(value, vf);
				ext = null;
				break;
			case list:
				propValue = uuidUrn();
				ext = propertyValueMapper.toRdf(value, vf);
				break;
			default:
				throw new AssertionError();
		}
		Triple propt = vf.createTriple(vid, propKey, propValue);

		InsertDataQuery insertQuery = intoContext(Queries.INSERT_DATA(
			SparqlTools.asTriplePattern(propt),
			GraphPatterns.tp(propKey, RDFS.SUBPROPERTYOF, VERTEX_PROPERTY_PROPERTY)
		));
		if (ext != null) {
			insertQuery.insertData(GraphPatterns.tp((Resource) propValue, RDF.VALUE, ext));
		}
		update(insertQuery.getQueryString());

		Triple valuet = (ext != null) ? vf.createTriple((Resource) propValue, RDF.VALUE, ext) : propt;
		SparqlStarVertexProperty<V> vp = new SparqlStarVertexProperty<>(v, propt, valuet, this);
		ElementHelper.attachProperties(vp, keyValues);
		return vp;
	}

	<V> Property<V> addEdgeProperty(SparqlStarEdge e, String key, V value) {
		logger.debug("addEdgeProperty");
		return addProperty(e, edgePropertyKeyMapper, EDGE_PROPERTY_PROPERTY, key, value);
	}

	<V> Property<V> addMetaProperty(SparqlStarVertexProperty<?> e, String key, V value) {
		logger.debug("addMetaProperty");
		return addProperty(e, metaPropertyKeyMapper, META_PROPERTY_PROPERTY, key, value);
	}

	private <V> Property<V> addProperty(RdfableEdge e, RdfMapper<String,IRI> propKeyMapper, IRI propertyProperty, String key, V value) {
		ElementHelper.validateProperty(key, value);
		Triple edge = e.rdfOut();
		IRI propKey = propKeyMapper.toRdf(key, vf);
		Value propValue = propertyValueMapper.toRdf(value, vf);
		setProperty(edge, propertyProperty, propKey, propValue);
		Triple prop = vf.createTriple(edge, propKey, propValue);
		return new SparqlStarProperty<>(e, prop, this, propKeyMapper);
	}

	void setVariable(SparqlStarVariables vars, String key, Object value) {
		logger.debug("setVariable");
		GraphVariableHelper.validateVariable(key, value);
		IRI propKey = variableKeyMapper.toRdf(key, vf);
		Value propValue = propertyValueMapper.toRdf(value, vf);
		setProperty(vars.rdf(), VARIABLE_PROPERTY, propKey, propValue);
	}

	void removeVariable(SparqlStarVariables vars, String key) {
		logger.debug("removeVertex");
		IRI propKey = variableKeyMapper.toRdf(key, vf);
		removePropertyValues(vars.rdf(), propKey);
	}

	private void setProperty(Resource subj, IRI propertyProperty, IRI propKey, Value propValue) {
		removePropertyValues(subj, propKey);
		InsertDataQuery insertQuery = intoContext(Queries.INSERT_DATA(
			GraphPatterns.tp(SparqlTools.resource(subj), propKey, propValue),
			GraphPatterns.tp(propKey, RDFS.SUBPROPERTYOF, propertyProperty)
		));
		update(insertQuery.getQueryString());
	}

	private void removePropertyValues(Resource subj, IRI propKey) {
		Variable valueVar = SparqlBuilder.var("v");
		ModifyQuery removePropsQuery = withContext(Queries.MODIFY().delete()
		.where(
			GraphPatterns.tp(SparqlTools.resource(subj), propKey, valueVar)
		));
		update(removePropsQuery.getQueryString());
	}

	void removeVertexProperty(SparqlStarVertexProperty<?> prop) {
		logger.debug("removeVertexProperty");
		Triple key = prop.rdfKey();
		Triple value = prop.rdfValue();

		Variable vpkVar = SparqlBuilder.var("vpKey");
		Variable vpvVar = SparqlBuilder.var("vpValue");
		ModifyQuery deleteMetaQuery = withContext(Queries.MODIFY()
			.delete()
			.where(
				GraphPatterns.tp(SparqlTools.triple(key), vpkVar, vpvVar)
			));
		update(deleteMetaQuery.getQueryString());

		DeleteDataQuery deleteQuery = fromContext(Queries.DELETE_DATA(
			SparqlTools.asTriplePattern(key),
			SparqlTools.asTriplePattern(value)
		));
		update(deleteQuery.getQueryString());
	}

	void removeProperty(RdfableProperty prop) {
		logger.debug("removeProperty");
		Triple key = prop.rdfKey();
		Triple value = prop.rdfValue();
		DeleteDataQuery q = fromContext(Queries.DELETE_DATA(
			SparqlTools.asTriplePattern(key),
			SparqlTools.asTriplePattern(value)
		));
		update(q.getQueryString());
	}

	void removeVertex(SparqlStarVertex v) {
		logger.debug("removeVertex");
		Resource vid = v.rdf();
		Variable predVar = SparqlBuilder.var("p");
		Variable objVar = SparqlBuilder.var("o");
		Variable vpkVar = SparqlBuilder.var("vpKey");
		Variable vpvVar = SparqlBuilder.var("vpValue");
		Variable extVar = SparqlBuilder.var("ext");
		ModifyQuery deletePropsQuery = withContext(Queries.MODIFY().delete(
			SparqlTools.resource(vid).has(predVar, objVar),
			SparqlTools.triple(SparqlTools.resource(vid), predVar, objVar).has(vpkVar, vpvVar),
			vpvVar.has(RDF.VALUE, extVar)
		).where(
			SparqlTools.resource(vid).has(predVar, objVar),
			GraphPatterns.and(
				SparqlTools.triple(SparqlTools.resource(vid), predVar, objVar).has(vpkVar, vpvVar),
				vpvVar.has(RDF.VALUE, extVar).optional()
			).optional()
		));
		update(deletePropsQuery.getQueryString());

		DeleteDataQuery deleteQuery = fromContext(Queries.DELETE_DATA(
			GraphPatterns.tp(vid, RDF.TYPE, v.rdfLabel())
		));
		update(deleteQuery.getQueryString());
	}

	void removeEdge(SparqlStarEdge e) {
		logger.debug("removeEdge");
		Triple out = e.rdfOut();
		Triple in = e.rdfIn();

		Variable keyVar = SparqlBuilder.var("key");
		Variable valueVar = SparqlBuilder.var("value");
		ModifyQuery deleteMetaQuery = withContext(Queries.MODIFY()
			.delete()
			.where(
				GraphPatterns.tp(SparqlTools.triple(out), keyVar, valueVar)
		));
		update(deleteMetaQuery.getQueryString());

		DeleteDataQuery deleteQuery = fromContext(Queries.DELETE_DATA(
			SparqlTools.asTriplePattern(out),
			SparqlTools.asTriplePattern(in)
		));
		update(deleteQuery.getQueryString());
	}

	<R> Optional<R> getVariable(SparqlStarVariables vars, String key) {
		IRI propKey = variableKeyMapper.toRdf(key, vf);
		Variable valueVar = SparqlBuilder.var("value");
		SelectQuery q = fromContext(Queries.SELECT(valueVar).where(
			GraphPatterns.tp(vars.rdf(), propKey, valueVar),
			GraphPatterns.tp(propKey, RDFS.SUBPROPERTYOF, VARIABLE_PROPERTY)
		));
		return (Optional<R>) getSingleResult(query(q.getQueryString()), "value").map(propertyValueMapper::fromRdf);
	}

	Set<String> getVariables(SparqlStarVariables vars) {
		Variable keyVar = SparqlBuilder.var("key");
		Variable valueVar = SparqlBuilder.var("value");
		SelectQuery q = fromContext(Queries.SELECT(keyVar).where(
			GraphPatterns.tp(vars.rdf(), keyVar, valueVar),
			GraphPatterns.tp(keyVar, RDFS.SUBPROPERTYOF, VARIABLE_PROPERTY)
		));
		Set<String> keys = new HashSet<>();
		try(TupleQueryResult iter = query(q.getQueryString())) {
			while (iter.hasNext()) {
				IRI propKey = (IRI) iter.next().getValue("key");
				keys.add(variableKeyMapper.fromRdf(propKey));
			}
		}
		return keys;
	}

	<V> Iterator<VertexProperty<V>> getVertexProperties(SparqlStarVertex v, String... propertyKeys) {
		Resource vertex = v.rdf();
		Variable keyVar = SparqlBuilder.var("key");
		Variable valueVar = SparqlBuilder.var("value");
		Variable extVar = SparqlBuilder.var("ext");
		SelectQuery q = fromContext(Queries.SELECT(keyVar, valueVar, extVar));
		if (propertyKeys.length > 0) {
			q.where(SparqlTools.values(keyVar, Arrays.stream(propertyKeys).map(key -> vertexPropertyKeyMapper.toRdf(key, vf))));
		}
		q.where(
			SparqlTools.resource(vertex).has(keyVar, valueVar),
			valueVar.has(RDF.VALUE, extVar).optional(),
			keyVar.has(RDFS.SUBPROPERTYOF, VERTEX_PROPERTY_PROPERTY)
		).distinct();
		TupleQueryResult iter = query(q.getQueryString());
		return new ConvertingIterator<BindingSet, VertexProperty<V>>(new CloseableIterationIterator<>(iter)) {
			@Override
			protected VertexProperty<V> convert(BindingSet bs) {
				IRI key = (IRI) bs.getValue("key");
				Value value = bs.getValue("value");
				Value ext = bs.getValue("ext");
				Triple vertexProperty = vf.createTriple(vertex, key, value);
				Triple vertexPropertyValue = (ext != null) ? vf.createTriple((Resource)value, RDF.VALUE, ext) : vertexProperty;
				return new SparqlStarVertexProperty<>(v, vertexProperty, vertexPropertyValue, SparqlStarGraph.this);
			}
		};
	}

	<V> Iterator<Property<V>> getEdgeProperties(SparqlStarEdge e, String... propertyKeys) {
		return getProperties(e, edgePropertyKeyMapper, EDGE_PROPERTY_PROPERTY, propertyKeys);
	}

	<V> Iterator<Property<V>> getMetaProperties(SparqlStarVertexProperty<?> e, String... propertyKeys) {
		return getProperties(e, metaPropertyKeyMapper, META_PROPERTY_PROPERTY, propertyKeys);
	}

	<V> Iterator<Property<V>> getProperties(RdfableEdge e, RdfMapper<String,IRI> propKeyMapper, IRI propertyProperty, String... propertyKeys) {
		Triple outEdge = e.rdfOut();
		Variable keyVar = SparqlBuilder.var("key");
		Variable valueVar = SparqlBuilder.var("value");
		SelectQuery q = fromContext(Queries.SELECT(keyVar, valueVar));
		if (propertyKeys.length > 0) {
			q.where(SparqlTools.values(keyVar, Arrays.stream(propertyKeys).map(key -> propKeyMapper.toRdf(key, vf))));
		}
		q.where(
			SparqlTools.triple(outEdge).has(keyVar, valueVar),
			keyVar.has(RDFS.SUBPROPERTYOF, propertyProperty)
		).distinct();
		TupleQueryResult iter = query(q.getQueryString());
		return new ConvertingIterator<BindingSet, Property<V>>(new CloseableIterationIterator<>(iter)) {
			@Override
			protected Property<V> convert(BindingSet bs) {
				IRI key = (IRI) bs.getValue("key");
				Value value = bs.getValue("value");
				Triple edgeProperty = vf.createTriple(outEdge, key, value);
				return new SparqlStarProperty<>(e, edgeProperty, SparqlStarGraph.this, propKeyMapper);
			}
		};
	}

	Iterator<Edge> getOutEdges(SparqlStarVertex v, String... edgeLabels) {
		Resource out = v.rdf();
		Variable edgeVar = SparqlBuilder.var("edge");
		Variable inVar = SparqlBuilder.var("in");
		Variable inLabelVar = SparqlBuilder.var("inLabel");
		SelectQuery q = fromContext(Queries.SELECT(edgeVar, inVar, inLabelVar));
		if (edgeLabels.length > 0) {
			q.where(SparqlTools.values(edgeVar, Arrays.stream(edgeLabels).map(label -> edgeLabelMapper.toRdf(label, vf))));
		}
		if (supportsMultiedges) {
			Variable extVar = SparqlBuilder.var("ext");
			q.select(extVar);
			q.where(
				SparqlTools.resource(out).has(edgeVar, extVar),
				extVar.has(RDFS.SEEALSO, inVar),
				inVar.isA(inLabelVar),
				inLabelVar.has(RDFS.SUBCLASSOF, VERTEX_CLASS),
				edgeVar.has(RDFS.SUBPROPERTYOF, EDGE_PROPERTY)
			);
		} else {
			q.where(
				SparqlTools.resource(out).has(edgeVar, inVar),
				inVar.isA(inLabelVar),
				inLabelVar.has(RDFS.SUBCLASSOF, VERTEX_CLASS),
				edgeVar.has(RDFS.SUBPROPERTYOF, EDGE_PROPERTY)
			);
		}
		TupleQueryResult iter = query(q.getQueryString());
		return new ConvertingIterator<BindingSet, Edge>(new CloseableIterationIterator<>(iter)) {
			@Override
			protected Edge convert(BindingSet bs) {
				IRI edgeProperty = (IRI) bs.getValue("edge");
				Resource in = (Resource) bs.getValue("in");
				IRI inLabel = (IRI) bs.getValue("inLabel");
				Triple outEdge;
				Triple inEdge;
				if (supportsMultiedges) {
					Resource ext = (Resource) bs.getValue("ext");
					outEdge = vf.createTriple(out, edgeProperty, ext);
					inEdge = vf.createTriple(ext, RDFS.SEEALSO, in);
				} else {
					outEdge = vf.createTriple(out, edgeProperty, in);
					inEdge = outEdge;
				}
				Vertex inV = new SparqlStarVertex(in, inLabel, SparqlStarGraph.this);
				return new SparqlStarEdge(outEdge, inEdge, v, inV, SparqlStarGraph.this);
			}
		};
	}

	Iterator<Edge> getInEdges(SparqlStarVertex v, String... edgeLabels) {
		Resource in = v.rdf();
		Variable edgeVar = SparqlBuilder.var("edge");
		Variable outVar = SparqlBuilder.var("out");
		Variable outLabelVar = SparqlBuilder.var("outLabel");
		SelectQuery q = fromContext(Queries.SELECT(edgeVar, outVar, outLabelVar));
		if (edgeLabels.length > 0) {
			q.where(SparqlTools.values(edgeVar, Arrays.stream(edgeLabels).map(label -> edgeLabelMapper.toRdf(label, vf))));
		}
		if (supportsMultiedges) {
			Variable extVar = SparqlBuilder.var("ext");
			q.select(extVar);
			q.where(
				outVar.has(edgeVar, extVar),
				extVar.has(RDFS.SEEALSO, in),
				outVar.isA(outLabelVar),
				outLabelVar.has(RDFS.SUBCLASSOF, VERTEX_CLASS),
				edgeVar.has(RDFS.SUBPROPERTYOF, EDGE_PROPERTY)
			);
		} else {
			q.where(
				outVar.has(edgeVar, in),
				outVar.isA(outLabelVar),
				outLabelVar.has(RDFS.SUBCLASSOF, VERTEX_CLASS),
				edgeVar.has(RDFS.SUBPROPERTYOF, EDGE_PROPERTY)
			);
		}
		TupleQueryResult iter = query(q.getQueryString());
		return new ConvertingIterator<BindingSet, Edge>(new CloseableIterationIterator<>(iter)) {
			@Override
			protected Edge convert(BindingSet bs) {
				IRI edgeProperty = (IRI) bs.getValue("edge");
				Resource out = (Resource) bs.getValue("out");
				IRI outLabel = (IRI) bs.getValue("outLabel");
				Triple outEdge;
				Triple inEdge;
				if (supportsMultiedges) {
					Resource ext = (Resource) bs.getValue("ext");
					outEdge = vf.createTriple(out, edgeProperty, ext);
					inEdge = vf.createTriple(ext, RDFS.SEEALSO, in);
				} else {
					outEdge = vf.createTriple(out, edgeProperty, in);
					inEdge = outEdge;
				}
				Vertex outV = new SparqlStarVertex(out, outLabel, SparqlStarGraph.this);
				return new SparqlStarEdge(outEdge, inEdge, outV, v, SparqlStarGraph.this);
			}
		};
	}

	Iterator<Vertex> getOutVertices(SparqlStarVertex v, String... edgeLabels) {
		Resource out = v.rdf();
		Variable edgeVar = SparqlBuilder.var("edge");
		Variable inVar = SparqlBuilder.var("in");
		Variable inLabelVar = SparqlBuilder.var("inLabel");
		SelectQuery q = fromContext(Queries.SELECT(inVar, inLabelVar));
		if (edgeLabels.length > 0) {
			q.where(SparqlTools.values(edgeVar, Arrays.stream(edgeLabels).map(label -> edgeLabelMapper.toRdf(label, vf))));
		}
		if (supportsMultiedges) {
			Variable extVar = SparqlBuilder.var("ext");
			q.where(
				SparqlTools.resource(out).has(edgeVar, extVar),
				extVar.has(RDFS.SEEALSO, inVar),
				inVar.isA(inLabelVar),
				inLabelVar.has(RDFS.SUBCLASSOF, VERTEX_CLASS),
				edgeVar.has(RDFS.SUBPROPERTYOF, EDGE_PROPERTY)
			);
		} else {
			q.where(
				SparqlTools.resource(out).has(edgeVar, inVar),
				inVar.isA(inLabelVar),
				inLabelVar.has(RDFS.SUBCLASSOF, VERTEX_CLASS),
				edgeVar.has(RDFS.SUBPROPERTYOF, EDGE_PROPERTY)
			);
		}
		TupleQueryResult iter = query(q.getQueryString());
		return new ConvertingIterator<BindingSet, Vertex>(new CloseableIterationIterator<>(iter)) {
			@Override
			protected Vertex convert(BindingSet bs) {
				Resource vId = (Resource) bs.getValue("in");
				IRI vLabel = (IRI) bs.getValue("inLabel");
				return new SparqlStarVertex(vId, vLabel, SparqlStarGraph.this);
			}
		};
	}

	Iterator<Vertex> getInVertices(SparqlStarVertex v, String... edgeLabels) {
		Resource in = v.rdf();
		Variable edgeVar = SparqlBuilder.var("edge");
		Variable outVar = SparqlBuilder.var("out");
		Variable outLabelVar = SparqlBuilder.var("outLabel");
		SelectQuery q = fromContext(Queries.SELECT(outVar, outLabelVar));
		if (edgeLabels.length > 0) {
			q.where(SparqlTools.values(edgeVar, Arrays.stream(edgeLabels).map(label -> edgeLabelMapper.toRdf(label, vf))));
		}
		if (supportsMultiedges) {
			Variable extVar = SparqlBuilder.var("ext");
			q.where(
				outVar.has(edgeVar, extVar),
				extVar.has(RDFS.SEEALSO, SparqlTools.resource(in)),
				outVar.isA(outLabelVar),
				outLabelVar.has(RDFS.SUBCLASSOF, VERTEX_CLASS),
				edgeVar.has(RDFS.SUBPROPERTYOF, EDGE_PROPERTY)
			);
		} else {
			q.where(
				outVar.has(edgeVar, in),
				outVar.isA(outLabelVar),
				outLabelVar.has(RDFS.SUBCLASSOF, VERTEX_CLASS),
				edgeVar.has(RDFS.SUBPROPERTYOF, EDGE_PROPERTY)
			);
		}
		TupleQueryResult iter = query(q.getQueryString());
		return new ConvertingIterator<BindingSet, Vertex>(new CloseableIterationIterator<>(iter)) {
			@Override
			protected Vertex convert(BindingSet bs) {
				Resource vId = (Resource) bs.getValue("out");
				IRI vLabel = (IRI) bs.getValue("outLabel");
				return new SparqlStarVertex(vId, vLabel, SparqlStarGraph.this);
			}
		};
	}

	@Override
	public Iterator<Vertex> vertices(Object... vertexIds) {
		Variable idVar = SparqlBuilder.var("id");
		Variable labelVar = SparqlBuilder.var("label");
		SelectQuery q = fromContext(Queries.SELECT(idVar, labelVar));
		if (vertexIds.length > 0) {
			q.where(SparqlTools.values(idVar, Arrays.stream(vertexIds).map(this::toRdfVertexId)));
		}
		q.where(
			idVar.isA(labelVar),
			labelVar.has(RDFS.SUBCLASSOF, VERTEX_CLASS)
		);
		TupleQueryResult iter = query(q.getQueryString());
		return new ConvertingIterator<BindingSet, Vertex>(new CloseableIterationIterator<>(iter)) {
			@Override
			protected Vertex convert(BindingSet bs) {
				Resource id = (Resource) bs.getValue("id");
				IRI label = (IRI) bs.getValue("label");
				return new SparqlStarVertex(id, label, SparqlStarGraph.this);
			}
		};
	}

	@Override
	public Iterator<Edge> edges(Object... edgeIds) {
		Variable edgeVar = SparqlBuilder.var("edge");
		Variable extVar = SparqlBuilder.var("ext");
		Variable outVar = SparqlBuilder.var("out");
		Variable outLabelVar = SparqlBuilder.var("outLabel");
		Variable inVar = SparqlBuilder.var("in");
		Variable inLabelVar = SparqlBuilder.var("inLabel");
		SelectQuery q = fromContext(Queries.SELECT(edgeVar, outVar, outLabelVar, inVar, inLabelVar));
		if (edgeIds.length > 0) {
			q.where(SparqlTools.values(
				new Variable[] {outVar, edgeVar, supportsMultiedges ? extVar : inVar},
				Arrays.stream(edgeIds).map(id -> {
					try {
						Triple t = toRdfEdgeId(id);
						return new Value[] {t.getSubject(), t.getPredicate(), t.getObject()};
					} catch(RuntimeException e) {
						return null;
					}
				}
			).filter(Objects::nonNull)));
		}
		if (supportsMultiedges) {
			q.select(extVar);
			q.where(
				outVar.has(edgeVar, extVar),
				extVar.has(RDFS.SEEALSO, inVar),
				outVar.isA(outLabelVar),
				outLabelVar.has(RDFS.SUBCLASSOF, VERTEX_CLASS),
				inVar.isA(inLabelVar),
				inLabelVar.has(RDFS.SUBCLASSOF, VERTEX_CLASS),
				edgeVar.has(RDFS.SUBPROPERTYOF, EDGE_PROPERTY)
			);
		} else {
			q.where(
				outVar.has(edgeVar, inVar),
				outVar.isA(outLabelVar),
				outLabelVar.has(RDFS.SUBCLASSOF, VERTEX_CLASS),
				inVar.isA(inLabelVar),
				inLabelVar.has(RDFS.SUBCLASSOF, VERTEX_CLASS),
				edgeVar.has(RDFS.SUBPROPERTYOF, EDGE_PROPERTY)
			);
		}
		TupleQueryResult iter = query(q.getQueryString());
		return new ConvertingIterator<BindingSet, Edge>(new CloseableIterationIterator<>(iter)) {
			@Override
			protected Edge convert(BindingSet bs) {
				IRI edgeProperty = (IRI) bs.getValue("edge");
				Resource out = (Resource) bs.getValue("out");
				IRI outLabel = (IRI) bs.getValue("outLabel");
				Resource in = (Resource) bs.getValue("in");
				IRI inLabel = (IRI) bs.getValue("inLabel");
				Triple outEdge;
				Triple inEdge;
				if (supportsMultiedges) {
					Resource ext = (Resource) bs.getValue("ext");
					outEdge = vf.createTriple(out, edgeProperty, ext);
					inEdge = vf.createTriple(ext, RDFS.SEEALSO, in);
				} else {
					outEdge = vf.createTriple(out, edgeProperty, in);
					inEdge = outEdge;
				}
				Vertex outV = new SparqlStarVertex(out, outLabel, SparqlStarGraph.this);
				Vertex inV = new SparqlStarVertex(in, inLabel, SparqlStarGraph.this);
				return new SparqlStarEdge(outEdge, inEdge, outV, inV, SparqlStarGraph.this);
			}
		};
	}

	@Override
	public Transaction tx() {
		return transaction;
	}

	@Override
	public <C extends GraphComputer> C compute(Class<C> graphComputerClass)
			throws IllegalArgumentException {
		throw Graph.Exceptions.graphComputerNotSupported();
	}

	@Override
	public GraphComputer compute()
			throws IllegalArgumentException {
		throw Graph.Exceptions.graphComputerNotSupported();
	}

	@Override
	public void close() throws Exception {
		transaction.close();
		closeConnection();
		Iterator<AtomicReference<RepositoryConnection>> iter = connHolders.keySet().iterator();
		while (iter.hasNext()) {
			close(iter.next());
			iter.remove();
		}
		repo.shutDown();
	}

	@Override
	public String toString() {
		return StringFactory.graphString(this, repo.toString());
	}
}
