package io.github.pulquero.tinkerpopstar;

import org.apache.tinkerpop.gremlin.structure.Graph.Features;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.eclipse.rdf4j.model.Triple;

public class SparqlStarFeatures implements Features {
    @Override
	public GraphFeatures graph() {
        return new GraphFeatures() {
            @Override public boolean supportsComputer() { return false; }
            @Override public boolean supportsThreadedTransactions() { return false; }
            @Override
            public VariableFeatures variables() {
            	return new VariableFeatures() {
            	};
            }
        };
    }

    @Override
	public VertexFeatures vertex() {
        return new VertexFeatures() {
    		@Override public boolean supportsNumericIds() { return false; }
    		@Override public boolean supportsAnyIds() { return false; }
        	@Override public boolean supportsNullPropertyValues() { return false; }
            @Override public boolean supportsDuplicateMultiProperties() { return false; }
            @Override public boolean supportsMetaProperties() { return false; }
            @Override public VertexProperty.Cardinality getCardinality(String key) { return VertexProperty.Cardinality.set; }
            @Override
        	public boolean willAllowId(Object o) {
        		return true;
        	}
            @Override
            public VertexPropertyFeatures properties() {
            	return new VertexPropertyFeatures() {
            		@Override public boolean supportsAnyIds() { return false; }
            		@Override public boolean supportsUserSuppliedIds() { return false; }
            	};
            }
        };
    }

    @Override
	public EdgeFeatures edge() {
        return new EdgeFeatures() {
        	@Override public boolean supportsUserSuppliedIds() { return false; }
            @Override public boolean supportsNumericIds() { return false; }
            @Override public boolean supportsUuidIds() { return false; }
            @Override public boolean supportsAnyIds() { return false; }
        	@Override public boolean supportsNullPropertyValues() { return false; }
            @Override
        	public boolean willAllowId(Object o) {
        		return (o instanceof Triple);
        	}
            @Override
            public EdgePropertyFeatures properties() {
            	return new EdgePropertyFeatures() {
            	};
            }
        };
    }

    @Override
    public String toString() {
    	return StringFactory.featureString(this);
    }
}
