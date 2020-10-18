package io.github.pulquero.tinkerpopstar;

import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import org.eclipse.rdf4j.repository.RepositoryConnection;

class SparqlStarTransaction extends AbstractThreadLocalTransaction {
	private final SparqlStarGraph g;

	SparqlStarTransaction(SparqlStarGraph g) {
		super(g);
		this.g = g;
	}

	@Override
	public boolean isOpen() {
		RepositoryConnection conn = g.getExistingConnection();
		return (conn != null) && conn.isActive();
	}

	@Override
	protected void doOpen() {
		g.ensureConnection().begin();
	}

	@Override
	protected void doRollback() throws TransactionException {
		RepositoryConnection conn = g.getExistingConnection();
		if (conn == null) {
			throw Transaction.Exceptions.transactionMustBeOpenToReadWrite();
		}
		conn.rollback();
	}
	
	@Override
	protected void doCommit() throws TransactionException {
		RepositoryConnection conn = g.getExistingConnection();
		if (conn == null) {
			throw Transaction.Exceptions.transactionMustBeOpenToReadWrite();
		}
		conn.commit();
	}
}
