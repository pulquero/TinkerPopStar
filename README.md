# TinkerPopStar

![CI](https://github.com/pulquero/tinkerpopstar/actions/workflows/ci.yml/badge.svg)
[![Coverage](https://codecov.io/github/pulquero/tinkerpopstar/coverage.svg?branch=main)](https://codecov.io/gh/pulquero/tinkerpopstar/)

**TinkerPop over SPARQL-star**

Do you want Gremlins in your RDF store? Then you have come to the right place!

This is an implementation of [Gremlin/TinkerPop](https://tinkerpop.apache.org/) over SPARQL-star. You can do two things with it:

##### 1) I have no interest in RDF, I happen to have a triple store and just want to use it as a storage back-end.
  
If your triple store supports SPARQL-star then you can just point `SparqlStarGraph` at it, and Gremlin away as normal.
We support the majority of graph features so maybe we can do something that your current implementation can't (or do it better).

##### 2) I ❤️ RDF, but I want to take advantage of some of the TinkerPop stack.

Most graph analytic tools assume a standard definition of a graph, but RDF isn't (it is a directed 3-uniform hypergraph,
and that is before we throw in named graphs and RDF-star).
TinkerPopStar lets you expose a slice of your RDF data as a standard graph and use Gremlin to process it.
Real-world data often does not have the structure of a standard graph, so RDF can provide a good fit.
At the other end, analytics need a standard graph. TinkerPopStar can provide the bridge.

#### TinkerPopStar RDF schema

Essentially:

An edge `(a)-[e]-(b)` maps to the RDF triple `a e b.`

A vertex property `a.prop = value` maps to the RDF triple `a prop value.`

An edge property `e.prop = value` maps to the RDF* triple `<<a e b>> prop value.`

A vertex meta-property `(a.prop = value).metaprop = metavalue` maps to the RDF* triple `<<a prop value>> metaprop metavalue.`

These are accompanied by some RDFS statements to make the data understandable by RDF tools and allow mixed data - other triples can freely co-exist with the TinkerPopStar triples.

**Exceptions:**

Multiedges (enabled by default) `(a)-[e]-(b)` map to the RDF triples `a e uuid. uuid rdfs:seeAlso b.`

List vertex properties `a.prop = value` maps to the RDF triples `a prop uuid. uuid rdf:value value.`
