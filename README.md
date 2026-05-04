# graphar-js

A JavaScript library for [Apache GraphAr](https://graphar.apache.org/).

## Current Status

This project is an in-progress JavaScript port of the Apache GraphAr C++
implementation. The current focus is to understand the GraphAr specification
while keeping the implementation close to the upstream C++ behavior.

At this point, the library can:

- load GraphAr graph, vertex, and edge metadata from YAML files
- read vertex property chunks
- read vertex label chunks
- read edge topology chunks
- expose high-level vertex access through `vertex.id()`, `vertex.property(...)`,
  `vertex.label()`, `vertex.hasLabel(...)`, and `VerticesCollection.find(...)`
- expose high-level vertex collection and iterator helpers through
  `VerticesCollection.size()`, `getIterator()`, `getEndIterator()`,
  `VertexIter.isEnd()`, `equals(...)`, `notEquals(...)`, `advance(...)`,
  `id()`, `property(...)`, `label()`, and `hasLabel(...)`
- expose high-level edge access through the canonical `EdgeIter` current-edge
  accessors `source()`, `destination()`, and `property(...)`, plus
  `EdgesCollection.findSrc(...)` and `EdgesCollection.findDst(...)`
- expose high-level edge collection and iterator helpers through
  `EdgesCollection.size()`, `getIterator()`, `getEndIterator()`,
  `EdgeIter.isEnd()`, `equals(...)`, `notEquals(...)`, and `advance()`
- support partial edge collections through
  `EdgesCollection.make(..., vertexChunkBegin, vertexChunkEnd)`
- validate high-level vertex and edge collection bounds, including vertex
  lookup ids, edge collection vertex chunk ranges, and edge search vertex ids
- apply projection/filter on property chunk readers
- iterate edges for the four GraphAr adjacency list layouts:
  `ordered_by_source`, `ordered_by_dest`, `unordered_by_source`, and
  `unordered_by_dest`

The current integration fixture lives under
[`test/fixtures/graphar-minimal`](./test/fixtures/graphar-minimal). It is a
small Parquet-backed GraphAr graph used by tests and by the current demo.

## Current Example

The current browser demo reads a GraphAr graph info file over HTTP, then:

1. loads the graph metadata with `GraphInfo.load`
2. opens a vertex collection with `VerticesCollection.make`
3. opens an edge collection with `EdgesCollection.make`
4. prints a few sample vertices and edges
5. shows high-level lookup by internal vertex id and edge search
6. shows reading a partial edge collection by vertex chunk range

```js
import {
  AdjListType,
  EdgesCollection,
  GraphInfo,
  VerticesCollection,
  initWasm,
} from 'graphar-js';

await initWasm();

const graphInfo = await GraphInfo.load({
  path: 'http://localhost:9000/my-bucket/parquet/ldbc_sample.graph.yml',
});

const vertices = await VerticesCollection.make(graphInfo, 'person');
console.log(vertices.size());

const vertexIterator = await vertices.getIterator();
for (const vertex of vertexIterator) {
  console.log(
    vertex.id(),
    await vertex.property('id'),
    await vertex.property('firstName'),
  );
  break;
}

const vertexBegin = await vertices.getIterator();
console.log(
  vertexBegin.id(),
  await vertexBegin.property('firstName'),
);
vertexBegin.advance();

const vertex = await vertices.find(3n);
console.log(
  vertex.id(),
  await vertex.property('id'),
  await vertex.label(),
);

const edges = await EdgesCollection.make(
  graphInfo,
  'person',
  'knows',
  'person',
  AdjListType.ORDERED_BY_SOURCE,
);
console.log(edges.size());

const edgeIterator = await edges.getIterator();
for await (const edgeIter of edgeIterator) {
  console.log(
    await edgeIter.source(),
    await edgeIter.destination(),
    await edgeIter.property('creationDate'),
  );
  break;
}

const begin = await edges.getIterator();
const found = await edges.findSrc(0n, begin);
if (!found.isEnd()) {
  console.log(await found.source(), await found.destination());
}

const secondSourceChunk = await EdgesCollection.make(
  graphInfo,
  'person',
  'knows',
  'person',
  AdjListType.ORDERED_BY_SOURCE,
  1n,
  2n,
);
const partialBegin = await secondSourceChunk.getIterator();
if (!partialBegin.isEnd()) {
  console.log(await partialBegin.source(), await partialBegin.destination());
}
```

For a runnable example, see [demo/main.js](./demo/main.js).

## Current Constraints

The implementation is not ready for npm publish yet. The main current
constraints are:

- Graph info and payload files must be read through `http://` or `https://`.
  Local filesystem paths and absolute paths such as `/tmp/graph/...` are not
  supported by the current `FileSystem` implementation.
- Payload reading is Parquet-only. Metadata may mention other file types, but
  the reader path currently always uses `parquet-wasm`.
- The reader path is browser-oriented and depends on `parquet-wasm`; Node-based
  checks need explicit WASM initialization.
- High-level vertex access is available through `vertex.id()`,
  `vertex.property(...)`, `vertex.label()`, `vertex.hasLabel(...)`, and
  `VerticesCollection.find(...)`.
- High-level vertex iterators support C++-style begin/end usage through
  `getIterator()`, `getEndIterator()`, `isEnd()`, `equals(...)`,
  `notEquals(...)`, and `advance(...)`. They also expose current vertex access
  through `id()`, `property(...)`, `label()`, and `hasLabel(...)`.
- `VerticesCollection.size()` returns a `BigInt`. Filtered vertex collections
  report the filtered size, while `VerticesCollection.find(...)` continues to
  use internal vertex ids and rejects ids outside `[0, vertexNum)`.
- Property projection/filter is implemented on vertex and edge property chunk
  readers. Vertex collection filtering is available through
  `VerticesCollection.verticesWithLabel(...)`,
  `verticesWithMultipleLabels(...)`, and `verticesWithProperty(...)`.
- High-level edge access is available through `EdgeIter.source()`,
  `EdgeIter.destination()`, `EdgeIter.property(...)`,
  `EdgesCollection.findSrc(...)`, and `EdgesCollection.findDst(...)`.
- High-level edge iterators support C++-style begin/end usage through
  `getIterator()`, `getEndIterator()`, `isEnd()`, `equals(...)`,
  `notEquals(...)`, and `advance()`. Reading `source()`, `destination()`, or
  `property(...)` from an end iterator is rejected with a high-level error.
- `EdgesCollection.size()` returns a `BigInt`. `findSrc(...)` and
  `findDst(...)` return the end iterator when the requested source or
  destination id is outside the corresponding vertex id range.
- Partial edge collections are available through
  `EdgesCollection.make(..., vertexChunkBegin, vertexChunkEnd)`, where the
  chunk range is half-open and searches stay inside that range. Invalid ranges
  are rejected; empty ranges such as `[1n, 1n)` are allowed and produce empty
  collections.
- Edge-iterator traversal helpers such as `firstSrc(...)`, `firstDst(...)`,
  `nextSrc(...)`, and `nextDst(...)` exist to support the collection search
  APIs, but they should still be treated as low-level, not-yet-stable helpers.
- Row filters are evaluated in JavaScript after Parquet decode because the
  current `parquet-wasm` path does not expose the C++ reader's filter pushdown
  API.
- The public API is still being stabilized while the port progresses and is
  still being validated against the upstream C++ logic.

## Local Demo

Install dependencies:

```bash
npm install
```

Start the demo:

```bash
npm run dev
```

The demo page lets you enter a GraphAr graph info URL and inspect a small
sample of the loaded graph. The default URL is:

```text
http://localhost:9000/my-bucket/parquet/ldbc_sample.graph.yml
```

To use the repository fixture with that URL, serve
`test/fixtures/graphar-minimal` at `http://localhost:9000/my-bucket/`.

## Peer Dependencies

The package currently expects these peer dependencies:

- `apache-arrow`
- `js-yaml`
- `parquet-wasm`
