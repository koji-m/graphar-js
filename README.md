# graphar-js

A JavaScript library for [Apache GraphAr](https://graphar.apache.org/).

## Current Status

This project is an in-progress JavaScript port of the Apache GraphAr C++
implementation. The current focus is to understand the GraphAr specification
while keeping the implementation close to the upstream C++ behavior.

At this point, the library can:

- load GraphAr graph, vertex, and edge metadata from YAML files
- read vertex property chunks
- read edge topology chunks
- iterate edges for supported adjacency list layouts

## Current Example

The current browser demo reads a GraphAr graph info file over HTTP, then:

1. loads the graph metadata with `GraphInfo.load`
2. opens a vertex collection with `VerticesCollection.make`
3. opens an edge collection with `EdgesCollection.make`
4. prints a few sample vertices and edges

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
const vertexIterator = await vertices.getIterator();
for (const vertex of vertexIterator) {
  console.log(
    await vertex.property('id'),
    await vertex.property('firstName'),
    await vertex.property('lastName'),
  );
  break;
}

const edges = await EdgesCollection.make(
  graphInfo,
  'person',
  'knows',
  'person',
  AdjListType.ORDERED_BY_SOURCE,
);

const edgeIterator = await edges.getIterator();
for await (const edge of edgeIterator) {
  console.log(await edge.source(), await edge.destination());
  break;
}
```

For a runnable example, see [demo/main.js](./demo/main.js).

## Current Constraints

The implementation is not ready for npm publish yet. The main current
constraints are:

- only `http://` and `https://` graph locations are supported by the current
  `FileSystem` implementation
- payload reading is effectively Parquet-only right now
- the reader path is browser-oriented and depends on `parquet-wasm`
- the public API is still being stabilized while the port progresses
- the implementation is still being validated against the upstream C++ logic

## Local Demo

Install dependencies:

```bash
npm install
```

Start the demo:

```bash
npm run dev
```

The demo page lets you enter a GraphAr graph info URL and inspect a small sample
of the loaded graph.

## Peer Dependencies

The package currently expects these peer dependencies:

- `apache-arrow`
- `js-yaml`
- `parquet-wasm`
