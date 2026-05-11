# graphar-js

A Node-first JavaScript reader library for
[Apache GraphAr](https://graphar.apache.org/).

## Runtime Support

- official first-milestone support: Node.js
- experimental: browser usage

The first npm milestone is focused on a stable Node.js reader package. Browser
usage remains available for demos and smoke tests, but is not part of the
first stable support contract.

## Current Status

This project is an in-progress JavaScript port of the Apache GraphAr C++
implementation. The current focus is to understand the GraphAr specification
while keeping the implementation close to the upstream C++ behavior.

At this point, the library can:

- load GraphAr graph, vertex, and edge metadata from YAML files
- validate GraphAr metadata for `file_type` and GraphInfo structural
  constraints during load, following the current upstream C++ rules where this
  port already implements them
- default omitted metadata prefixes following the current upstream C++ rules:
  property-group prefixes default to joined property names, adjacency-list
  prefixes default to the adjacency-list type name, vertex prefixes default to
  `<vertex-type>/`, and edge prefixes default to
  `<src>_<edge>_<dst>/`
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
- expose C++-style expression builders for property filtering through
  `_Property`, `_Literal`, `_Equal`, `_NotEqual`, `_GreaterThan`,
  `_GreaterEqual`, `_LessThan`, `_LessEqual`, `_And`, and `_Or`
- iterate edges for the four GraphAr adjacency list layouts:
  `ordered_by_source`, `ordered_by_dest`, `unordered_by_source`, and
  `unordered_by_dest`

The current integration fixture lives under
[`test/fixtures/graphar-minimal`](./test/fixtures/graphar-minimal). It is a
small Parquet-backed GraphAr graph used by tests and by the current demo.

## Reader v0 Public Surface

The current plan for the first npm milestone is a reader-only package with the
following public API surface.

- runtime initialization: `initWasm(...)`
- metadata loading and inspection: `GraphInfo`, `VertexInfo`, `EdgeInfo`,
  `PropertyGroup`, `AdjacentList`
- graph metadata enums: `AdjListType`, `FileType`
- high-level vertex reader surface:
  `VerticesCollection.make(...)`,
  `VerticesCollection.verticesWithLabel(...)`,
  `VerticesCollection.verticesWithMultipleLabels(...)`,
  `VerticesCollection.verticesWithProperty(...)`,
  `VerticesCollection.size()`,
  `VerticesCollection.find(...)`,
  `VerticesCollection.getIterator()`, and
  `VerticesCollection.getEndIterator()`
- high-level vertex iterator surface:
  `VertexIter.isEnd()`, `equals(...)`, `notEquals(...)`, `advance(...)`,
  `id()`, `property(...)`, `isValid(...)`, `label()`, and `hasLabel(...)`
- high-level edge reader surface:
  `EdgesCollection.make(...)`,
  `EdgesCollection.size()`,
  `EdgesCollection.getIterator()`,
  `EdgesCollection.getEndIterator()`,
  `EdgesCollection.findSrc(...)`, and `EdgesCollection.findDst(...)`
- high-level edge iterator surface:
  `EdgeIter.isEnd()`, `equals(...)`, `notEquals(...)`, `advance()`,
  `source()`, `destination()`, `property(...)`, and `isValid(...)`
- property-expression helpers used by
  `VerticesCollection.verticesWithProperty(...)`:
  `_Property`, `_Literal`, `_Equal`, `_NotEqual`, `_GreaterThan`,
  `_GreaterEqual`, `_LessThan`, `_LessEqual`, `_And`, `_Or`, and `_Not`

For upstream `Vertex::IsValid` / `Edge::IsValid` parity, this JS port exposes
`vertex.isValid(...)`, `VertexIter.isValid(...)`, and `EdgeIter.isValid(...)`.

## Experimental And Internal

The following APIs exist today but are not part of the intended reader v0
stable package contract.

- browser usage is still experimental even though the repository includes a
  browser demo and a browser smoke test
- edge-iterator traversal helpers `firstSrc(...)`, `firstDst(...)`,
  `nextSrc(...)`, and `nextDst(...)` are currently exposed on `EdgeIter`
  instances to support collection search behavior, but should be treated as
  unstable helper APIs
- compatibility support for the older plain-object filter shape in
  `VerticesCollection.verticesWithProperty(...)` remains available during the
  port, but the intended stable filtering surface is the C++-mirrored
  expression-helper API
- low-level chunk readers, reader utilities, filesystem helpers, HTTP helpers,
  and general utility modules under `src/core/` are internal implementation
  details rather than package-root public APIs
- in particular, `fileSystemFromUriOrPath(...)` is an internal filesystem
  helper that mirrors the role of the upstream C++ utility, but is not part of
  the intended JS reader public API

## Node Example

The current Node-first usage pattern is:

1. initializes the Parquet WASM runtime
2. loads the graph metadata with `GraphInfo.load`
3. opens a vertex collection with `VerticesCollection.make`
4. opens an edge collection with `EdgesCollection.make`
5. performs high-level lookup and iteration

```js
import { readFile } from 'node:fs/promises';
import {
  AdjListType,
  EdgesCollection,
  GraphInfo,
  VerticesCollection,
  initWasm,
} from 'graphar-js';

await initWasm({
  module_or_path: await readFile(
    new URL('./node_modules/parquet-wasm/esm/parquet_wasm_bg.wasm', import.meta.url),
  ),
});

const graphInfo = await GraphInfo.load({
  path: '/absolute/path/to/graph/parquet/ldbc_sample.graph.yml',
});

const vertices = await VerticesCollection.make(graphInfo, 'person');
console.log(vertices.size());

const vertex = await vertices.find(3n);
console.log(
  vertex.id(),
  await vertex.property('id'),
  await vertex.property('firstName'),
);

const edges = await EdgesCollection.make(
  graphInfo,
  'person',
  'knows',
  'person',
  AdjListType.ORDERED_BY_SOURCE,
);
const found = await edges.findSrc(0n, await edges.getIterator());
if (!found.isEnd()) {
  console.log(
    await found.source(),
    await found.destination(),
    await found.property('creationDate'),
  );
}
```

## Experimental Browser Example

The repository still includes a browser demo and an experimental browser smoke
test. Browser usage currently assumes HTTP-accessible GraphAr assets and is not
part of the first stable release contract.

For a runnable browser demo, see [demo/main.js](./demo/main.js).

## Current Constraints

The implementation is not ready for npm publish yet. The main current
constraints are:

- In Node.js, graph info and payload files may also be read from `file://...`
  URIs and absolute local filesystem paths such as `/tmp/graph/...`.
- In browsers, graph info and payload files must be read through `http://` or
  `https://`, but browser usage is still experimental rather than part of the
  first stable support contract.
- Relative local filesystem paths such as `./graph/...` are not supported.
  This matches the current C++ `FileSystemFromUriOrPath` constraint.
- For local loading, graph-level metadata prefixes must also resolve to an
  absolute local path or `file://...`. A graph YAML with `prefix: ./` may work
  over HTTP but is expected to fail on the local reader path, matching the
  current upstream C++ behavior.
- Nested vertex and edge metadata may omit their own `prefix` fields. In that
  case the loader applies the same defaulting rules as the upstream C++
  implementation before validation and before building payload paths.
- For graph-level metadata loaded over `http://` or `https://`, this JS port
  intentionally resolves relative graph `prefix` values such as `./` against
  the graph info URL. This is a current JS-vs-C++ parity difference that is
  kept for the Node-first/HTTP reader path.
- GraphInfo metadata validation currently rejects unsupported storage
  `file_type` values in `property_groups` and `adj_lists`, plus the current
  C++-mirrored CSV restrictions such as list payload types and non-single
  cardinality in CSV property groups.
- Payload reading is Parquet-only. The reader path now checks `file_type`
  explicitly and rejects `csv` / `orc` / `json` payloads with an unsupported
  error instead of silently treating them as Parquet.
- Node.js usage requires explicit `initWasm(...)` before reading Parquet-backed
  payloads.
- Browser usage also depends on `parquet-wasm`, but the browser initialization
  and dependency-resolution story is still considered experimental.
- The intended package-root filesystem surface is `initWasm(...)` only.
  Filesystem resolution remains an internal reader detail; callers are expected
  to pass supported graph info paths into higher-level APIs such as
  `GraphInfo.load(...)`.
- High-level vertex access is available through `vertex.id()`,
  `vertex.property(...)`, `vertex.isValid(...)`, `vertex.label()`,
  `vertex.hasLabel(...)`, and `VerticesCollection.find(...)`.
- High-level vertex iterators support C++-style begin/end usage through
  `getIterator()`, `getEndIterator()`, `isEnd()`, `equals(...)`,
  `notEquals(...)`, and `advance(...)`. They also expose current vertex access
  through `id()`, `property(...)`, `isValid(...)`, `label()`, and
  `hasLabel(...)`.
- `VerticesCollection.size()` returns a `BigInt`. Filtered vertex collections
  report the filtered size, while `VerticesCollection.find(...)` continues to
  use internal vertex ids and rejects ids outside `[0, vertexNum)`.
- Property projection/filter is implemented on vertex and edge property chunk
  readers. Vertex collection filtering is available through
  `VerticesCollection.verticesWithLabel(...)`,
  `verticesWithMultipleLabels(...)`, and `verticesWithProperty(...)`.
- The current filtering surface is intended to mirror the upstream C++
  high-level reader APIs as follows:
  `VerticesCollection::verticesWithLabel(...)` ->
  `VerticesCollection.verticesWithLabel(...)`
  `VerticesCollection::verticesWithMultipleLabels(...)` ->
  `VerticesCollection.verticesWithMultipleLabels(...)`
  `VerticesCollection::verticesWithProperty(...)` ->
  `VerticesCollection.verticesWithProperty(...)`
- The current property-expression helper surface also mirrors the upstream C++
  helper names:
  `graphar::_Property(...)` -> `_Property(...)`
  `graphar::_Literal(...)` -> `_Literal(...)`
  `graphar::_Equal(...)` -> `_Equal(...)`
  `graphar::_NotEqual(...)` -> `_NotEqual(...)`
  `graphar::_GreaterThan(...)` -> `_GreaterThan(...)`
  `graphar::_GreaterEqual(...)` -> `_GreaterEqual(...)`
  `graphar::_LessThan(...)` -> `_LessThan(...)`
  `graphar::_LessEqual(...)` -> `_LessEqual(...)`
  `graphar::_And(...)` -> `_And(...)`
  `graphar::_Or(...)` -> `_Or(...)`
  `graphar::_Not(...)` -> `_Not(...)`
- `VerticesCollection.verticesWithProperty(...)` accepts those C++-style
  expression helper trees and still accepts the older plain-object filter form
  for compatibility during the port.
- The JS expression helpers intentionally use plain-object expression trees
  instead of porting the C++ `Expression` class hierarchy 1:1. This is a
  deliberate JS-specific adaptation at the representation layer; the goal is
  to keep the public filtering semantics aligned with the upstream C++ reader.
- Current filtering coverage is vertex-focused. The helper expressions are used
  by `VerticesCollection.verticesWithProperty(...)`; there is not yet a
  separate public JS API matching the C++ low-level reader `Filter(...)`
  methods directly.
- High-level edge access is available through `EdgeIter.source()`,
  `EdgeIter.destination()`, `EdgeIter.property(...)`, `EdgeIter.isValid(...)`,
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
- `fileSystemFromUriOrPath(...)` is no longer considered part of the intended
  package-root public API even though the implementation continues to use it
  internally.
- Row filters are evaluated in JavaScript after Parquet decode because the
  current `parquet-wasm` path does not expose the C++ reader's filter pushdown
  API.
- The public API is still being stabilized while the port progresses and is
  still being validated against the upstream C++ logic.

## Test Gates

The current Node-first release gate is:

```bash
npm run test:release-node
```

The browser smoke test remains available, but is experimental and should not be
treated as part of the first stable support contract:

```bash
npm run test:smoke-browser:experimental
```

## Local Demo

Install dependencies:

```bash
npm install
```

Start the demo:

```bash
npm run dev
```

The experimental browser demo page lets you enter a GraphAr graph info URL and
inspect a small sample of the loaded graph. The default URL is:

```text
http://localhost:9000/my-bucket/parquet/ldbc_sample.graph.yml
```

To use the repository fixture with that URL, serve
`test/fixtures/graphar-minimal` at `http://localhost:9000/my-bucket/`.
