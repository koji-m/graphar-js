# Minimal GraphAr Fixture

This fixture is a deliberately small GraphAr graph for integration tests and
specification study. It follows the GraphAr file layout described in
`_ref/incubator-graphar/docs/specification/format.md` and the path conventions
used by the official C++ implementation.

The generated data lives under `parquet/`. Recreate it with:

```sh
node test/fixtures/graphar-minimal/generate-fixture.mjs
```

## Logical Graph

Graph name: `ldbc_sample`

Vertex type: `person`

| internal id | id  | firstName | labels |
| ----------- | --- | --------- | ------ |
| 0           | 100 | Ann       | `active;engineer` |
| 1           | 101 | Bob       | `active` |
| 2           | 102 | Cyd       | `contractor` |
| 3           | 103 | Dan       | `active;contractor` |
| 4           | 104 | Eve       | `` |

Edge triplet: `person` - `knows` - `person`

| logical edge | source | destination | creationDate |
| ------------ | ------ | ----------- | ------------ |
| 0            | 0      | 1           | 2020-01-01   |
| 1            | 0      | 2           | 2020-01-02   |
| 2            | 1      | 3           | 2020-01-03   |
| 3            | 2      | 0           | 2020-01-04   |
| 4            | 3      | 4           | 2020-01-05   |
| 5            | 4      | 0           | 2020-01-06   |

## Chunk Parameters

| parameter | value | purpose |
| --------- | ----- | ------- |
| vertex chunk size | 2 | Creates three vertex chunks, including a final partial chunk. |
| edge chunk size | 2 | Creates multiple edge chunks inside a vertex-chunk partition. |
| source chunk size | 2 | Partitions source-aligned edge layouts by source vertex id. |
| destination chunk size | 2 | Partitions destination-aligned edge layouts by destination vertex id. |

## Metadata Files

| file | meaning |
| ---- | ------- |
| `parquet/ldbc_sample.graph.yml` | Graph-level metadata. It points to the vertex and edge info files. |
| `parquet/person.vertex.yml` | Metadata for `person` vertices, including chunk size and property groups. |
| `parquet/person_knows_person.edge.yml` | Metadata for `person knows person` edges, including adjacency list layouts and edge properties. |

## Vertex Files

`vertex/person/vertex_count` stores the vertex count as one little-endian
uint64 value. For this fixture the value is `5`.

Vertex property chunks are split by internal vertex id:

| file | rows | meaning |
| ---- | ---- | ------- |
| `vertex/person/id/chunk0` | vertex 0..1 | `_graphArVertexIndex`, `id` |
| `vertex/person/id/chunk1` | vertex 2..3 | `_graphArVertexIndex`, `id` |
| `vertex/person/id/chunk2` | vertex 4 | `_graphArVertexIndex`, `id` |
| `vertex/person/firstName/chunk0` | vertex 0..1 | `_graphArVertexIndex`, `firstName` |
| `vertex/person/firstName/chunk1` | vertex 2..3 | `_graphArVertexIndex`, `firstName` |
| `vertex/person/firstName/chunk2` | vertex 4 | `_graphArVertexIndex`, `firstName` |
| `vertex/person/labels/chunk0` | vertex 0..1 | `active`, `engineer`, `contractor` |
| `vertex/person/labels/chunk1` | vertex 2..3 | `active`, `engineer`, `contractor` |
| `vertex/person/labels/chunk2` | vertex 4 | `active`, `engineer`, `contractor` |

The `_graphArVertexIndex` column is included because GraphAr stores the internal
vertex id in vertex payload files.

Label chunks follow the GraphAr C++ writer layout: each possible label from the
vertex metadata becomes one boolean column, and each row marks whether that
vertex carries the label.

## ordered_by_source

This layout is source-aligned and source-ordered. It is the CSR-like form.

`edge/person_knows_person/ordered_by_source/vertex_count` is `5`.

| part | source ids | ordered edges | edge count | offset file values |
| ---- | ---------- | ------------- | ---------- | ------------------ |
| part0 | 0, 1 | `0->1`, `0->2`, `1->3` | 3 | `offset/chunk0 = [0, 2, 3]` |
| part1 | 2, 3 | `2->0`, `3->4` | 2 | `offset/chunk1 = [0, 1, 2]` |
| part2 | 4 | `4->0` | 1 | `offset/chunk2 = [0, 1]` |

Adjacency list files:

| file | rows |
| ---- | ---- |
| `ordered_by_source/adj_list/part0/chunk0` | `0->1`, `0->2` |
| `ordered_by_source/adj_list/part0/chunk1` | `1->3` |
| `ordered_by_source/adj_list/part1/chunk0` | `2->0`, `3->4` |
| `ordered_by_source/adj_list/part2/chunk0` | `4->0` |

Edge property files under `ordered_by_source/creationDate/` have the same row
order and chunking as the adjacency list files.

## ordered_by_dest

This layout is destination-aligned and destination-ordered. It is the CSC-like
form.

`edge/person_knows_person/ordered_by_dest/vertex_count` is `5`.

| part | destination ids | ordered edges | edge count | offset file values |
| ---- | --------------- | ------------- | ---------- | ------------------ |
| part0 | 0, 1 | `2->0`, `4->0`, `0->1` | 3 | `offset/chunk0 = [0, 2, 3]` |
| part1 | 2, 3 | `0->2`, `1->3` | 2 | `offset/chunk1 = [0, 1, 2]` |
| part2 | 4 | `3->4` | 1 | `offset/chunk2 = [0, 1]` |

Adjacency list files:

| file | rows |
| ---- | ---- |
| `ordered_by_dest/adj_list/part0/chunk0` | `2->0`, `4->0` |
| `ordered_by_dest/adj_list/part0/chunk1` | `0->1` |
| `ordered_by_dest/adj_list/part1/chunk0` | `0->2`, `1->3` |
| `ordered_by_dest/adj_list/part2/chunk0` | `3->4` |

Edge property files under `ordered_by_dest/creationDate/` have the same row
order and chunking as the adjacency list files.

## unordered_by_source

This layout is source-aligned but not ordered. It is the COO-like source
partitioned form. The fixture keeps a stable row order so tests can assert
exact values, but there is no offset table.

`edge/person_knows_person/unordered_by_source/vertex_count` is `5`.

| part | source ids | rows | edge count |
| ---- | ---------- | ---- | ---------- |
| part0 | 0, 1 | `0->1`, `1->3`, `0->2` | 3 |
| part1 | 2, 3 | `3->4`, `2->0` | 2 |
| part2 | 4 | `4->0` | 1 |

Edge property files under `unordered_by_source/creationDate/` have the same row
order and chunking as the adjacency list files.

## unordered_by_dest

This layout is destination-aligned but not ordered. It is the COO-like
destination partitioned form. The fixture keeps a stable row order so tests can
assert exact values, but there is no offset table.

`edge/person_knows_person/unordered_by_dest/vertex_count` is `5`.

| part | destination ids | rows | edge count |
| ---- | --------------- | ---- | ---------- |
| part0 | 0, 1 | `4->0`, `0->1`, `2->0` | 3 |
| part1 | 2, 3 | `1->3`, `0->2` | 2 |
| part2 | 4 | `3->4` | 1 |

Edge property files under `unordered_by_dest/creationDate/` have the same row
order and chunking as the adjacency list files.

## File Meaning Summary

| pattern | meaning |
| ------- | ------- |
| `vertex/<label>/vertex_count` | Number of vertices for the vertex label, encoded as little-endian uint64. |
| `vertex/<label>/<property-group>/chunkN` | Parquet payload for one vertex property group and one vertex chunk. |
| `vertex/<label>/labels/chunkN` | Parquet label payload with one boolean column per declared vertex label. |
| `edge/<src>_<edge>_<dst>/<adj-list>/vertex_count` | Number of vertices in the vertex space used by that adjacency list. |
| `edge/<src>_<edge>_<dst>/<adj-list>/edge_countN` | Number of edges in vertex chunk partition `N`, encoded as little-endian uint64. |
| `edge/<src>_<edge>_<dst>/<adj-list>/adj_list/partN/chunkM` | Parquet topology chunk with source and destination internal ids. |
| `edge/<src>_<edge>_<dst>/<adj-list>/offset/chunkN` | Parquet offset table for ordered adjacency lists. The first value is always `0` for the partition. |
| `edge/<src>_<edge>_<dst>/<adj-list>/<property-group>/partN/chunkM` | Parquet edge property chunk aligned row-for-row with the adjacency list chunk. |
