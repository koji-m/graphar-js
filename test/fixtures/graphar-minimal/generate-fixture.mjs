#!/usr/bin/env node
import * as arrow from 'apache-arrow';
import { mkdir, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import initWasm, {
  Compression,
  Table,
  WriterPropertiesBuilder,
  writeParquet,
} from 'parquet-wasm/esm';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const outDir = path.join(__dirname, 'parquet');

const VERTEX_CHUNK_SIZE = 2;
const EDGE_CHUNK_SIZE = 2;
const SRC_CHUNK_SIZE = 2;
const DST_CHUNK_SIZE = 2;

const vertices = [
  { internalId: 0n, id: 100n, firstName: 'Ann' },
  { internalId: 1n, id: 101n, firstName: 'Bob' },
  { internalId: 2n, id: 102n, firstName: 'Cyd' },
  { internalId: 3n, id: 103n, firstName: 'Dan' },
  { internalId: 4n, id: 104n, firstName: 'Eve' },
];

const logicalEdges = [
  { src: 0n, dst: 1n, creationDate: '2020-01-01' },
  { src: 0n, dst: 2n, creationDate: '2020-01-02' },
  { src: 1n, dst: 3n, creationDate: '2020-01-03' },
  { src: 2n, dst: 0n, creationDate: '2020-01-04' },
  { src: 3n, dst: 4n, creationDate: '2020-01-05' },
  { src: 4n, dst: 0n, creationDate: '2020-01-06' },
];

const unorderedBySource = [
  logicalEdges[0],
  logicalEdges[2],
  logicalEdges[1],
  logicalEdges[4],
  logicalEdges[3],
  logicalEdges[5],
];

const unorderedByDest = [
  logicalEdges[5],
  logicalEdges[0],
  logicalEdges[3],
  logicalEdges[2],
  logicalEdges[1],
  logicalEdges[4],
];

function uint64le(value) {
  const buffer = new ArrayBuffer(8);
  new DataView(buffer).setBigUint64(0, BigInt(value), true);
  return new Uint8Array(buffer);
}

async function writeUint64(relativePath, value) {
  const target = path.join(outDir, relativePath);
  await mkdir(path.dirname(target), { recursive: true });
  await writeFile(target, uint64le(value));
}

function int64Vector(values) {
  return arrow.vectorFromArray(values.map((value) => BigInt(value)), new arrow.Int64());
}

function stringVector(values) {
  return arrow.vectorFromArray(values, new arrow.Utf8());
}

async function writeParquetTable(relativePath, columns) {
  const target = path.join(outDir, relativePath);
  await mkdir(path.dirname(target), { recursive: true });
  const table = arrow.tableFromArrays(columns);
  const wasmTable = Table.fromIPCStream(arrow.tableToIPC(table, 'stream'));
  const writerProperties = new WriterPropertiesBuilder()
    .setCompression(Compression.UNCOMPRESSED)
    .build();
  const parquetBytes = writeParquet(wasmTable, writerProperties);
  await writeFile(target, parquetBytes);
}

function chunks(rows, chunkSize) {
  const result = [];
  for (let i = 0; i < rows.length; i += chunkSize) {
    result.push(rows.slice(i, i + chunkSize));
  }
  return result;
}

function chunkVertices() {
  return chunks(vertices, VERTEX_CHUNK_SIZE);
}

function partIndex(value, chunkSize) {
  return Number(BigInt(value) / BigInt(chunkSize));
}

function partsBy(rows, key, chunkSize) {
  const partCount =
    Math.ceil(vertices.length / chunkSize);
  return Array.from({ length: partCount }, (_, part) =>
    rows.filter((row) => partIndex(row[key], chunkSize) === part),
  );
}

function sortByBigInt(rows, keys) {
  return [...rows].sort((a, b) => {
    for (const key of keys) {
      if (a[key] < b[key]) return -1;
      if (a[key] > b[key]) return 1;
    }
    return 0;
  });
}

function offsetsForPart(rows, alignedKey, part, chunkSize) {
  const firstVertex = BigInt(part * chunkSize);
  const localVertexCount = Math.min(chunkSize, vertices.length - part * chunkSize);
  const offsets = [0n];
  let edgeOffset = 0n;
  for (let i = 0; i < localVertexCount; i++) {
    const vertexId = firstVertex + BigInt(i);
    edgeOffset += BigInt(rows.filter((row) => row[alignedKey] === vertexId).length);
    offsets.push(edgeOffset);
  }
  return offsets;
}

async function writeMetadata() {
  await writeFile(
    path.join(outDir, 'ldbc_sample.graph.yml'),
    `name: ldbc_sample
prefix: ./
vertices:
  - person.vertex.yml
edges:
  - person_knows_person.edge.yml
version: gar/v1
`,
  );

  await writeFile(
    path.join(outDir, 'person.vertex.yml'),
    `type: person
chunk_size: ${VERTEX_CHUNK_SIZE}
prefix: vertex/person/
property_groups:
  - properties:
      - name: id
        data_type: int64
        is_primary: true
    file_type: parquet
  - properties:
      - name: firstName
        data_type: string
        is_primary: false
    file_type: parquet
version: gar/v1
`,
  );

  await writeFile(
    path.join(outDir, 'person_knows_person.edge.yml'),
    `src_type: person
edge_type: knows
dst_type: person
chunk_size: ${EDGE_CHUNK_SIZE}
src_chunk_size: ${SRC_CHUNK_SIZE}
dst_chunk_size: ${DST_CHUNK_SIZE}
directed: true
prefix: edge/person_knows_person/
adj_lists:
  - ordered: true
    aligned_by: src
    file_type: parquet
  - ordered: true
    aligned_by: dst
    file_type: parquet
  - ordered: false
    aligned_by: src
    file_type: parquet
  - ordered: false
    aligned_by: dst
    file_type: parquet
property_groups:
  - file_type: parquet
    properties:
      - name: creationDate
        data_type: string
        is_primary: false
version: gar/v1
`,
  );
}

async function writeVertexData() {
  await writeUint64('vertex/person/vertex_count', vertices.length);
  const vertexChunks = chunkVertices();
  for (const [chunkIndex, rows] of vertexChunks.entries()) {
    await writeParquetTable(`vertex/person/id/chunk${chunkIndex}`, {
      _graphArVertexIndex: int64Vector(rows.map((row) => row.internalId)),
      id: int64Vector(rows.map((row) => row.id)),
    });
    await writeParquetTable(`vertex/person/firstName/chunk${chunkIndex}`, {
      _graphArVertexIndex: int64Vector(rows.map((row) => row.internalId)),
      firstName: stringVector(rows.map((row) => row.firstName)),
    });
  }
}

async function writeAdjListData({
  adjListName,
  rows,
  alignedKey,
  alignedChunkSize,
  ordered,
}) {
  const parts = partsBy(rows, alignedKey, alignedChunkSize);
  await writeUint64(
    `edge/person_knows_person/${adjListName}/vertex_count`,
    vertices.length,
  );

  for (const [part, partRows] of parts.entries()) {
    await writeUint64(
      `edge/person_knows_person/${adjListName}/edge_count${part}`,
      partRows.length,
    );

    if (ordered) {
      await writeParquetTable(
        `edge/person_knows_person/${adjListName}/offset/chunk${part}`,
        {
          _graphArOffset: int64Vector(
            offsetsForPart(partRows, alignedKey, part, alignedChunkSize),
          ),
        },
      );
    }

    for (const [chunkIndex, chunkRows] of chunks(partRows, EDGE_CHUNK_SIZE).entries()) {
      await writeParquetTable(
        `edge/person_knows_person/${adjListName}/adj_list/part${part}/chunk${chunkIndex}`,
        {
          _graphArSrcIndex: int64Vector(chunkRows.map((row) => row.src)),
          _graphArDstIndex: int64Vector(chunkRows.map((row) => row.dst)),
        },
      );
      await writeParquetTable(
        `edge/person_knows_person/${adjListName}/creationDate/part${part}/chunk${chunkIndex}`,
        {
          creationDate: stringVector(chunkRows.map((row) => row.creationDate)),
        },
      );
    }
  }
}

async function main() {
  await initWasm();
  await rm(outDir, { recursive: true, force: true });
  await mkdir(outDir, { recursive: true });
  await writeMetadata();
  await writeVertexData();
  await writeAdjListData({
    adjListName: 'ordered_by_source',
    rows: partsBy(sortByBigInt(logicalEdges, ['src', 'dst']), 'src', SRC_CHUNK_SIZE).flat(),
    alignedKey: 'src',
    alignedChunkSize: SRC_CHUNK_SIZE,
    ordered: true,
  });
  await writeAdjListData({
    adjListName: 'ordered_by_dest',
    rows: partsBy(sortByBigInt(logicalEdges, ['dst', 'src']), 'dst', DST_CHUNK_SIZE).flat(),
    alignedKey: 'dst',
    alignedChunkSize: DST_CHUNK_SIZE,
    ordered: true,
  });
  await writeAdjListData({
    adjListName: 'unordered_by_source',
    rows: unorderedBySource,
    alignedKey: 'src',
    alignedChunkSize: SRC_CHUNK_SIZE,
    ordered: false,
  });
  await writeAdjListData({
    adjListName: 'unordered_by_dest',
    rows: unorderedByDest,
    alignedKey: 'dst',
    alignedChunkSize: DST_CHUNK_SIZE,
    ordered: false,
  });
}

await main();
