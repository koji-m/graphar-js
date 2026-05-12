import * as arrow from 'apache-arrow';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { EdgeInfo, GraphInfo, VertexInfo } from '../src/core/graph-info.js';
import {
  EdgesCollection,
  PropertyList,
  VerticesCollection,
} from '../src/core/graph-reader.js';
import { AdjListType } from '../src/core/types.js';

const countsByPath = new Map();
const tablesByPath = new Map();

const fs = {
  readFileAsSingleUint64: vi.fn(async (path) => {
    if (!countsByPath.has(path)) {
      throw new Error(`Unexpected count path: ${path}`);
    }
    return countsByPath.get(path);
  }),
  readFileAsTable: vi.fn(async (path) => {
    if (!tablesByPath.has(path)) {
      throw new Error(`Unexpected table path: ${path}`);
    }
    return tablesByPath.get(path);
  }),
};

vi.mock('../src/core/filesystem.js', () => ({
  fileSystemFromUriOrPath: vi.fn((path) => [fs, path]),
}));

function makeVertexInfo() {
  return VertexInfo.load({
    type: 'person',
    chunk_size: 1,
    prefix: 'vertex/person/',
    property_groups: [
      {
        prefix: 'props/',
        file_type: 'parquet',
        properties: [
          {
            name: 'id',
            data_type: 'int64',
            is_primary: true,
          },
          {
            name: 'nickname',
            data_type: 'string',
            is_nullable: true,
          },
        ],
      },
    ],
    version: 'gar/v1',
  });
}

function makeEdgeInfo() {
  return EdgeInfo.load({
    src_type: 'person',
    edge_type: 'knows',
    dst_type: 'person',
    chunk_size: 1,
    src_chunk_size: 1,
    dst_chunk_size: 1,
    directed: true,
    prefix: 'edge/person_knows_person/',
    adj_lists: [{ ordered: false, aligned_by: 'src', file_type: 'parquet' }],
    property_groups: [
      {
        prefix: 'props/',
        file_type: 'parquet',
        properties: [
          {
            name: 'creationDate',
            data_type: 'string',
            is_nullable: false,
          },
          {
            name: 'weight',
            data_type: 'int64',
            is_nullable: true,
          },
        ],
      },
    ],
    version: 'gar/v1',
  });
}

function makeListVertexInfo() {
  return VertexInfo.load({
    type: 'person',
    chunk_size: 1,
    prefix: 'vertex/person/',
    property_groups: [
      {
        prefix: 'props/',
        file_type: 'parquet',
        properties: [
          {
            name: 'feature',
            data_type: 'list<float>',
            is_primary: false,
          },
        ],
      },
    ],
    version: 'gar/v1',
  });
}

function seedGraphTables(basePrefix) {
  countsByPath.set(`${basePrefix}vertex/person/vertex_count`, 1n);
  countsByPath.set(
    `${basePrefix}edge/person_knows_person/unordered_by_source/vertex_count`,
    1n,
  );
  countsByPath.set(
    `${basePrefix}edge/person_knows_person/unordered_by_source/edge_count0`,
    1n,
  );

  tablesByPath.set(
    `${basePrefix}vertex/person/props/chunk0`,
    arrow.tableFromArrays({
      _graphArVertexIndex: [0n],
      id: [100n],
      nickname: [null],
    }),
  );
  tablesByPath.set(
    `${basePrefix}edge/person_knows_person/unordered_by_source/adj_list/part0/chunk0`,
    arrow.tableFromArrays({
      src: [0n],
      dst: [1n],
    }),
  );
  tablesByPath.set(
    `${basePrefix}edge/person_knows_person/unordered_by_source/props/part0/chunk0`,
    arrow.tableFromArrays({
      creationDate: ['2020-01-01'],
      weight: [null],
    }),
  );
}

function seedListVertexTables(basePrefix) {
  countsByPath.set(`${basePrefix}vertex/person/vertex_count`, 1n);
  tablesByPath.set(
    `${basePrefix}vertex/person/props/chunk0`,
    arrow.tableFromArrays({
      _graphArVertexIndex: [0n],
      feature: [[1.5, 2.5, 3.5]],
    }),
  );
}

describe('graph-reader isValid parity', () => {
  beforeEach(() => {
    countsByPath.clear();
    tablesByPath.clear();
    fs.readFileAsSingleUint64.mockClear();
    fs.readFileAsTable.mockClear();
  });

  it('checks vertex property nullability on vertex objects and iterators', async () => {
    const prefix = 'http://example.test/graphs/';
    seedGraphTables(prefix);
    const vertexInfo = makeVertexInfo();
    const graphInfo = new GraphInfo('g', [vertexInfo], [], [], prefix);

    const vertices = await VerticesCollection.make(graphInfo, 'person');
    const vertex = await vertices.find(0n);
    const iterator = await vertices.getIterator();
    const end = await vertices.getEndIterator();

    await expect(vertex.isValid('id')).resolves.toBe(true);
    await expect(vertex.isValid('nickname')).resolves.toBe(false);
    await expect(vertex.property('nickname')).rejects.toThrow(
      /The value of the nickname is null/,
    );
    await expect(vertex.isValid('missing')).rejects.toThrow(
      /Vertex property missing not found in vertex info/,
    );

    await expect(iterator.isValid('id')).resolves.toBe(true);
    await expect(iterator.isValid('nickname')).resolves.toBe(false);
    await expect(iterator.property('nickname')).rejects.toThrow(
      /The value of the nickname is null/,
    );
    await expect(end.isValid('id')).rejects.toThrow(/Vertex iterator is at end/);
  });

  it('checks edge property nullability on edge iterators', async () => {
    const prefix = 'http://example.test/graphs/';
    seedGraphTables(prefix);
    const vertexInfo = makeVertexInfo();
    const edgeInfo = makeEdgeInfo();
    const graphInfo = new GraphInfo('g', [vertexInfo], [edgeInfo], [], prefix);

    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.UNORDERED_BY_SOURCE,
    );
    const iterator = await edges.getIterator();
    const end = await edges.getEndIterator();

    await expect(iterator.isValid('creationDate')).resolves.toBe(true);
    await expect(iterator.isValid('weight')).resolves.toBe(false);
    await expect(iterator.property('weight')).rejects.toThrow(
      /The value of the weight is null/,
    );
    await expect(iterator.isValid('missing')).rejects.toThrow(
      /Edge property missing not found in edge info/,
    );
    await expect(end.isValid('creationDate')).rejects.toThrow(
      /Edge iterator is at end/,
    );
  });

  it('wraps list properties in a dedicated PropertyList value', async () => {
    const prefix = 'http://example.test/graphs/';
    seedListVertexTables(prefix);
    const vertexInfo = makeListVertexInfo();
    const graphInfo = new GraphInfo('g', [vertexInfo], [], [], prefix);

    const vertices = await VerticesCollection.make(graphInfo, 'person');
    const vertex = await vertices.find(0n);
    const iterator = await vertices.getIterator();

    const vertexValue = await vertex.property('feature');
    const iteratorValue = await iterator.property('feature');

    expect(vertexValue).toBeInstanceOf(PropertyList);
    expect(vertexValue.length).toBe(3);
    expect(vertexValue.size()).toBe(3);
    expect(vertexValue.at(0)).toBe(1.5);
    expect(vertexValue.toArray()).toEqual([1.5, 2.5, 3.5]);
    expect([...vertexValue]).toEqual([1.5, 2.5, 3.5]);

    expect(iteratorValue).toBeInstanceOf(PropertyList);
    expect(iteratorValue.toArray()).toEqual([1.5, 2.5, 3.5]);
    await expect(vertex.isValid('feature')).resolves.toBe(true);
    await expect(iterator.isValid('feature')).resolves.toBe(true);
  });
});
