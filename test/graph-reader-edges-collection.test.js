import { beforeEach, describe, expect, it, vi } from 'vitest';
import { EdgeInfo, GraphInfo } from '../src/core/graph-info.js';
import { EdgesCollection } from '../src/core/graph-reader.js';
import { AdjListType } from '../src/core/types.js';

const edgeCountByPath = new Map();
const vertexCountByPath = new Map();
const fs = {
  readFileAsSingleUint64: vi.fn(async (path) => {
    if (edgeCountByPath.has(path)) {
      return edgeCountByPath.get(path);
    }
    if (vertexCountByPath.has(path)) {
      return vertexCountByPath.get(path);
    }
    throw new Error(`Unexpected path: ${path}`);
  }),
};

vi.mock('../src/core/filesystem.js', () => ({
  fileSystemFromUriOrPath: vi.fn((path) => [fs, path]),
}));

function makeEdgeInfo() {
  return EdgeInfo.load({
    src_type: 'person',
    edge_type: 'knows',
    dst_type: 'person',
    chunk_size: 16,
    src_chunk_size: 100,
    dst_chunk_size: 10,
    directed: true,
    prefix: 'edge/person_knows_person/',
    adj_lists: [
      { ordered: true, aligned_by: 'src', file_type: 'parquet' },
      { ordered: true, aligned_by: 'dst', file_type: 'parquet' },
      { ordered: false, aligned_by: 'src', file_type: 'parquet' },
      { ordered: false, aligned_by: 'dst', file_type: 'parquet' },
    ],
    version: 'gar/v1',
  });
}

describe('EdgesCollection adjList type routing', () => {
  beforeEach(() => {
    edgeCountByPath.clear();
    vertexCountByPath.clear();
    fs.readFileAsSingleUint64.mockClear();
  });

  it('uses ordered_by_dest counts when building an ordered_by_dest collection', async () => {
    const edgeInfo = makeEdgeInfo();
    const graphInfo = new GraphInfo('g', [], [edgeInfo], [], 'http://example.test/graphs/');

    vertexCountByPath.set(
      'http://example.test/graphs/edge/person_knows_person/ordered_by_dest/vertex_count',
      20n,
    );
    edgeCountByPath.set(
      'http://example.test/graphs/edge/person_knows_person/ordered_by_dest/edge_count0',
      17n,
    );
    edgeCountByPath.set(
      'http://example.test/graphs/edge/person_knows_person/ordered_by_dest/edge_count1',
      33n,
    );

    const collection = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_DEST,
    );

    expect(collection.adjListType).toBe(AdjListType.ORDERED_BY_DEST);
    expect(collection.indexConverter.edgeChunkNums).toEqual([2n, 3n]);
    expect(collection.edgeNum).toBe(50n);
  });

  it('uses unordered_by_source counts when building an unordered_by_source collection', async () => {
    const edgeInfo = makeEdgeInfo();
    const graphInfo = new GraphInfo('g', [], [edgeInfo], [], 'http://example.test/graphs/');

    vertexCountByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_source/vertex_count',
      200n,
    );
    edgeCountByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_source/edge_count0',
      8n,
    );
    edgeCountByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_source/edge_count1',
      24n,
    );

    const collection = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.UNORDERED_BY_SOURCE,
      0n,
      2n,
    );

    expect(collection.adjListType).toBe(AdjListType.UNORDERED_BY_SOURCE);
    expect(collection.indexConverter.edgeChunkNums).toEqual([1n, 2n]);
    expect(collection.edgeNum).toBe(32n);
  });

  it('uses unordered_by_dest counts when building an unordered_by_dest collection', async () => {
    const edgeInfo = makeEdgeInfo();
    const graphInfo = new GraphInfo('g', [], [edgeInfo], [], 'http://example.test/graphs/');

    vertexCountByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_dest/vertex_count',
      20n,
    );
    edgeCountByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_dest/edge_count0',
      1n,
    );
    edgeCountByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_dest/edge_count1',
      16n,
    );

    const collection = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.UNORDERED_BY_DEST,
    );

    expect(collection.adjListType).toBe(AdjListType.UNORDERED_BY_DEST);
    expect(collection.indexConverter.edgeChunkNums).toEqual([1n, 1n]);
    expect(collection.edgeNum).toBe(17n);
  });
});
