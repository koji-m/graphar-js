import { beforeEach, describe, expect, it, vi } from 'vitest';
import { EdgeInfo, GraphInfo } from '../src/core/graph-info.js';
import { EdgesCollection } from '../src/core/graph-reader.js';
import { AdjListType } from '../src/core/types.js';

const edgeCountByPath = new Map();
const vertexCountByPath = new Map();
const tableByPath = new Map();

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
  readFileAsTable: vi.fn(async (path) => {
    const table = tableByPath.get(path);
    if (!table) {
      throw new Error(`Unexpected table path: ${path}`);
    }
    return table;
  }),
};

vi.mock('../src/core/filesystem.js', () => ({
  fileSystemFromUriOrPath: vi.fn((path) => [fs, path]),
}));

function makeAdjTable(rows) {
  return {
    numRows: rows.length,
    slice(rowOffset) {
      const slicedRows = rows.slice(rowOffset);
      return {
        batches: [
          {
            getChildAt(index) {
              return {
                get(offset) {
                  return slicedRows[offset]?.[index];
                },
              };
            },
          },
        ],
      };
    },
  };
}

function makePropertyTable(rowCount) {
  const rows = Array.from({ length: rowCount }, (_, index) => [`p${index}`]);
  return {
    numRows: rows.length,
    slice(rowOffset) {
      const slicedRows = rows.slice(rowOffset);
      return {
        batches: [
          {
            getChildAt(index) {
              return {
                get(offset) {
                  return slicedRows[offset]?.[index];
                },
              };
            },
          },
        ],
      };
    },
  };
}

function makeEdgeInfo() {
  return EdgeInfo.load({
    src_type: 'person',
    edge_type: 'knows',
    dst_type: 'person',
    chunk_size: 2,
    src_chunk_size: 100,
    dst_chunk_size: 100,
    directed: true,
    prefix: 'edge/person_knows_person/',
    adj_lists: [{ ordered: false, aligned_by: 'src', file_type: 'parquet' }],
    property_groups: [
      {
        file_type: 'parquet',
        properties: [
          {
            name: 'creationDate',
            data_type: 'string',
            is_primary: false,
          },
        ],
      },
    ],
    version: 'gar/v1',
  });
}

describe('EdgeIter chunk transitions', () => {
  beforeEach(() => {
    edgeCountByPath.clear();
    vertexCountByPath.clear();
    tableByPath.clear();
    fs.readFileAsSingleUint64.mockClear();
    fs.readFileAsTable.mockClear();
  });

  it('keeps topology and property readers aligned when moving to the next edge chunk in the same vertex chunk', async () => {
    const edgeInfo = makeEdgeInfo();
    const graphInfo = new GraphInfo(
      'g',
      [],
      [edgeInfo],
      [],
      'http://example.test/graphs/',
    );

    vertexCountByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_source/vertex_count',
      100n,
    );
    edgeCountByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_source/edge_count0',
      3n,
    );
    tableByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_source/adj_list/part0/chunk0',
      makeAdjTable([
        [1n, 10n],
        [2n, 20n],
      ]),
    );
    tableByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_source/adj_list/part0/chunk1',
      makeAdjTable([[3n, 30n]]),
    );
    tableByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_source/creationDate/part0/chunk0',
      makePropertyTable(2),
    );
    tableByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_source/creationDate/part0/chunk1',
      makePropertyTable(1),
    );

    const collection = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.UNORDERED_BY_SOURCE,
    );
    const iter = await collection.getIterator();
    const seen = [];

    for await (const edgeIter of iter) {
      seen.push(await edgeIter.source());
      if (seen.length === 2) {
        expect(edgeIter.globalChunkIndex).toBe(0n);
        expect(edgeIter.propertyReaders[0].chunkIndex).toBe(0);
      }
      if (seen.length === 3) {
        expect(edgeIter.globalChunkIndex).toBe(1n);
        expect(edgeIter.adjListReader.chunkIndex).toBe(1);
        expect(edgeIter.propertyReaders[0].chunkIndex).toBe(1);
        break;
      }
    }

    expect(seen).toEqual([1n, 2n, 3n]);
  });

  it('moves topology and property readers together when crossing into the next vertex chunk', async () => {
    const edgeInfo = makeEdgeInfo();
    const graphInfo = new GraphInfo(
      'g',
      [],
      [edgeInfo],
      [],
      'http://example.test/graphs/',
    );

    vertexCountByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_source/vertex_count',
      200n,
    );
    edgeCountByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_source/edge_count0',
      1n,
    );
    edgeCountByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_source/edge_count1',
      1n,
    );
    tableByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_source/adj_list/part0/chunk0',
      makeAdjTable([[1n, 10n]]),
    );
    tableByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_source/adj_list/part1/chunk0',
      makeAdjTable([[101n, 110n]]),
    );
    tableByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_source/creationDate/part0/chunk0',
      makePropertyTable(1),
    );
    tableByPath.set(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_source/creationDate/part1/chunk0',
      makePropertyTable(1),
    );

    const collection = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.UNORDERED_BY_SOURCE,
    );
    const iter = await collection.getIterator();
    const seen = [];

    for await (const edgeIter of iter) {
      seen.push(await edgeIter.source());
      if (seen.length === 2) {
        expect(edgeIter.globalChunkIndex).toBe(1n);
        expect(edgeIter.vertexChunkIndex).toBe(1);
        expect(edgeIter.adjListReader.vertexChunkIndex).toBe(1);
        expect(edgeIter.propertyReaders[0].vertexChunkIndex).toBe(1);
        expect(edgeIter.curOffset).toBe(0n);
        break;
      }
    }

    expect(seen).toEqual([1n, 101n]);
  });
});
