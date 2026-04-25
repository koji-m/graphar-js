import { beforeEach, describe, expect, it, vi } from 'vitest';
import { AdjListPropertyArrowChunkReader } from '../src/core/chunk-reader.js';
import { EdgeInfo } from '../src/core/graph-info.js';
import { AdjListType } from '../src/core/types.js';

const fs = {
  readFileAsSingleUint64: vi.fn(),
  readFileAsTable: vi.fn(),
};

vi.mock('../src/core/filesystem.js', () => ({
  fileSystemFromUriOrPath: vi.fn((path) => [fs, path]),
}));

function makeEdgeInfo(adjLists, { srcChunkSize = 100, dstChunkSize = 200 } = {}) {
  return EdgeInfo.load({
    src_type: 'person',
    edge_type: 'knows',
    dst_type: 'person',
    chunk_size: 16,
    src_chunk_size: srcChunkSize,
    dst_chunk_size: dstChunkSize,
    directed: true,
    prefix: 'edge/person_knows_person/',
    adj_lists: adjLists,
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

function makePropertyTable() {
  return {
    slice: vi.fn((rowOffset) => ({ rowOffset })),
  };
}

describe('AdjListPropertyArrowChunkReader', () => {
  beforeEach(() => {
    fs.readFileAsSingleUint64.mockReset();
    fs.readFileAsTable.mockReset();
  });

  it('seekSrc moves unordered_by_source readers to the first property chunk in the source vertex chunk', async () => {
    const edgeInfo = makeEdgeInfo([
      { ordered: false, aligned_by: 'src', file_type: 'parquet' },
    ]);
    fs.readFileAsSingleUint64
      .mockResolvedValueOnce(250n)
      .mockResolvedValueOnce(32n);

    const reader = await AdjListPropertyArrowChunkReader.create({
      edgeInfo,
      propertyGroup: edgeInfo.propertyGroups[0],
      adjListType: AdjListType.UNORDERED_BY_SOURCE,
      prefix: 'http://example.test/graphs/',
    });

    const result = await reader.seekSrc(101n);

    expect(result).toEqual({ ok: true });
    expect(reader.vertexChunkIndex).toBe(1);
    expect(reader.chunkIndex).toBe(0);
    expect(reader.seekOffset).toBe(0n);
  });

  it('seekDst uses offset lookup for ordered_by_dest readers', async () => {
    const edgeInfo = makeEdgeInfo(
      [{ ordered: true, aligned_by: 'dst', file_type: 'parquet' }],
      { dstChunkSize: 2 },
    );
    fs.readFileAsSingleUint64
      .mockResolvedValueOnce(5n)
      .mockResolvedValueOnce(40n);
    fs.readFileAsTable.mockResolvedValueOnce({
      getChildAt: () => ({
        get: (index) => [11n, 20n, 25n][index],
      }),
    });

    const reader = await AdjListPropertyArrowChunkReader.create({
      edgeInfo,
      propertyGroup: edgeInfo.propertyGroups[0],
      adjListType: AdjListType.ORDERED_BY_DEST,
      prefix: 'http://example.test/graphs/',
    });

    const result = await reader.seekDst(1n);

    expect(result).toEqual({ ok: true });
    expect(reader.vertexChunkIndex).toBe(0);
    expect(reader.seekOffset).toBe(20n);
    expect(fs.readFileAsTable).toHaveBeenCalledWith(
      'http://example.test/graphs/edge/person_knows_person/ordered_by_dest/offset/chunk0',
      'parquet',
    );
  });

  it('getChunk reads the property file path aligned with the current chunk position', async () => {
    const edgeInfo = makeEdgeInfo([
      { ordered: false, aligned_by: 'src', file_type: 'parquet' },
    ]);
    const propertyTable = makePropertyTable();
    fs.readFileAsSingleUint64
      .mockResolvedValueOnce(250n)
      .mockResolvedValueOnce(32n)
      .mockResolvedValueOnce(32n);
    fs.readFileAsTable.mockResolvedValue(propertyTable);

    const reader = await AdjListPropertyArrowChunkReader.create({
      edgeInfo,
      propertyGroup: edgeInfo.propertyGroups[0],
      adjListType: AdjListType.UNORDERED_BY_SOURCE,
      prefix: 'http://example.test/graphs/',
    });

    await reader.seekSrc(101n);
    const chunk = await reader.getChunk();

    expect(fs.readFileAsTable).toHaveBeenLastCalledWith(
      'http://example.test/graphs/edge/person_knows_person/unordered_by_source/creationDate/part1/chunk0',
      'parquet',
    );
    expect(propertyTable.slice).toHaveBeenCalledWith(0);
    expect(chunk).toEqual({ rowOffset: 0 });
  });

  it('rejects invalid seek direction for the current adjacency list type', async () => {
    const edgeInfo = makeEdgeInfo([
      { ordered: false, aligned_by: 'dst', file_type: 'parquet' },
    ]);
    fs.readFileAsSingleUint64.mockResolvedValueOnce(5n);

    const reader = await AdjListPropertyArrowChunkReader.create({
      edgeInfo,
      propertyGroup: edgeInfo.propertyGroups[0],
      adjListType: AdjListType.UNORDERED_BY_DEST,
      prefix: 'http://example.test/graphs/',
    });

    const result = await reader.seekSrc(0n);

    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('Invalid');
  });
});
