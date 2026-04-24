import { beforeEach, describe, expect, it, vi } from 'vitest';
import { AdjListOffsetArrowChunkReader } from '../src/core/chunk-reader.js';
import { EdgeInfo } from '../src/core/graph-info.js';
import { AdjListType } from '../src/core/types.js';

const fs = {
  readFileAsSingleUint64: vi.fn(),
  readFileAsTable: vi.fn(),
};

vi.mock('../src/core/filesystem.js', () => ({
  fileSystemFromUriOrPath: vi.fn((path) => [fs, path]),
}));

function createOffsetTable(offsets) {
  const column = {
    get: vi.fn((index) => offsets[index]),
  };
  return {
    slice: vi.fn((rowOffset) => ({
      getChildAt: vi.fn((index) => (index === 0 ? column : null)),
      rowOffset,
    })),
    column,
  };
}

describe('AdjListOffsetArrowChunkReader', () => {
  beforeEach(() => {
    fs.readFileAsSingleUint64.mockReset();
    fs.readFileAsTable.mockReset();
  });

  it('creates an ordered_by_source reader with source chunk sizing', async () => {
    const edgeInfo = EdgeInfo.load({
      src_type: 'person',
      edge_type: 'knows',
      dst_type: 'person',
      chunk_size: 1024,
      src_chunk_size: 100,
      dst_chunk_size: 200,
      directed: true,
      prefix: 'edge/person_knows_person/',
      adj_lists: [
        {
          ordered: true,
          aligned_by: 'src',
          file_type: 'parquet',
        },
      ],
      version: 'gar/v1',
    });
    fs.readFileAsSingleUint64.mockResolvedValue(250n);

    const reader = await AdjListOffsetArrowChunkReader.create({
      edgeInfo,
      adjListType: AdjListType.ORDERED_BY_SOURCE,
      prefix: 'http://example.test/graphs/',
    });

    expect(reader.vertexChunkNum).toBe(3n);
    expect(reader.vertexChunkSize).toBe(100);
  });

  it('seeks and returns the sliced offset column for the current vertex id', async () => {
    const edgeInfo = EdgeInfo.load({
      src_type: 'person',
      edge_type: 'knows',
      dst_type: 'person',
      chunk_size: 1024,
      src_chunk_size: 100,
      dst_chunk_size: 200,
      directed: true,
      prefix: 'edge/person_knows_person/',
      adj_lists: [
        {
          ordered: true,
          aligned_by: 'src',
          file_type: 'parquet',
        },
      ],
      version: 'gar/v1',
    });
    fs.readFileAsSingleUint64.mockResolvedValue(250n);
    const table = createOffsetTable([0n, 3n, 8n, 10n]);
    fs.readFileAsTable.mockResolvedValue(table);

    const reader = await AdjListOffsetArrowChunkReader.create({
      edgeInfo,
      adjListType: AdjListType.ORDERED_BY_SOURCE,
      prefix: 'http://example.test/graphs/',
    });

    expect(await reader.seek(1n)).toEqual({ ok: true });
    const chunk = await reader.getChunk();

    expect(fs.readFileAsTable).toHaveBeenCalledWith(
      'http://example.test/graphs/edge/person_knows_person/ordered_by_source/offset/chunk0',
      'parquet',
    );
    expect(table.slice).toHaveBeenCalledWith(1);
    expect(chunk.get(0)).toBe(0n);
    expect(chunk.get(1)).toBe(3n);
  });

  it('advances to the next offset chunk and resets seekId to the chunk start', async () => {
    const edgeInfo = EdgeInfo.load({
      src_type: 'person',
      edge_type: 'knows',
      dst_type: 'person',
      chunk_size: 1024,
      src_chunk_size: 100,
      dst_chunk_size: 200,
      directed: true,
      prefix: 'edge/person_knows_person/',
      adj_lists: [
        {
          ordered: true,
          aligned_by: 'src',
          file_type: 'parquet',
        },
      ],
      version: 'gar/v1',
    });
    fs.readFileAsSingleUint64.mockResolvedValue(250n);

    const reader = await AdjListOffsetArrowChunkReader.create({
      edgeInfo,
      adjListType: AdjListType.ORDERED_BY_SOURCE,
      prefix: 'http://example.test/graphs/',
    });

    expect(await reader.nextChunk()).toEqual({ ok: true });
    expect(reader.chunkIndex).toBe(1);
    expect(reader.seekId).toBe(100n);
  });

  it('returns IndexError when seeking past the last vertex chunk', async () => {
    const edgeInfo = EdgeInfo.load({
      src_type: 'person',
      edge_type: 'knows',
      dst_type: 'person',
      chunk_size: 1024,
      src_chunk_size: 100,
      dst_chunk_size: 200,
      directed: true,
      prefix: 'edge/person_knows_person/',
      adj_lists: [
        {
          ordered: true,
          aligned_by: 'src',
          file_type: 'parquet',
        },
      ],
      version: 'gar/v1',
    });
    fs.readFileAsSingleUint64.mockResolvedValue(250n);

    const reader = await AdjListOffsetArrowChunkReader.create({
      edgeInfo,
      adjListType: AdjListType.ORDERED_BY_SOURCE,
      prefix: 'http://example.test/graphs/',
    });

    const result = await reader.seek(300n);
    expect(result.ok).toBe(false);
    expect(result.error.code).toBe('IndexError');
  });
});
