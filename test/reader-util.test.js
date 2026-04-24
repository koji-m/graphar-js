import * as arrow from 'apache-arrow';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { EdgeInfo } from '../src/core/graph-info.js';
import { getAdjListOffsetOfVertex } from '../src/core/reader-util.js';
import { AdjListType } from '../src/core/types.js';

const fs = {
  readFileAsTable: vi.fn(),
};

vi.mock('../src/core/filesystem.js', () => ({
  fileSystemFromUriOrPath: vi.fn((path) => [fs, path]),
}));

describe('getAdjListOffsetOfVertex', () => {
  beforeEach(() => {
    fs.readFileAsTable.mockReset();
  });

  it('reads begin and end offsets for ordered_by_source from the offset chunk', async () => {
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

    fs.readFileAsTable.mockResolvedValue(
      new arrow.Table({
        offset: arrow.vectorFromArray([0n, 3n, 8n, 10n]),
      }),
    );

    const [beginOffset, endOffset] = await getAdjListOffsetOfVertex(
      'http://example.test/graphs/',
      edgeInfo,
      AdjListType.ORDERED_BY_SOURCE,
      1n,
    );

    expect(beginOffset).toBe(3n);
    expect(endOffset).toBe(8n);
    expect(fs.readFileAsTable).toHaveBeenCalledWith(
      'http://example.test/graphs/edge/person_knows_person/ordered_by_source/offset/chunk0',
      'parquet',
    );
  });

  it('uses destination chunk sizing for ordered_by_dest', async () => {
    const edgeInfo = EdgeInfo.load({
      src_type: 'person',
      edge_type: 'knows',
      dst_type: 'person',
      chunk_size: 1024,
      src_chunk_size: 100,
      dst_chunk_size: 2,
      directed: true,
      prefix: 'edge/person_knows_person/',
      adj_lists: [
        {
          ordered: true,
          aligned_by: 'dst',
          file_type: 'parquet',
        },
      ],
      version: 'gar/v1',
    });

    fs.readFileAsTable.mockResolvedValue(
      new arrow.Table({
        offset: arrow.vectorFromArray([11n, 20n, 25n]),
      }),
    );

    const [beginOffset, endOffset] = await getAdjListOffsetOfVertex(
      'http://example.test/graphs/',
      edgeInfo,
      AdjListType.ORDERED_BY_DEST,
      1n,
    );

    expect(beginOffset).toBe(20n);
    expect(endOffset).toBe(25n);
    expect(fs.readFileAsTable).toHaveBeenCalledWith(
      'http://example.test/graphs/edge/person_knows_person/ordered_by_dest/offset/chunk0',
      'parquet',
    );
  });

  it('rejects unordered adjacency list types', async () => {
    const edgeInfo = EdgeInfo.load({
      src_type: 'person',
      edge_type: 'knows',
      dst_type: 'person',
      chunk_size: 1024,
      src_chunk_size: 100,
      dst_chunk_size: 100,
      directed: true,
      prefix: 'edge/person_knows_person/',
      adj_lists: [
        {
          ordered: false,
          aligned_by: 'src',
          file_type: 'parquet',
        },
      ],
      version: 'gar/v1',
    });

    await expect(
      getAdjListOffsetOfVertex(
        'http://example.test/graphs/',
        edgeInfo,
        AdjListType.UNORDERED_BY_SOURCE,
        0n,
      ),
    ).rejects.toThrow(
      /The adj list type has to be ordered_by_source or ordered_by_dest/,
    );
  });
});
