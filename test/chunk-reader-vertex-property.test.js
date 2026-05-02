import * as arrow from 'apache-arrow';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { VertexPropertyArrowChunkReader } from '../src/core/chunk-reader.js';
import GENERAL_PARAMS from '../src/core/general-params.js';
import { VertexInfo } from '../src/core/graph-info.js';

const fs = {
  readFileAsSingleUint64: vi.fn(),
  readFileAsTable: vi.fn(),
};

vi.mock('../src/core/filesystem.js', () => ({
  fileSystemFromUriOrPath: vi.fn((path) => [fs, path]),
}));

function makeVertexInfo() {
  return VertexInfo.load({
    type: 'person',
    chunk_size: 2,
    prefix: 'vertex/person/',
    labels: ['active', 'contractor'],
    property_groups: [
      {
        file_type: 'parquet',
        properties: [
          {
            name: 'id',
            data_type: 'int64',
            is_primary: true,
          },
          {
            name: 'firstName',
            data_type: 'string',
            is_primary: false,
          },
        ],
      },
    ],
    version: 'gar/v1',
  });
}

describe('VertexPropertyArrowChunkReader', () => {
  beforeEach(() => {
    fs.readFileAsSingleUint64.mockReset();
    fs.readFileAsTable.mockReset();
  });

  it('projects selected vertex property columns', async () => {
    const vertexInfo = makeVertexInfo();
    fs.readFileAsSingleUint64.mockResolvedValue(5n);
    fs.readFileAsTable.mockResolvedValue(
      arrow.tableFromArrays({
        _graphArVertexIndex: [0n, 1n],
        id: [100n, 101n],
        firstName: ['Ann', 'Bob'],
      }),
    );

    const reader = await VertexPropertyArrowChunkReader.create({
      vertexInfo,
      propertyGroup: vertexInfo.propertyGroups[0],
      prefix: 'http://example.test/graphs/',
      options: {
        columns: ['firstName'],
      },
    });

    reader.seek(0n);
    const chunk = await reader.getChunk();

    expect(fs.readFileAsTable).toHaveBeenCalledWith(
      'http://example.test/graphs/vertex/person/id_firstName/chunk0',
      'parquet',
      ['firstName'],
    );
    expect(chunk.schema.fields.map((field) => field.name)).toEqual(['firstName']);
    expect(chunk.getChild('firstName').get(0)).toBe('Ann');
  });

  it('filters vertex property rows with JS-side expressions', async () => {
    const vertexInfo = makeVertexInfo();
    fs.readFileAsSingleUint64.mockResolvedValue(5n);
    fs.readFileAsTable.mockResolvedValue(
      arrow.tableFromArrays({
        _graphArVertexIndex: [0n, 1n],
        id: [100n, 101n],
        firstName: ['Ann', 'Bob'],
      }),
    );

    const reader = await VertexPropertyArrowChunkReader.create({
      vertexInfo,
      propertyGroup: vertexInfo.propertyGroups[0],
      prefix: 'http://example.test/graphs/',
      options: {
        filter: {
          op: 'eq',
          column: 'firstName',
          value: 'Bob',
        },
      },
    });

    reader.seek(0n);
    const chunk = await reader.getChunk();

    expect(chunk.numRows).toBe(1);
    expect(chunk.getChild('id').get(0)).toBe(101n);
    expect(chunk.getChild('firstName').get(0)).toBe('Bob');
  });

  it('filters vertex label chunks with declared label columns', async () => {
    const vertexInfo = makeVertexInfo();
    fs.readFileAsSingleUint64.mockResolvedValue(5n);
    fs.readFileAsTable.mockResolvedValue(
      arrow.tableFromArrays({
        active: [true, false],
        contractor: [false, true],
      }),
    );

    const reader = await VertexPropertyArrowChunkReader.create({
      vertexInfo,
      propertyGroup: vertexInfo.propertyGroups[0],
      prefix: 'http://example.test/graphs/',
      options: {
        filter: {
          op: 'eq',
          column: 'contractor',
          value: true,
        },
      },
    });

    reader.seek(0n);
    const chunk = await reader.getLabelChunk();

    expect(chunk.numRows).toBe(1);
    expect(chunk.getChild('active').get(0)).toBe(false);
    expect(chunk.getChild('contractor').get(0)).toBe(true);
  });

  it('keeps the vertex index column when propertyNames narrows the reader to one property', async () => {
    const vertexInfo = makeVertexInfo();
    fs.readFileAsSingleUint64.mockResolvedValue(5n);
    fs.readFileAsTable.mockResolvedValue(
      arrow.tableFromArrays({
        _graphArVertexIndex: [0n, 1n],
        id: [100n, 101n],
        firstName: ['Ann', 'Bob'],
      }),
    );

    const reader = await VertexPropertyArrowChunkReader.create({
      vertexInfo,
      propertyGroup: vertexInfo.propertyGroups[0],
      propertyNames: ['firstName'],
      prefix: 'http://example.test/graphs/',
    });

    reader.seek(0n);
    const chunk = await reader.getChunk();

    expect(fs.readFileAsTable).toHaveBeenCalledWith(
      'http://example.test/graphs/vertex/person/id_firstName/chunk0',
      'parquet',
      ['_graphArVertexIndex', 'firstName'],
    );
    expect(chunk.schema.fields.map((field) => field.name)).toEqual([
      '_graphArVertexIndex',
      'firstName',
    ]);
    expect(chunk.getChild('_graphArVertexIndex').get(0)).toBe(0n);
    expect(chunk.getChild('firstName').get(0)).toBe('Ann');
  });

  it('rejects select columns outside propertyNames', async () => {
    const vertexInfo = makeVertexInfo();
    fs.readFileAsSingleUint64.mockResolvedValue(5n);

    const reader = await VertexPropertyArrowChunkReader.create({
      vertexInfo,
      propertyGroup: vertexInfo.propertyGroups[0],
      propertyNames: ['firstName'],
      prefix: 'http://example.test/graphs/',
    });

    reader.select(['id']);
    reader.seek(0n);

    await expect(reader.getChunk()).rejects.toThrow(
      /Column id is not in select properties/,
    );
  });

  it('rejects filter columns outside propertyNames', async () => {
    const vertexInfo = makeVertexInfo();
    fs.readFileAsSingleUint64.mockResolvedValue(5n);

    const reader = await VertexPropertyArrowChunkReader.create({
      vertexInfo,
      propertyGroup: vertexInfo.propertyGroups[0],
      propertyNames: ['firstName'],
      prefix: 'http://example.test/graphs/',
      options: {
        filter: {
          op: 'eq',
          column: 'id',
          value: 100n,
        },
      },
    });

    reader.seek(0n);

    await expect(reader.getChunk()).rejects.toThrow(
      /Column id is not in select properties/,
    );
  });

  it('allows filtering within propertyNames while keeping the vertex index column', async () => {
    const vertexInfo = makeVertexInfo();
    fs.readFileAsSingleUint64.mockResolvedValue(5n);
    fs.readFileAsTable.mockResolvedValue(
      arrow.tableFromArrays({
        _graphArVertexIndex: [0n, 1n],
        id: [100n, 101n],
        firstName: ['Ann', 'Bob'],
      }),
    );

    const reader = await VertexPropertyArrowChunkReader.create({
      vertexInfo,
      propertyGroup: vertexInfo.propertyGroups[0],
      propertyNames: ['firstName'],
      prefix: 'http://example.test/graphs/',
      options: {
        filter: {
          op: 'eq',
          column: 'firstName',
          value: 'Bob',
        },
      },
    });

    reader.seek(0n);
    const chunk = await reader.getChunk();

    expect(fs.readFileAsTable).toHaveBeenCalledWith(
      'http://example.test/graphs/vertex/person/id_firstName/chunk0',
      'parquet',
      ['_graphArVertexIndex', 'firstName'],
    );
    expect(chunk.numRows).toBe(1);
    expect(chunk.schema.fields.map((field) => field.name)).toEqual([
      '_graphArVertexIndex',
      'firstName',
    ]);
    expect(chunk.getChild('_graphArVertexIndex').get(0)).toBe(1n);
    expect(chunk.getChild('firstName').get(0)).toBe('Bob');
  });

  it('keeps the full vertex-property schema on the no-filter property-group path', async () => {
    const vertexInfo = makeVertexInfo();
    fs.readFileAsSingleUint64.mockResolvedValue(5n);
    fs.readFileAsTable.mockResolvedValue(
      arrow.tableFromArrays({
        _graphArVertexIndex: [0n, 1n],
        id: [100n, 101n],
        firstName: ['Ann', 'Bob'],
      }),
    );

    const reader = await VertexPropertyArrowChunkReader.create({
      vertexInfo,
      propertyGroup: vertexInfo.propertyGroups[0],
      prefix: 'http://example.test/graphs/',
    });

    reader.seek(0n);
    const chunk = await reader.getChunk();

    expect(chunk.schema.fields.map((field) => field.name)).toEqual([
      GENERAL_PARAMS.kVertexIndexCol,
      'id',
      'firstName',
    ]);
  });

  it('keeps the narrowed schema shape on the no-filter propertyNames path', async () => {
    const vertexInfo = makeVertexInfo();
    fs.readFileAsSingleUint64.mockResolvedValue(5n);
    fs.readFileAsTable.mockResolvedValue(
      arrow.tableFromArrays({
        _graphArVertexIndex: [0n, 1n],
        id: [100n, 101n],
        firstName: ['Ann', 'Bob'],
      }),
    );

    const reader = await VertexPropertyArrowChunkReader.create({
      vertexInfo,
      propertyGroup: vertexInfo.propertyGroups[0],
      propertyNames: ['firstName'],
      prefix: 'http://example.test/graphs/',
    });

    reader.seek(0n);
    const chunk = await reader.getChunk();

    expect(chunk.schema.fields.map((field) => field.name)).toEqual([
      GENERAL_PARAMS.kVertexIndexCol,
      'firstName',
    ]);
  });

  it('does not force the no-filter schema after JS-side filtering', async () => {
    const vertexInfo = makeVertexInfo();
    fs.readFileAsSingleUint64.mockResolvedValue(5n);
    fs.readFileAsTable.mockResolvedValue(
      arrow.tableFromArrays({
        _graphArVertexIndex: [0n, 1n],
        id: [100n, 101n],
        firstName: ['Ann', 'Bob'],
      }),
    );

    const reader = await VertexPropertyArrowChunkReader.create({
      vertexInfo,
      propertyGroup: vertexInfo.propertyGroups[0],
      prefix: 'http://example.test/graphs/',
      options: {
        filter: {
          op: 'eq',
          column: 'firstName',
          value: 'Bob',
        },
      },
    });

    reader.seek(0n);
    const chunk = await reader.getChunk();

    expect(chunk.schema.fields.map((field) => field.name)).toEqual([
      GENERAL_PARAMS.kVertexIndexCol,
      'id',
      'firstName',
    ]);
    expect(chunk.numRows).toBe(1);
  });

  it('returns to the no-filter path after clearing select and filter constraints', async () => {
    const vertexInfo = makeVertexInfo();
    fs.readFileAsSingleUint64.mockResolvedValue(5n);
    fs.readFileAsTable.mockResolvedValue(
      arrow.tableFromArrays({
        _graphArVertexIndex: [0n, 1n],
        id: [100n, 101n],
        firstName: ['Ann', 'Bob'],
      }),
    );

    const reader = await VertexPropertyArrowChunkReader.create({
      vertexInfo,
      propertyGroup: vertexInfo.propertyGroups[0],
      prefix: 'http://example.test/graphs/',
      options: {
        columns: ['firstName'],
        filter: {
          op: 'eq',
          column: 'firstName',
          value: 'Bob',
        },
      },
    });

    reader.select(null);
    reader.filter(null);
    reader.seek(0n);
    const chunk = await reader.getChunk();

    expect(fs.readFileAsTable).toHaveBeenLastCalledWith(
      'http://example.test/graphs/vertex/person/id_firstName/chunk0',
      'parquet',
    );
    expect(chunk.schema.fields.map((field) => field.name)).toEqual([
      GENERAL_PARAMS.kVertexIndexCol,
      'id',
      'firstName',
    ]);
    expect(chunk.numRows).toBe(2);
  });
});
