import { describe, expect, it } from 'vitest';
import {
  AdjacentList,
  EdgeInfo,
  PropertyGroup,
  VertexInfo,
} from '../src/core/graph-info.js';
import { AdjListType, Type } from '../src/core/types.js';

describe('PropertyGroup', () => {
  it('uses joined property names as the default prefix', () => {
    const propertyGroup = new PropertyGroup({
      fileType: 'parquet',
      properties: [{ name: 'firstName' }, { name: 'lastName' }],
    });

    expect(propertyGroup.properties).toEqual([
      { name: 'firstName' },
      { name: 'lastName' },
    ]);
    expect(propertyGroup.fileType).toBe('parquet');
    expect(propertyGroup.prefix).toBe('firstName_lastName/');
  });

  it('preserves an explicit prefix', () => {
    const propertyGroup = new PropertyGroup({
      fileType: 'parquet',
      prefix: 'names/',
      properties: [{ name: 'firstName' }],
    });

    expect(propertyGroup.prefix).toBe('names/');
  });
});

describe('AdjacentList', () => {
  it('uses the adjacency list type name as the default prefix', () => {
    const adjacentList = new AdjacentList(
      AdjListType.ORDERED_BY_SOURCE,
      'parquet',
      '',
    );

    expect(adjacentList.type).toBe(AdjListType.ORDERED_BY_SOURCE);
    expect(adjacentList.fileType).toBe('parquet');
    expect(adjacentList.prefix).toBe('ordered_by_source/');
  });

  it('preserves an explicit prefix', () => {
    const adjacentList = new AdjacentList(
      AdjListType.UNORDERED_BY_DEST,
      'parquet',
      'coo_by_dst/',
    );

    expect(adjacentList.type).toBe(AdjListType.UNORDERED_BY_DEST);
    expect(adjacentList.fileType).toBe('parquet');
    expect(adjacentList.prefix).toBe('coo_by_dst/');
  });
});

describe('VertexInfo', () => {
  it('loads metadata from a YAML-equivalent object', () => {
    const vertexInfo = VertexInfo.load({
      type: 'person',
      chunk_size: 100,
      prefix: 'vertex/person/',
      labels: ['organisation'],
      property_groups: [
        {
          prefix: 'id/',
          file_type: 'parquet',
          properties: [
            {
              name: 'id',
              data_type: 'int64',
              is_primary: true,
            },
          ],
        },
        {
          file_type: 'parquet',
          properties: [
            {
              name: 'firstName',
              data_type: 'string',
              is_primary: false,
            },
            {
              name: 'feature',
              data_type: 'list<float>',
              is_primary: false,
              cardinality: 'list',
            },
          ],
        },
      ],
      version: 'gar/v1',
    });

    expect(vertexInfo.type).toBe('person');
    expect(vertexInfo.chunkSize).toBe(100);
    expect(vertexInfo.prefix).toBe('vertex/person/');
    expect(vertexInfo.labels).toEqual(['organisation']);
    expect(vertexInfo.version.version()).toBe(1);
    expect(vertexInfo.version.toString()).toBe('gar/v1');
    expect(vertexInfo.propertyGroups).toHaveLength(2);

    const idGroup = vertexInfo.propertyGroups[0];
    expect(idGroup.prefix).toBe('id/');
    expect(idGroup.fileType).toBe('parquet');
    expect(idGroup.properties).toHaveLength(1);
    expect(idGroup.properties[0].name).toBe('id');
    expect(idGroup.properties[0].type.id).toBe(Type.INT64);
    expect(idGroup.properties[0].isPrimary).toBe(true);
    expect(idGroup.properties[0].isNullable).toBe(false);
    expect(idGroup.properties[0].cardinality).toBe('single');

    const propertyGroup = vertexInfo.propertyGroups[1];
    expect(propertyGroup.prefix).toBe('firstName_feature/');
    expect(propertyGroup.fileType).toBe('parquet');
    expect(propertyGroup.properties).toHaveLength(2);
    expect(propertyGroup.properties[0].name).toBe('firstName');
    expect(propertyGroup.properties[0].type.id).toBe(Type.STRING);
    expect(propertyGroup.properties[0].isPrimary).toBe(false);
    expect(propertyGroup.properties[0].isNullable).toBe(true);
    expect(propertyGroup.properties[0].cardinality).toBe('single');
    expect(propertyGroup.properties[1].name).toBe('feature');
    expect(propertyGroup.properties[1].type.id).toBe(Type.LIST);
    expect(propertyGroup.properties[1].type.child.id).toBe(Type.FLOAT);
    expect(propertyGroup.properties[1].cardinality).toBe('list');
  });

  it('builds vertex metadata file paths', () => {
    const vertexInfo = VertexInfo.load({
      type: 'test_vertex',
      chunk_size: 100,
      prefix: 'test_vertex/',
      property_groups: [
        {
          prefix: 'p0_p1/',
          file_type: 'parquet',
          properties: [
            {
              name: 'p0',
              data_type: 'int32',
              is_primary: true,
            },
            {
              name: 'p1',
              data_type: 'string',
              is_primary: false,
            },
          ],
        },
      ],
      version: 'gar/v1',
    });

    const propertyGroup = vertexInfo.propertyGroups[0];

    expect(vertexInfo.getPathPrefix(propertyGroup)).toBe(
      'test_vertex/p0_p1/',
    );
    expect(vertexInfo.getFilePath(propertyGroup, 0)).toBe(
      'test_vertex/p0_p1/chunk0',
    );
    expect(vertexInfo.getVerticesNumFilePath()).toBe(
      'test_vertex/vertex_count',
    );
  });
});

describe('EdgeInfo', () => {
  it('loads metadata from a YAML-equivalent object', () => {
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
        {
          ordered: true,
          aligned_by: 'src',
          file_type: 'parquet',
        },
        {
          ordered: true,
          aligned_by: 'dst',
          file_type: 'parquet',
        },
      ],
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

    expect(edgeInfo.srcType).toBe('person');
    expect(edgeInfo.edgeType).toBe('knows');
    expect(edgeInfo.dstType).toBe('person');
    expect(edgeInfo.chunkSize).toBe(1024);
    expect(edgeInfo.srcChunkSize).toBe(100);
    expect(edgeInfo.dstChunkSize).toBe(100);
    expect(edgeInfo.directed).toBe(true);
    expect(edgeInfo.prefix).toBe('edge/person_knows_person/');
    expect(edgeInfo.version.version()).toBe(1);
    expect(edgeInfo.version.toString()).toBe('gar/v1');

    expect(edgeInfo.adjacentList).toHaveLength(3);
    expect(edgeInfo.adjacentList[0].type).toBe(
      AdjListType.UNORDERED_BY_SOURCE,
    );
    expect(edgeInfo.adjacentList[0].prefix).toBe('unordered_by_source/');
    expect(edgeInfo.adjacentList[1].type).toBe(AdjListType.ORDERED_BY_SOURCE);
    expect(edgeInfo.adjacentList[1].prefix).toBe('ordered_by_source/');
    expect(edgeInfo.adjacentList[2].type).toBe(AdjListType.ORDERED_BY_DEST);
    expect(edgeInfo.adjacentList[2].prefix).toBe('ordered_by_dest/');
    expect(edgeInfo.hasAdjacentListType(AdjListType.ORDERED_BY_SOURCE)).toBe(
      true,
    );
    expect(edgeInfo.hasAdjacentListType(AdjListType.UNORDERED_BY_DEST)).toBe(
      false,
    );

    expect(edgeInfo.propertyGroups).toHaveLength(1);
    const propertyGroup = edgeInfo.propertyGroups[0];
    expect(propertyGroup.prefix).toBe('creationDate/');
    expect(propertyGroup.fileType).toBe('parquet');
    expect(propertyGroup.properties).toHaveLength(1);
    expect(propertyGroup.properties[0].name).toBe('creationDate');
    expect(propertyGroup.properties[0].type.id).toBe(Type.STRING);
    expect(propertyGroup.properties[0].isPrimary).toBe(false);
    expect(propertyGroup.properties[0].isNullable).toBe(true);
  });

  it('builds edge metadata file paths', () => {
    const edgeInfo = EdgeInfo.load({
      src_type: 'person',
      edge_type: 'knows',
      dst_type: 'person',
      chunk_size: 1024,
      src_chunk_size: 100,
      dst_chunk_size: 100,
      directed: true,
      prefix: 'test_edge/',
      adj_lists: [
        {
          ordered: true,
          aligned_by: 'src',
          file_type: 'csv',
          prefix: 'ordered_by_source/',
        },
      ],
      property_groups: [
        {
          prefix: 'p0_p1/',
          file_type: 'csv',
          properties: [
            {
              name: 'p0',
              data_type: 'int32',
              is_primary: true,
            },
            {
              name: 'p1',
              data_type: 'string',
              is_primary: false,
            },
          ],
        },
      ],
      version: 'gar/v1',
    });

    const propertyGroup = edgeInfo.propertyGroups[0];

    expect(edgeInfo.getAdjListPathPrefix(AdjListType.ORDERED_BY_SOURCE)).toBe(
      'test_edge/ordered_by_source/adj_list/',
    );
    expect(edgeInfo.getAdjListFilePath(0, 0, AdjListType.ORDERED_BY_SOURCE)).toBe(
      'test_edge/ordered_by_source/adj_list/part0/chunk0',
    );
    expect(edgeInfo.getOffsetPathPrefix(AdjListType.ORDERED_BY_SOURCE)).toBe(
      'test_edge/ordered_by_source/offset/',
    );
    expect(
      edgeInfo.getAdjListOffsetFilePath(0, AdjListType.ORDERED_BY_SOURCE),
    ).toBe('test_edge/ordered_by_source/offset/chunk0');
    expect(edgeInfo.getEdgesNumFilePath(0, AdjListType.ORDERED_BY_SOURCE)).toBe(
      'test_edge/ordered_by_source/edge_count0',
    );
    expect(
      edgeInfo.getVerticesNumFilePath(AdjListType.ORDERED_BY_SOURCE),
    ).toBe('test_edge/ordered_by_source/vertex_count');
    expect(
      edgeInfo.getPropertyGroupPathPrefix(
        propertyGroup,
        AdjListType.ORDERED_BY_SOURCE,
      ),
    ).toBe('test_edge/ordered_by_source/p0_p1/');
    expect(
      edgeInfo.getPropertyFilePath(
        propertyGroup,
        AdjListType.ORDERED_BY_SOURCE,
        0,
        0,
      ),
    ).toBe('test_edge/ordered_by_source/p0_p1/part0/chunk0');
  });

  it('rejects non-single cardinality for edge properties', () => {
    expect(() =>
      EdgeInfo.load({
        src_type: 'person',
        edge_type: 'knows',
        dst_type: 'person',
        chunk_size: 1024,
        src_chunk_size: 100,
        dst_chunk_size: 100,
        directed: true,
        adj_lists: [
          {
            ordered: true,
            aligned_by: 'src',
            file_type: 'parquet',
          },
        ],
        property_groups: [
          {
            file_type: 'parquet',
            properties: [
              {
                name: 'creationDate',
                data_type: 'string',
                is_primary: false,
                cardinality: 'list',
              },
            ],
          },
        ],
      }),
    ).toThrow(/Unsupported cardinality: list for edge property/);
  });
});
