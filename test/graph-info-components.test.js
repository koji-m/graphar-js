import { describe, expect, it } from 'vitest';
import {
  AdjacentList,
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
