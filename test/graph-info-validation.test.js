import { describe, expect, it } from 'vitest';
import {
  AdjacentList,
  EdgeInfo,
  PropertyGroup,
  VertexInfo,
} from '../src/core/graph-info.js';
import { AdjListType, FileType, Type } from '../src/core/types.js';

describe('GraphInfo metadata validation', () => {
  it('rejects json property groups at the PropertyGroup layer', () => {
    const propertyGroup = new PropertyGroup({
      fileType: FileType.JSON,
      properties: [
        { name: 'score', type: { id: Type.FLOAT }, cardinality: 'single' },
      ],
    });

    expect(propertyGroup.isValidated()).toBe(false);
  });

  it('rejects csv property groups with list type at the PropertyGroup layer', () => {
    const propertyGroup = new PropertyGroup({
      fileType: FileType.CSV,
      properties: [
        {
          name: 'feature',
          type: { id: Type.LIST },
          cardinality: 'single',
        },
      ],
    });

    expect(propertyGroup.isValidated()).toBe(false);
  });

  it('rejects csv property groups with non-single cardinality at the PropertyGroup layer', () => {
    const propertyGroup = new PropertyGroup({
      fileType: FileType.CSV,
      properties: [
        {
          name: 'tags',
          type: { id: Type.STRING },
          cardinality: 'list',
        },
      ],
    });

    expect(propertyGroup.isValidated()).toBe(false);
  });

  it('rejects unsupported file types in adjacent lists at the AdjacentList layer', () => {
    const adjacentList = new AdjacentList(
      AdjListType.ORDERED_BY_SOURCE,
      FileType.JSON,
      'ordered_by_source/',
    );

    expect(adjacentList.isValidated()).toBe(false);
  });

  it('rejects duplicate property names across property groups during VertexInfo load', () => {
    expect(() =>
      VertexInfo.load({
        type: 'person',
        chunk_size: 100,
        prefix: 'vertex/person/',
        property_groups: [
          {
            file_type: 'parquet',
            properties: [{ name: 'id', data_type: 'int64', is_primary: true }],
          },
          {
            file_type: 'parquet',
            properties: [{ name: 'id', data_type: 'string', is_primary: false }],
          },
        ],
        version: 'gar/v1',
      }),
    ).toThrow(/Invalid vertex info metadata/);
  });

  it('rejects csv property groups with list type during VertexInfo load', () => {
    expect(() =>
      VertexInfo.load({
        type: 'person',
        chunk_size: 100,
        prefix: 'vertex/person/',
        property_groups: [
          {
            file_type: 'csv',
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
      }),
    ).toThrow(/Invalid vertex info metadata/);
  });

  it('rejects unsupported storage file types during VertexInfo load', () => {
    expect(() =>
      VertexInfo.load({
        type: 'person',
        chunk_size: 100,
        prefix: 'vertex/person/',
        property_groups: [
          {
            file_type: 'json',
            properties: [{ name: 'id', data_type: 'int64', is_primary: true }],
          },
        ],
        version: 'gar/v1',
      }),
    ).toThrow(/Invalid vertex info metadata/);
  });

  it('accepts omitted vertex prefixes and validates after defaulting them', () => {
    const vertexInfo = VertexInfo.load({
      type: 'person',
      chunk_size: 100,
      property_groups: [
        {
          file_type: 'parquet',
          properties: [{ name: 'id', data_type: 'int64', is_primary: true }],
        },
      ],
      version: 'gar/v1',
    });

    expect(vertexInfo.isValidated()).toBe(true);
    expect(vertexInfo.prefix).toBe('person/');
  });

  it('rejects non-single cardinality for edge properties during EdgeInfo load', () => {
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
    ).toThrow(/Invalid edge info metadata/);
  });

  it('rejects duplicate adjacent list types during EdgeInfo load', () => {
    expect(() =>
      EdgeInfo.load({
        src_type: 'person',
        edge_type: 'knows',
        dst_type: 'person',
        chunk_size: 1024,
        src_chunk_size: 100,
        dst_chunk_size: 100,
        directed: true,
        prefix: 'edge/person_knows_person/',
        adj_lists: [
          { ordered: true, aligned_by: 'src', file_type: 'parquet' },
          { ordered: true, aligned_by: 'src', file_type: 'parquet' },
        ],
      }),
    ).toThrow(/Invalid edge info metadata/);
  });

  it('rejects unsupported storage file types in adjacent lists during EdgeInfo load', () => {
    expect(() =>
      EdgeInfo.load({
        src_type: 'person',
        edge_type: 'knows',
        dst_type: 'person',
        chunk_size: 1024,
        src_chunk_size: 100,
        dst_chunk_size: 100,
        directed: true,
        prefix: 'edge/person_knows_person/',
        adj_lists: [{ ordered: true, aligned_by: 'src', file_type: 'json' }],
      }),
    ).toThrow(/Invalid edge info metadata/);
  });

  it('accepts omitted edge prefixes and validates after defaulting them', () => {
    const edgeInfo = EdgeInfo.load({
      src_type: 'person',
      edge_type: 'knows',
      dst_type: 'person',
      chunk_size: 1024,
      src_chunk_size: 100,
      dst_chunk_size: 100,
      directed: true,
      adj_lists: [{ ordered: true, aligned_by: 'src', file_type: 'parquet' }],
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

    expect(edgeInfo.isValidated()).toBe(true);
    expect(edgeInfo.prefix).toBe('person_knows_person/');
  });
});
