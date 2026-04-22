import { describe, expect, it } from 'vitest';
import { AdjacentList, PropertyGroup } from '../src/core/graph-info.js';
import { AdjListType } from '../src/core/types.js';

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
