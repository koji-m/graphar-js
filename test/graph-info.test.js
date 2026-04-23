import { describe, expect, it, vi } from 'vitest';
import { GraphInfo } from '../src/core/graph-info.js';
import { AdjListType, Type } from '../src/core/types.js';

const files = new Map([
  [
    'http://example.test/graphs/person.vertex.yml',
    `type: person
chunk_size: 100
prefix: vertex/person/
property_groups:
  - file_type: parquet
    properties:
      - name: id
        data_type: int64
        is_primary: true
version: gar/v1
`,
  ],
  [
    'http://example.test/graphs/person_knows_person.edge.yml',
    `src_type: person
edge_type: knows
dst_type: person
chunk_size: 1024
src_chunk_size: 100
dst_chunk_size: 100
directed: true
prefix: edge/person_knows_person/
adj_lists:
  - ordered: true
    aligned_by: src
    file_type: parquet
property_groups:
  - file_type: parquet
    properties:
      - name: creationDate
        data_type: string
        is_primary: false
version: gar/v1
`,
  ],
]);

const fs = {
  readFileAsText: vi.fn(async (path) => {
    const content = files.get(path);
    if (content === undefined) {
      throw new Error(`Unexpected path: ${path}`);
    }
    return content;
  }),
};

vi.mock('../src/core/filesystem.js', () => ({
  fileSystemFromUriOrPath: vi.fn((path) => [fs, path]),
}));

function graphMeta() {
  return {
    name: 'ldbc_sample',
    prefix: './',
    vertices: ['person.vertex.yml'],
    edges: ['person_knows_person.edge.yml'],
    labels: ['sample'],
    version: 'gar/v1',
    extra_info: [{ key: 'category', value: 'test graph' }],
  };
}

function graphYaml() {
  return `name: ldbc_sample
prefix: ./
vertices:
  - person.vertex.yml
edges:
  - person_knows_person.edge.yml
labels:
  - sample
version: gar/v1
extra_info:
  - key: category
    value: test graph
`;
}

describe('GraphInfo', () => {
  it('constructs graph metadata from a YAML-equivalent object', async () => {
    fs.readFileAsText.mockClear();

    const graphInfo = await GraphInfo.constructGraphInfo(
      graphMeta(),
      'graph',
      'http://example.test/graphs/',
      fs,
      'http://example.test/graphs/',
    );

    expect(graphInfo.graphName).toBe('ldbc_sample');
    expect(graphInfo.prefix).toBe('./');
    expect(graphInfo.version.version()).toBe(1);
    expect(graphInfo.labels).toEqual(['sample']);
    expect(graphInfo.extraInfo).toEqual({ category: 'test graph' });
    expect(graphInfo.vertexInfos).toHaveLength(1);
    expect(graphInfo.edgeInfos).toHaveLength(1);
    expect(fs.readFileAsText).toHaveBeenCalledWith(
      'http://example.test/graphs/person.vertex.yml',
    );
    expect(fs.readFileAsText).toHaveBeenCalledWith(
      'http://example.test/graphs/person_knows_person.edge.yml',
    );

    const vertexInfo = graphInfo.getVertexInfo('person');
    expect(vertexInfo.type).toBe('person');
    expect(vertexInfo.chunkSize).toBe(100);
    expect(vertexInfo.propertyGroups[0].properties[0].type.id).toBe(
      Type.INT64,
    );

    const edgeInfo = graphInfo.getEdgeInfo('person', 'knows', 'person');
    expect(edgeInfo.edgeType).toBe('knows');
    expect(edgeInfo.hasAdjacentListType(AdjListType.ORDERED_BY_SOURCE)).toBe(
      true,
    );
    expect(edgeInfo.propertyGroups[0].properties[0].type.id).toBe(Type.STRING);
  });

  it('uses default graph name and prefix when absent', async () => {
    const graphInfo = await GraphInfo.constructGraphInfo(
      {
        vertices: [],
        edges: [],
      },
      'graph',
      'http://example.test/graphs/',
      fs,
      'http://example.test/graphs/',
    );

    expect(graphInfo.graphName).toBe('graph');
    expect(graphInfo.prefix).toBe('http://example.test/graphs/');
    expect(graphInfo.version).toBeNull();
    expect(graphInfo.vertexInfos).toEqual([]);
    expect(graphInfo.edgeInfos).toEqual([]);
    expect(graphInfo.labels).toEqual([]);
    expect(graphInfo.extraInfo).toEqual({});
  });

  it('loads graph metadata from YAML input and a relative location', async () => {
    fs.readFileAsText.mockClear();

    const graphInfo = await GraphInfo.load({
      input: graphYaml(),
      relativeLocation: 'http://example.test/graphs/',
    });

    expect(graphInfo.graphName).toBe('ldbc_sample');
    expect(graphInfo.prefix).toBe('./');
    expect(graphInfo.getVertexInfo('person')).not.toBeUndefined();
    expect(graphInfo.getEdgeInfo('person', 'knows', 'person')).not.toBeUndefined();
    expect(fs.readFileAsText).toHaveBeenCalledWith(
      'http://example.test/graphs/person.vertex.yml',
    );
    expect(fs.readFileAsText).toHaveBeenCalledWith(
      'http://example.test/graphs/person_knows_person.edge.yml',
    );
  });
});
