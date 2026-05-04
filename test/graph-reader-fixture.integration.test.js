import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import {
  AdjListType,
  EdgesCollection,
  GraphInfo,
  initWasm,
  VerticesCollection,
} from '../src/index.js';
import { getAdjListOffsetOfVertex } from '../src/core/reader-util.js';

const fixtureDir = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  'fixtures',
  'graphar-minimal',
  'parquet',
);
const wasmPath = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  '..',
  'node_modules',
  'parquet-wasm',
  'esm',
  'parquet_wasm_bg.wasm',
);

function stubFixtureFetch(rootDir, baseUrl) {
  vi.stubGlobal('fetch', async (input) => {
    const url = new URL(input instanceof Request ? input.url : input);
    if (url.origin !== new URL(baseUrl).origin) {
      return new Response('Not found', { status: 404 });
    }
    try {
      const requestedPath = decodeURIComponent(url.pathname.slice(1));
      const filePath = path.resolve(rootDir, requestedPath);
      if (
        !filePath.startsWith(`${rootDir}${path.sep}`) &&
        filePath !== rootDir
      ) {
        return new Response('Forbidden', { status: 403 });
      }
      const data = await readFile(filePath);
      return new Response(data, { status: 200 });
    } catch {
      return new Response('Not found', { status: 404 });
    }
  });
}

async function collectEdges(collection) {
  const iterator = await collection.getIterator();
  const edges = [];
  for await (const edge of iterator) {
    edges.push([await edge.source(), await edge.destination()]);
  }
  return edges;
}

async function collectEdgesWithProperties(collection) {
  const iterator = await collection.getIterator();
  const edges = [];
  for await (const edge of iterator) {
    edges.push({
      src: await edge.source(),
      dst: await edge.destination(),
      creationDate: await edge.property('creationDate'),
    });
  }
  return edges;
}

describe('Graph reader minimal fixture integration', () => {
  const fixtureBaseUrl = 'http://fixture.test/';
  let graphInfo;

  beforeAll(async () => {
    await initWasm({ module_or_path: await readFile(wasmPath) });
    stubFixtureFetch(fixtureDir, fixtureBaseUrl);
    graphInfo = await GraphInfo.load({
      path: `${fixtureBaseUrl}ldbc_sample.graph.yml`,
    });
  });

  afterAll(async () => {
    vi.unstubAllGlobals();
  });

  it('loads graph, vertex, and edge metadata from the fixture', () => {
    expect(graphInfo.graphName).toBe('ldbc_sample');
    expect(graphInfo.prefix).toBe(fixtureBaseUrl);
    expect(graphInfo.vertexInfos).toHaveLength(1);
    expect(graphInfo.edgeInfos).toHaveLength(1);

    const vertexInfo = graphInfo.getVertexInfo('person');
    expect(vertexInfo.chunkSize).toBe(2);
    expect(vertexInfo.labels).toEqual(['active', 'engineer', 'contractor']);
    expect(vertexInfo.propertyGroups.map((group) => group.prefix)).toEqual([
      'id/',
      'firstName/',
    ]);

    const edgeInfo = graphInfo.getEdgeInfo('person', 'knows', 'person');
    expect(edgeInfo.chunkSize).toBe(2);
    expect(edgeInfo.srcChunkSize).toBe(2);
    expect(edgeInfo.dstChunkSize).toBe(2);
    expect(edgeInfo.directed).toBe(true);
    expect(edgeInfo.adjacentList.map((adjList) => adjList.type)).toEqual([
      AdjListType.ORDERED_BY_SOURCE,
      AdjListType.ORDERED_BY_DEST,
      AdjListType.UNORDERED_BY_SOURCE,
      AdjListType.UNORDERED_BY_DEST,
    ]);
  });

  it('iterates vertex properties across all vertex chunks', async () => {
    const vertices = await VerticesCollection.make(graphInfo, 'person');
    const iterator = await vertices.getIterator();
    const rows = [];

    for (const vertex of iterator) {
      rows.push({
        internalId: vertex.id(),
        id: await vertex.property('id'),
        firstName: await vertex.property('firstName'),
        labels: await vertex.label(),
      });
    }

    expect(vertices.vertexNum).toBe(5n);
    expect(rows).toEqual([
      {
        internalId: 0n,
        id: 100n,
        firstName: 'Ann',
        labels: ['active', 'engineer'],
      },
      { internalId: 1n, id: 101n, firstName: 'Bob', labels: ['active'] },
      { internalId: 2n, id: 102n, firstName: 'Cyd', labels: ['contractor'] },
      {
        internalId: 3n,
        id: 103n,
        firstName: 'Dan',
        labels: ['active', 'contractor'],
      },
      { internalId: 4n, id: 104n, firstName: 'Eve', labels: [] },
    ]);
  });

  it('reports vertex collection sizes', async () => {
    const vertices = await VerticesCollection.make(graphInfo, 'person');
    const activeVertices = await VerticesCollection.verticesWithLabel(
      'active',
      vertices,
    );
    const contractorVertices =
      await VerticesCollection.verticesWithMultipleLabels(
        ['active', 'contractor'],
        vertices,
      );

    expect(vertices.size()).toBe(5n);
    expect(activeVertices.size()).toBe(3n);
    expect(contractorVertices.size()).toBe(1n);
  });

  it('reports vertex iterator end positions', async () => {
    const vertices = await VerticesCollection.make(graphInfo, 'person');
    const begin = await vertices.getIterator();
    const end = await vertices.getEndIterator();

    expect(begin.isEnd()).toBe(false);
    expect(end.isEnd()).toBe(true);

    const visitedIds = [];
    for (const vertex of begin) {
      visitedIds.push(vertex.id());
    }

    expect(visitedIds).toEqual([0n, 1n, 2n, 3n, 4n]);
    expect(begin.isEnd()).toBe(true);
  });

  it('compares vertex iterators by logical offset', async () => {
    const vertices = await VerticesCollection.make(graphInfo, 'person');
    const left = await vertices.getIterator();
    const right = await vertices.getIterator();
    const end = await vertices.getEndIterator();

    expect(left.equals(right)).toBe(true);
    expect(left.notEquals(right)).toBe(false);
    expect(left.equals(end)).toBe(false);
    expect(left.notEquals(end)).toBe(true);

    const iterator = left[Symbol.iterator]();
    iterator.next();

    expect(left.equals(right)).toBe(false);

    for (const _vertex of left) {
      // Consume the remaining vertices.
    }

    expect(left.equals(end)).toBe(true);
  });

  it('advances vertex iterators by logical offset', async () => {
    const vertices = await VerticesCollection.make(graphInfo, 'person');
    const iterator = await vertices.getIterator();
    const end = await vertices.getEndIterator();

    expect(iterator.advance()).toBe(iterator);
    expect(iterator.equals(end)).toBe(false);

    const ids = [];
    for (const vertex of iterator) {
      ids.push(vertex.id());
    }

    expect(ids).toEqual([1n, 2n, 3n, 4n]);
    expect(iterator.equals(end)).toBe(true);

    const skipped = await vertices.getIterator();
    skipped.advance(5n);

    expect(skipped.isEnd()).toBe(true);
    expect(skipped.equals(end)).toBe(true);
  });

  it('reads current vertex values through vertex iterators', async () => {
    const vertices = await VerticesCollection.make(graphInfo, 'person');
    const iterator = await vertices.getIterator();

    expect(iterator.id()).toBe(0n);
    expect(await iterator.property('id')).toBe(100n);
    expect(await iterator.property('firstName')).toBe('Ann');
    expect(await iterator.label()).toEqual(['active', 'engineer']);
    expect(await iterator.hasLabel('engineer')).toBe(true);

    iterator.advance(3n);

    expect(iterator.id()).toBe(3n);
    expect(await iterator.property('id')).toBe(103n);
    expect(await iterator.property('firstName')).toBe('Dan');
    expect(await iterator.label()).toEqual(['active', 'contractor']);
    expect(await iterator.hasLabel('engineer')).toBe(false);
  });

  it('rejects current vertex reads at the end iterator', async () => {
    const vertices = await VerticesCollection.make(graphInfo, 'person');
    const end = await vertices.getEndIterator();

    expect(() => end.id()).toThrow(/Vertex iterator is at end/);
    await expect(end.property('id')).rejects.toThrow(
      /Vertex iterator is at end/,
    );
    await expect(end.label()).rejects.toThrow(/Vertex iterator is at end/);
    await expect(end.hasLabel('active')).rejects.toThrow(
      /Vertex iterator is at end/,
    );
  });

  it('reports filtered vertex iterator end positions', async () => {
    const vertices = await VerticesCollection.make(graphInfo, 'person');
    const activeVertices = await VerticesCollection.verticesWithLabel(
      'active',
      vertices,
    );
    const emptyVertices = await VerticesCollection.verticesWithProperty(
      'firstName',
      {
        op: 'eq',
        column: 'firstName',
        value: 'NoMatch',
      },
      vertices,
    );

    const activeBegin = await activeVertices.getIterator();
    const activeEnd = await activeVertices.getEndIterator();
    const emptyBegin = await emptyVertices.getIterator();
    const emptyEnd = await emptyVertices.getEndIterator();

    expect(activeBegin.isEnd()).toBe(false);
    expect(activeEnd.isEnd()).toBe(true);
    expect(emptyBegin.isEnd()).toBe(true);
    expect(emptyEnd.isEnd()).toBe(true);

    const activeIds = [];
    for (const vertex of activeBegin) {
      activeIds.push(vertex.id());
    }

    expect(activeIds).toEqual([0n, 1n, 3n]);
    expect(activeBegin.isEnd()).toBe(true);
  });

  it('compares filtered vertex iterators by filtered offset', async () => {
    const vertices = await VerticesCollection.make(graphInfo, 'person');
    const activeVertices = await VerticesCollection.verticesWithLabel(
      'active',
      vertices,
    );
    const left = await activeVertices.getIterator();
    const right = await activeVertices.getIterator();
    const end = await activeVertices.getEndIterator();

    expect(left.equals(right)).toBe(true);
    expect(left.equals(end)).toBe(false);

    const iterator = left[Symbol.iterator]();
    iterator.next();

    expect(left.equals(right)).toBe(false);

    for (const _vertex of left) {
      // Consume the remaining filtered vertices.
    }

    expect(left.equals(end)).toBe(true);
  });

  it('advances filtered vertex iterators by filtered offset', async () => {
    const vertices = await VerticesCollection.make(graphInfo, 'person');
    const activeVertices = await VerticesCollection.verticesWithLabel(
      'active',
      vertices,
    );
    const iterator = await activeVertices.getIterator();
    const end = await activeVertices.getEndIterator();

    iterator.advance(2n);

    const ids = [];
    for (const vertex of iterator) {
      ids.push(vertex.id());
    }

    expect(ids).toEqual([3n]);
    expect(iterator.equals(end)).toBe(true);

    const skipped = await activeVertices.getIterator();
    skipped.advance(activeVertices.size());

    expect(skipped.isEnd()).toBe(true);
    expect(skipped.equals(end)).toBe(true);
  });

  it('reads current filtered vertex values through vertex iterators', async () => {
    const vertices = await VerticesCollection.make(graphInfo, 'person');
    const activeVertices = await VerticesCollection.verticesWithLabel(
      'active',
      vertices,
    );
    const iterator = await activeVertices.getIterator();

    expect(iterator.id()).toBe(0n);
    expect(await iterator.property('firstName')).toBe('Ann');

    iterator.advance(2n);

    expect(iterator.id()).toBe(3n);
    expect(await iterator.property('id')).toBe(103n);
    expect(await iterator.label()).toEqual(['active', 'contractor']);
    expect(await iterator.hasLabel('contractor')).toBe(true);
  });

  it('finds vertices by internal id', async () => {
    const vertices = await VerticesCollection.make(graphInfo, 'person');
    const vertex = await vertices.find(3);

    expect(vertex.id()).toBe(3n);
    expect(await vertex.property('id')).toBe(103n);
    expect(await vertex.property('firstName')).toBe('Dan');
    expect(await vertex.label()).toEqual(['active', 'contractor']);
  });

  it('finds first and last vertices by internal id', async () => {
    const vertices = await VerticesCollection.make(graphInfo, 'person');
    const firstVertex = await vertices.find(0);
    const lastVertex = await vertices.find(4n);

    expect(firstVertex.id()).toBe(0n);
    expect(await firstVertex.property('id')).toBe(100n);
    expect(await firstVertex.property('firstName')).toBe('Ann');

    expect(lastVertex.id()).toBe(4n);
    expect(await lastVertex.property('id')).toBe(104n);
    expect(await lastVertex.property('firstName')).toBe('Eve');
  });

  it('rejects vertex find ids outside the internal id range', async () => {
    const vertices = await VerticesCollection.make(graphInfo, 'person');

    await expect(vertices.find(-1)).rejects.toThrow(
      /Internal vertex id -1 is out of range: \[0, 5\)/,
    );
    await expect(vertices.find(5)).rejects.toThrow(
      /Internal vertex id 5 is out of range: \[0, 5\)/,
    );
  });

  it('uses internal ids for find on filtered vertex collections', async () => {
    const activeVertices = await VerticesCollection.verticesWithLabel(
      'active',
      graphInfo,
      'person',
    );
    const vertex = await activeVertices.find(4);

    expect(vertex.id()).toBe(4n);
    expect(await vertex.property('id')).toBe(104n);
    expect(await vertex.label()).toEqual([]);
  });

  it('rejects filtered vertex find ids outside the internal id range', async () => {
    const activeVertices = await VerticesCollection.verticesWithLabel(
      'active',
      graphInfo,
      'person',
    );

    await expect(activeVertices.find(-1n)).rejects.toThrow(
      /Internal vertex id -1 is out of range: \[0, 5\)/,
    );
    await expect(activeVertices.find(5n)).rejects.toThrow(
      /Internal vertex id 5 is out of range: \[0, 5\)/,
    );
  });

  it('reads vertex labels from label chunks', async () => {
    const vertices = await VerticesCollection.make(graphInfo, 'person');
    const iterator = await vertices.getIterator();
    const checks = [];

    for (const vertex of iterator) {
      checks.push({
        labels: await vertex.label(),
        hasActive: await vertex.hasLabel('active'),
        hasEngineer: await vertex.hasLabel('engineer'),
        hasContractor: await vertex.hasLabel('contractor'),
      });
    }

    expect(checks).toEqual([
      {
        labels: ['active', 'engineer'],
        hasActive: true,
        hasEngineer: true,
        hasContractor: false,
      },
      {
        labels: ['active'],
        hasActive: true,
        hasEngineer: false,
        hasContractor: false,
      },
      {
        labels: ['contractor'],
        hasActive: false,
        hasEngineer: false,
        hasContractor: true,
      },
      {
        labels: ['active', 'contractor'],
        hasActive: true,
        hasEngineer: false,
        hasContractor: true,
      },
      {
        labels: [],
        hasActive: false,
        hasEngineer: false,
        hasContractor: false,
      },
    ]);
  });

  it('filters vertices by label and property', async () => {
    const activeVertices = await VerticesCollection.verticesWithLabel(
      'active',
      graphInfo,
      'person',
    );
    const activeIterator = await activeVertices.getIterator();
    const activeIds = [];
    for (const vertex of activeIterator) {
      activeIds.push(await vertex.property('id'));
    }

    const contractorVertices =
      await VerticesCollection.verticesWithMultipleLabels(
        ['active', 'contractor'],
        graphInfo,
        'person',
      );
    const contractorIterator = await contractorVertices.getIterator();
    const contractorIds = [];
    for (const vertex of contractorIterator) {
      contractorIds.push(await vertex.property('id'));
    }

    const namedVertices = await VerticesCollection.verticesWithProperty(
      'firstName',
      {
        op: 'eq',
        column: 'firstName',
        value: 'Dan',
      },
      graphInfo,
      'person',
    );
    const namedIterator = await namedVertices.getIterator();
    const namedIds = [];
    for (const vertex of namedIterator) {
      namedIds.push(await vertex.property('id'));
    }

    expect(activeIds).toEqual([100n, 101n, 103n]);
    expect(contractorIds).toEqual([103n]);
    expect(namedIds).toEqual([103n]);
  });

  it('resolves ordered adjacency offset ranges from the fixture offset tables', async () => {
    const edgeInfo = graphInfo.getEdgeInfo('person', 'knows', 'person');

    await expect(
      getAdjListOffsetOfVertex(
        graphInfo.prefix,
        edgeInfo,
        AdjListType.ORDERED_BY_SOURCE,
        0n,
      ),
    ).resolves.toEqual([0n, 2n]);
    await expect(
      getAdjListOffsetOfVertex(
        graphInfo.prefix,
        edgeInfo,
        AdjListType.ORDERED_BY_SOURCE,
        1n,
      ),
    ).resolves.toEqual([2n, 3n]);
    await expect(
      getAdjListOffsetOfVertex(
        graphInfo.prefix,
        edgeInfo,
        AdjListType.ORDERED_BY_SOURCE,
        2n,
      ),
    ).resolves.toEqual([0n, 1n]);
    await expect(
      getAdjListOffsetOfVertex(
        graphInfo.prefix,
        edgeInfo,
        AdjListType.ORDERED_BY_SOURCE,
        3n,
      ),
    ).resolves.toEqual([1n, 2n]);
    await expect(
      getAdjListOffsetOfVertex(
        graphInfo.prefix,
        edgeInfo,
        AdjListType.ORDERED_BY_SOURCE,
        4n,
      ),
    ).resolves.toEqual([0n, 1n]);

    await expect(
      getAdjListOffsetOfVertex(
        graphInfo.prefix,
        edgeInfo,
        AdjListType.ORDERED_BY_DEST,
        0n,
      ),
    ).resolves.toEqual([0n, 2n]);
    await expect(
      getAdjListOffsetOfVertex(
        graphInfo.prefix,
        edgeInfo,
        AdjListType.ORDERED_BY_DEST,
        1n,
      ),
    ).resolves.toEqual([2n, 3n]);
    await expect(
      getAdjListOffsetOfVertex(
        graphInfo.prefix,
        edgeInfo,
        AdjListType.ORDERED_BY_DEST,
        2n,
      ),
    ).resolves.toEqual([0n, 1n]);
    await expect(
      getAdjListOffsetOfVertex(
        graphInfo.prefix,
        edgeInfo,
        AdjListType.ORDERED_BY_DEST,
        3n,
      ),
    ).resolves.toEqual([1n, 2n]);
    await expect(
      getAdjListOffsetOfVertex(
        graphInfo.prefix,
        edgeInfo,
        AdjListType.ORDERED_BY_DEST,
        4n,
      ),
    ).resolves.toEqual([0n, 1n]);
  });

  it.each([
    [
      AdjListType.ORDERED_BY_SOURCE,
      [
        { src: 0n, dst: 1n, creationDate: '2020-01-01' },
        { src: 0n, dst: 2n, creationDate: '2020-01-02' },
        { src: 1n, dst: 3n, creationDate: '2020-01-03' },
        { src: 2n, dst: 0n, creationDate: '2020-01-04' },
        { src: 3n, dst: 4n, creationDate: '2020-01-05' },
        { src: 4n, dst: 0n, creationDate: '2020-01-06' },
      ],
    ],
    [
      AdjListType.ORDERED_BY_DEST,
      [
        { src: 2n, dst: 0n, creationDate: '2020-01-04' },
        { src: 4n, dst: 0n, creationDate: '2020-01-06' },
        { src: 0n, dst: 1n, creationDate: '2020-01-01' },
        { src: 0n, dst: 2n, creationDate: '2020-01-02' },
        { src: 1n, dst: 3n, creationDate: '2020-01-03' },
        { src: 3n, dst: 4n, creationDate: '2020-01-05' },
      ],
    ],
    [
      AdjListType.UNORDERED_BY_SOURCE,
      [
        { src: 0n, dst: 1n, creationDate: '2020-01-01' },
        { src: 1n, dst: 3n, creationDate: '2020-01-03' },
        { src: 0n, dst: 2n, creationDate: '2020-01-02' },
        { src: 3n, dst: 4n, creationDate: '2020-01-05' },
        { src: 2n, dst: 0n, creationDate: '2020-01-04' },
        { src: 4n, dst: 0n, creationDate: '2020-01-06' },
      ],
    ],
    [
      AdjListType.UNORDERED_BY_DEST,
      [
        { src: 4n, dst: 0n, creationDate: '2020-01-06' },
        { src: 0n, dst: 1n, creationDate: '2020-01-01' },
        { src: 2n, dst: 0n, creationDate: '2020-01-04' },
        { src: 1n, dst: 3n, creationDate: '2020-01-03' },
        { src: 0n, dst: 2n, creationDate: '2020-01-02' },
        { src: 3n, dst: 4n, creationDate: '2020-01-05' },
      ],
    ],
  ])('iterates %s edges from real adjacency chunks', async (adjListType, expected) => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      adjListType,
    );

    expect(edges.edgeNum).toBe(6n);
    expect(edges.indexConverter.edgeChunkNums).toEqual([2n, 1n, 1n]);
    expect(await collectEdges(edges)).toEqual(
      expected.map(({ src, dst }) => [src, dst]),
    );
    expect(await collectEdgesWithProperties(edges)).toEqual(expected);
  });

  it('reports edge collection sizes', async () => {
    for (const adjListType of [
      AdjListType.ORDERED_BY_SOURCE,
      AdjListType.ORDERED_BY_DEST,
      AdjListType.UNORDERED_BY_SOURCE,
      AdjListType.UNORDERED_BY_DEST,
    ]) {
      const edges = await EdgesCollection.make(
        graphInfo,
        'person',
        'knows',
        'person',
        adjListType,
      );
      const partialEdges = await EdgesCollection.make(
        graphInfo,
        'person',
        'knows',
        'person',
        adjListType,
        1n,
        2n,
      );

      expect(edges.size()).toBe(6n);
      expect(partialEdges.size()).toBe(2n);
    }
  });

  it('allows empty edge collections for empty vertex chunk ranges', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_SOURCE,
      1n,
      1n,
    );
    const begin = await edges.getIterator();
    const end = await edges.getEndIterator();

    expect(edges.size()).toBe(0n);
    expect(begin.isEnd()).toBe(true);
    expect(begin.equals(end)).toBe(true);
  });

  it.each([
    ['negative begin', -1n, 1n, /must be non-negative/],
    ['negative end', 0n, -1n, /must be non-negative/],
    ['end before begin', 2n, 1n, /begin must be less than or equal to end/],
    ['end past vertex chunks', 0n, 4n, /is out of range: \[0, 3\)/],
    ['begin past vertex chunks', 4n, 4n, /is out of range: \[0, 3\)/],
  ])(
    'rejects invalid edge collection vertex chunk ranges: %s',
    async (_caseName, vertexChunkBegin, vertexChunkEnd, expectedError) => {
      await expect(
        EdgesCollection.make(
          graphInfo,
          'person',
          'knows',
          'person',
          AdjListType.ORDERED_BY_SOURCE,
          vertexChunkBegin,
          vertexChunkEnd,
        ),
      ).rejects.toThrow(expectedError);
    },
  );

  it('compares edge iterators by chunk, offset, and adjacency type', async () => {
    const orderedBySourceEdges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_SOURCE,
    );
    const orderedByDestEdges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_DEST,
    );
    const begin = await orderedBySourceEdges.getIterator();
    const clonedBegin = await begin.clone();
    const end = await orderedBySourceEdges.getEndIterator();
    const otherAdjListBegin = await orderedByDestEdges.getIterator();

    expect(begin.equals(clonedBegin)).toBe(true);
    expect(begin.notEquals(clonedBegin)).toBe(false);
    expect(begin.equals(end)).toBe(false);
    expect(begin.notEquals(end)).toBe(true);
    expect(begin.equals(otherAdjListBegin)).toBe(false);

    await clonedBegin.advance();

    expect(begin.equals(clonedBegin)).toBe(false);
  });

  it('rejects current edge reads at the end iterator', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_SOURCE,
    );
    const end = await edges.getEndIterator();

    await expect(end.source()).rejects.toThrow(/Edge iterator is at end/);
    await expect(end.destination()).rejects.toThrow(/Edge iterator is at end/);
    await expect(end.property('creationDate')).rejects.toThrow(
      /Edge iterator is at end/,
    );
  });

  it('rejects current edge reads from empty edge collections', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_SOURCE,
      1n,
      1n,
    );
    const begin = await edges.getIterator();

    expect(begin.isEnd()).toBe(true);
    await expect(begin.source()).rejects.toThrow(/Edge iterator is at end/);
    await expect(begin.destination()).rejects.toThrow(/Edge iterator is at end/);
    await expect(begin.property('creationDate')).rejects.toThrow(
      /Edge iterator is at end/,
    );
  });

  it('returns false when advancing edge iterators to or from end', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_SOURCE,
    );
    const iterator = await edges.getIterator();
    const end = await edges.getEndIterator();

    expect(iterator.isEnd()).toBe(false);
    expect(await iterator.source()).toBe(0n);

    for (let i = 0; i < 5; i++) {
      expect(await iterator.advance()).toBe(true);
      expect(iterator.isEnd()).toBe(false);
    }

    expect(await iterator.source()).toBe(4n);
    expect(await iterator.destination()).toBe(0n);
    expect(await iterator.advance()).toBe(false);
    expect(iterator.isEnd()).toBe(true);
    expect(iterator.equals(end)).toBe(true);
    expect(await iterator.advance()).toBe(false);
    expect(iterator.equals(end)).toBe(true);
  });

  it('returns false when advancing edge iterators past partial collection ends', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_SOURCE,
      0n,
      1n,
    );
    const iterator = await edges.getIterator();
    const end = await edges.getEndIterator();

    expect(edges.size()).toBe(3n);
    expect(await iterator.source()).toBe(0n);
    expect(await iterator.advance()).toBe(true);
    expect(await iterator.source()).toBe(0n);
    expect(await iterator.advance()).toBe(true);
    expect(await iterator.source()).toBe(1n);
    expect(await iterator.advance()).toBe(false);
    expect(iterator.isEnd()).toBe(true);
    expect(iterator.equals(end)).toBe(true);
  });

  it('returns false when advancing empty edge collections', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_SOURCE,
      1n,
      1n,
    );
    const begin = await edges.getIterator();
    const end = await edges.getEndIterator();

    expect(begin.isEnd()).toBe(true);
    expect(await begin.advance()).toBe(false);
    expect(begin.equals(end)).toBe(true);
  });

  it('rejects unknown edge properties', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_SOURCE,
    );

    const iterator = await edges.getIterator();
    for await (const edge of iterator) {
      await expect(edge.property('missingProperty')).rejects.toThrow(
        /Edge property missingProperty not found in edge info/,
      );
      break;
    }
  });

  it.each([
    AdjListType.ORDERED_BY_SOURCE,
    AdjListType.ORDERED_BY_DEST,
    AdjListType.UNORDERED_BY_SOURCE,
    AdjListType.UNORDERED_BY_DEST,
  ])('returns end for out-of-range edge find ids in %s collections', async (adjListType) => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      adjListType,
    );
    const begin = await edges.getIterator();

    const negativeSrc = await edges.findSrc(-1n, begin);
    const negativeDst = await edges.findDst(-1n, begin);
    const pastEndSrc = await edges.findSrc(5n, begin);
    const pastEndDst = await edges.findDst(5n, begin);

    expect(negativeSrc.isEnd()).toBe(true);
    expect(negativeDst.isEnd()).toBe(true);
    expect(pastEndSrc.isEnd()).toBe(true);
    expect(pastEndDst.isEnd()).toBe(true);
  });

  it('tracks consecutive sources with ordered_by_source edge iterators', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_SOURCE,
    );
    const begin = await edges.getIterator();
    const seeker = await begin.clone();

    expect(await seeker.firstSrc(begin, 0n)).toBe(true);
    expect([await seeker.source(), await seeker.destination()]).toEqual([0n, 1n]);
    expect(await seeker.nextSrc()).toBe(true);
    expect([await seeker.source(), await seeker.destination()]).toEqual([0n, 2n]);
    expect(await seeker.nextSrc()).toBe(false);
  });

  it('tracks consecutive destinations with ordered_by_dest edge iterators', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_DEST,
    );
    const begin = await edges.getIterator();
    const seeker = await begin.clone();

    expect(await seeker.firstDst(begin, 0n)).toBe(true);
    expect([await seeker.source(), await seeker.destination()]).toEqual([2n, 0n]);
    expect(await seeker.nextDst()).toBe(true);
    expect([await seeker.source(), await seeker.destination()]).toEqual([4n, 0n]);
    expect(await seeker.nextDst()).toBe(false);
  });

  it('scans within the aligned source chunk for unordered_by_source iterators', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.UNORDERED_BY_SOURCE,
    );
    const begin = await edges.getIterator();
    const seeker = await begin.clone();

    expect(await seeker.firstSrc(begin, 0n)).toBe(true);
    expect([await seeker.source(), await seeker.destination()]).toEqual([0n, 1n]);
    expect(await seeker.nextSrc()).toBe(true);
    expect([await seeker.source(), await seeker.destination()]).toEqual([0n, 2n]);
    expect(await seeker.nextSrc()).toBe(false);
  });

  it('scans within the aligned destination chunk for unordered_by_dest iterators', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.UNORDERED_BY_DEST,
    );
    const begin = await edges.getIterator();
    const seeker = await begin.clone();

    expect(await seeker.firstDst(begin, 0n)).toBe(true);
    expect([await seeker.source(), await seeker.destination()]).toEqual([4n, 0n]);
    expect(await seeker.nextDst()).toBe(true);
    expect([await seeker.source(), await seeker.destination()]).toEqual([2n, 0n]);
    expect(await seeker.nextDst()).toBe(false);
  });

  it('finds outgoing edges from a given iterator in ordered_by_source collections', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_SOURCE,
    );
    const begin = await edges.getIterator();
    const found = await edges.findSrc(0n, begin);

    expect(found.isEnd()).toBe(false);
    expect([await found.source(), await found.destination()]).toEqual([0n, 1n]);

    const afterFirst = await found.clone();
    await afterFirst.advance();
    const foundAgain = await edges.findSrc(0n, afterFirst);
    expect(foundAgain.isEnd()).toBe(false);
    expect([await foundAgain.source(), await foundAgain.destination()]).toEqual([
      0n,
      2n,
    ]);

    const missing = await edges.findSrc(99n, begin);
    expect(missing.isEnd()).toBe(true);
  });

  it('uses ordered_by_source offsets that land in the second edge chunk of a partition', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_SOURCE,
    );
    const begin = await edges.getIterator();
    const found = await edges.findSrc(1n, begin);

    expect(found.isEnd()).toBe(false);
    expect(found.globalChunkIndex).toBe(1n);
    expect(found.curOffset).toBe(2n);
    expect([await found.source(), await found.destination()]).toEqual([1n, 3n]);
    expect(await found.property('creationDate')).toBe('2020-01-03');
    expect(await found.nextSrc()).toBe(false);
  });

  it('finds incoming edges from a given iterator in ordered_by_dest collections', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_DEST,
    );
    const begin = await edges.getIterator();
    const found = await edges.findDst(0n, begin);

    expect(found.isEnd()).toBe(false);
    expect([await found.source(), await found.destination()]).toEqual([2n, 0n]);

    const afterFirst = await found.clone();
    await afterFirst.advance();
    const foundAgain = await edges.findDst(0n, afterFirst);
    expect(foundAgain.isEnd()).toBe(false);
    expect([await foundAgain.source(), await foundAgain.destination()]).toEqual([
      4n,
      0n,
    ]);

    const missing = await edges.findDst(99n, begin);
    expect(missing.isEnd()).toBe(true);
  });

  it('uses ordered_by_dest offsets that land in the second edge chunk of a partition', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_DEST,
    );
    const begin = await edges.getIterator();
    const found = await edges.findDst(1n, begin);

    expect(found.isEnd()).toBe(false);
    expect(found.globalChunkIndex).toBe(1n);
    expect(found.curOffset).toBe(2n);
    expect([await found.source(), await found.destination()]).toEqual([0n, 1n]);
    expect(await found.property('creationDate')).toBe('2020-01-01');
    expect(await found.nextDst()).toBe(false);
  });

  it('finds edges by source in unordered_by_source collections', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.UNORDERED_BY_SOURCE,
    );
    const begin = await edges.getIterator();
    const found = await edges.findSrc(0n, begin);

    expect(found.isEnd()).toBe(false);
    expect([await found.source(), await found.destination()]).toEqual([0n, 1n]);

    const afterFirst = await found.clone();
    await afterFirst.advance();
    const foundAgain = await edges.findSrc(0n, afterFirst);
    expect(foundAgain.isEnd()).toBe(false);
    expect([await foundAgain.source(), await foundAgain.destination()]).toEqual([
      0n,
      2n,
    ]);
  });

  it('finds edges by destination in unordered_by_dest collections', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.UNORDERED_BY_DEST,
    );
    const begin = await edges.getIterator();
    const found = await edges.findDst(0n, begin);

    expect(found.isEnd()).toBe(false);
    expect([await found.source(), await found.destination()]).toEqual([4n, 0n]);

    const afterFirst = await found.clone();
    await afterFirst.advance();
    const foundAgain = await edges.findDst(0n, afterFirst);
    expect(foundAgain.isEnd()).toBe(false);
    expect([await foundAgain.source(), await foundAgain.destination()]).toEqual([
      2n,
      0n,
    ]);
  });

  it('limits ordered_by_source searches to the requested vertex chunk range', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_SOURCE,
      1n,
      2n,
    );
    const begin = await edges.getIterator();

    expect(edges.edgeNum).toBe(2n);
    expect(begin.isEnd()).toBe(false);
    expect([await begin.source(), await begin.destination()]).toEqual([2n, 0n]);

    const foundInRange = await edges.findSrc(2n, begin);
    expect(foundInRange.isEnd()).toBe(false);
    expect([await foundInRange.source(), await foundInRange.destination()]).toEqual([
      2n,
      0n,
    ]);

    const foundDstInRange = await edges.findDst(4n, begin);
    expect(foundDstInRange.isEnd()).toBe(false);
    expect([
      await foundDstInRange.source(),
      await foundDstInRange.destination(),
    ]).toEqual([3n, 4n]);

    const srcOutOfRange = await edges.findSrc(0n, begin);
    expect(srcOutOfRange.isEnd()).toBe(true);

    const dstOutOfRange = await edges.findDst(1n, begin);
    expect(dstOutOfRange.isEnd()).toBe(true);
  });

  it('limits ordered_by_dest searches to the requested vertex chunk range', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_DEST,
      1n,
      2n,
    );
    const begin = await edges.getIterator();

    expect(edges.edgeNum).toBe(2n);
    expect(begin.isEnd()).toBe(false);
    expect([await begin.source(), await begin.destination()]).toEqual([0n, 2n]);

    const foundDstInRange = await edges.findDst(2n, begin);
    expect(foundDstInRange.isEnd()).toBe(false);
    expect([
      await foundDstInRange.source(),
      await foundDstInRange.destination(),
    ]).toEqual([0n, 2n]);

    const foundSrcInRange = await edges.findSrc(1n, begin);
    expect(foundSrcInRange.isEnd()).toBe(false);
    expect([
      await foundSrcInRange.source(),
      await foundSrcInRange.destination(),
    ]).toEqual([1n, 3n]);

    const dstOutOfRange = await edges.findDst(0n, begin);
    expect(dstOutOfRange.isEnd()).toBe(true);

    const srcOutOfRange = await edges.findSrc(4n, begin);
    expect(srcOutOfRange.isEnd()).toBe(true);
  });

  it('limits unordered_by_source searches to the requested vertex chunk range', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.UNORDERED_BY_SOURCE,
      1n,
      2n,
    );
    const begin = await edges.getIterator();

    expect(edges.edgeNum).toBe(2n);
    expect(begin.isEnd()).toBe(false);
    expect([await begin.source(), await begin.destination()]).toEqual([3n, 4n]);

    const foundSrcInRange = await edges.findSrc(2n, begin);
    expect(foundSrcInRange.isEnd()).toBe(false);
    expect([
      await foundSrcInRange.source(),
      await foundSrcInRange.destination(),
    ]).toEqual([2n, 0n]);

    const foundDstInRange = await edges.findDst(4n, begin);
    expect(foundDstInRange.isEnd()).toBe(false);
    expect([
      await foundDstInRange.source(),
      await foundDstInRange.destination(),
    ]).toEqual([3n, 4n]);

    const srcOutOfRange = await edges.findSrc(0n, begin);
    expect(srcOutOfRange.isEnd()).toBe(true);

    const dstOutOfRange = await edges.findDst(1n, begin);
    expect(dstOutOfRange.isEnd()).toBe(true);
  });

  it('limits unordered_by_dest searches to the requested vertex chunk range', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.UNORDERED_BY_DEST,
      1n,
      2n,
    );
    const begin = await edges.getIterator();

    expect(edges.edgeNum).toBe(2n);
    expect(begin.isEnd()).toBe(false);
    expect([await begin.source(), await begin.destination()]).toEqual([1n, 3n]);

    const foundDstInRange = await edges.findDst(2n, begin);
    expect(foundDstInRange.isEnd()).toBe(false);
    expect([
      await foundDstInRange.source(),
      await foundDstInRange.destination(),
    ]).toEqual([0n, 2n]);

    const foundSrcInRange = await edges.findSrc(1n, begin);
    expect(foundSrcInRange.isEnd()).toBe(false);
    expect([
      await foundSrcInRange.source(),
      await foundSrcInRange.destination(),
    ]).toEqual([1n, 3n]);

    const dstOutOfRange = await edges.findDst(0n, begin);
    expect(dstOutOfRange.isEnd()).toBe(true);

    const srcOutOfRange = await edges.findSrc(4n, begin);
    expect(srcOutOfRange.isEnd()).toBe(true);
  });

  it('stops ordered_by_source nextSrc at the end of a partial collection', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_SOURCE,
      0n,
      1n,
    );
    const begin = await edges.getIterator();
    const seeker = await edges.findSrc(1n, begin);

    expect(seeker.isEnd()).toBe(false);
    expect([await seeker.source(), await seeker.destination()]).toEqual([1n, 3n]);
    expect(await seeker.nextSrc()).toBe(false);
    expect(seeker.isEnd()).toBe(true);
  });

  it('stops ordered_by_dest nextDst at the end of a partial collection', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.ORDERED_BY_DEST,
      0n,
      1n,
    );
    const begin = await edges.getIterator();
    const seeker = await edges.findDst(1n, begin);

    expect(seeker.isEnd()).toBe(false);
    expect([await seeker.source(), await seeker.destination()]).toEqual([0n, 1n]);
    expect(await seeker.nextDst()).toBe(false);
    expect(seeker.isEnd()).toBe(true);
  });

  it('stops unordered_by_source nextSrc at the end of a partial collection', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.UNORDERED_BY_SOURCE,
      0n,
      1n,
    );
    const begin = await edges.getIterator();
    const seeker = await edges.findSrc(0n, begin);

    expect(seeker.isEnd()).toBe(false);
    expect([await seeker.source(), await seeker.destination()]).toEqual([0n, 1n]);
    expect(await seeker.nextSrc()).toBe(true);
    expect([await seeker.source(), await seeker.destination()]).toEqual([0n, 2n]);
    expect(await seeker.nextSrc()).toBe(false);
    expect(seeker.isEnd()).toBe(true);
  });

  it('stops unordered_by_dest nextDst at the end of a partial collection', async () => {
    const edges = await EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      AdjListType.UNORDERED_BY_DEST,
      0n,
      1n,
    );
    const begin = await edges.getIterator();
    const seeker = await edges.findDst(0n, begin);

    expect(seeker.isEnd()).toBe(false);
    expect([await seeker.source(), await seeker.destination()]).toEqual([4n, 0n]);
    expect(await seeker.nextDst()).toBe(true);
    expect([await seeker.source(), await seeker.destination()]).toEqual([2n, 0n]);
    expect(await seeker.nextDst()).toBe(false);
    expect(seeker.isEnd()).toBe(true);
  });
});
