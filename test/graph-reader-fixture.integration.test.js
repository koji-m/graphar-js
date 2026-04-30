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

  it('finds vertices by internal id', async () => {
    const vertices = await VerticesCollection.make(graphInfo, 'person');
    const vertex = await vertices.find(3);

    expect(vertex.id()).toBe(3n);
    expect(await vertex.property('id')).toBe(103n);
    expect(await vertex.property('firstName')).toBe('Dan');
    expect(await vertex.label()).toEqual(['active', 'contractor']);
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
});
