import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { beforeAll, describe, expect, it } from 'vitest';
import { GraphInfo, initWasm, VerticesCollection } from '../src/index.js';

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

function toPosixDirectoryPath(inputPath) {
  return `${inputPath.split(path.sep).join('/')}/`;
}

async function loadLocalFixtureGraphInfo() {
  const input = await readFile(path.join(fixtureDir, 'ldbc_sample.graph.yml'), 'utf8');
  return await GraphInfo.load({
    input: input.replace('prefix: ./', `prefix: ${toPosixDirectoryPath(fixtureDir)}`),
    relativeLocation: toPosixDirectoryPath(fixtureDir),
  });
}

describe('Graph reader vertex fixture integration', () => {
  let graphInfo;

  beforeAll(async () => {
    await initWasm({ module_or_path: await readFile(wasmPath) });
    graphInfo = await loadLocalFixtureGraphInfo();
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
});
