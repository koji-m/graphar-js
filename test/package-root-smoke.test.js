import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { existsSync } from 'node:fs';
import { beforeAll, afterAll, describe, expect, it } from 'vitest';

const rootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const distEntry = path.join(rootDir, 'dist', 'graphar.es.js');
const fixtureDir = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  'fixtures',
  'graphar-minimal',
  'parquet',
);
const wasmPath = path.join(
  rootDir,
  'node_modules',
  'parquet-wasm',
  'esm',
  'parquet_wasm_bg.wasm',
);

function toPosixDirectoryPath(inputPath) {
  return `${inputPath.split(path.sep).join('/')}/`;
}

function createFixtureFetch(rootDir, baseUrl) {
  const rootPrefix = `${baseUrl}`;
  return async function fixtureFetch(input) {
    const requestUrl =
      typeof input === 'string' ? new URL(input) : new URL(input.url);
    const relativePath = decodeURIComponent(requestUrl.pathname.replace(/^\/+/, ''));
    const diskPath = path.join(rootDir, relativePath);
    try {
      const body = await readFile(diskPath);
      if (relativePath === 'ldbc_sample.graph.yml') {
        return new Response(
          body.toString('utf8').replace('prefix: ./', `prefix: ${rootPrefix}`),
          { status: 200, headers: { 'content-type': 'text/yaml' } },
        );
      }
      return new Response(body, {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      });
    } catch {
      return new Response('not found', { status: 404 });
    }
  };
}

async function loadFixtureGraphInfo(GraphInfo, baseUrl) {
  return await GraphInfo.load({
    path: new URL('ldbc_sample.graph.yml', baseUrl).href,
  });
}

describe.skipIf(!existsSync(distEntry))('package-root smoke', () => {
  let pkg;
  let graphInfo;
  let originalFetch;
  const fixtureBaseUrl = 'http://fixture.test/';

  beforeAll(async () => {
    originalFetch = globalThis.fetch;
    globalThis.fetch = createFixtureFetch(fixtureDir, fixtureBaseUrl);
    pkg = await import('graphar-js');
    await pkg.initWasm({ module_or_path: await readFile(wasmPath) });
    graphInfo = await loadFixtureGraphInfo(pkg.GraphInfo, fixtureBaseUrl);
  });

  afterAll(async () => {
    globalThis.fetch = originalFetch;
  });

  it('exposes the intended reader v0 package-root surface', () => {
    expect(pkg.GraphInfo).toBeTypeOf('function');
    expect(pkg.VerticesCollection).toBeTypeOf('function');
    expect(pkg.EdgesCollection).toBeTypeOf('function');
    expect(pkg.AdjListType).toBeTypeOf('object');
    expect(pkg.initWasm).toBeTypeOf('function');
    expect(pkg._Property).toBeTypeOf('function');
    expect('fileSystemFromUriOrPath' in pkg).toBe(false);
  });

  it('loads the fixture graph through the built package entry', async () => {
    const vertices = await pkg.VerticesCollection.make(graphInfo, 'person');
    const vertex = await vertices.find(3n);
    const edges = await pkg.EdgesCollection.make(
      graphInfo,
      'person',
      'knows',
      'person',
      pkg.AdjListType.ORDERED_BY_SOURCE,
    );
    const edgeIter = await edges.findSrc(0n, await edges.getIterator());

    expect(graphInfo.graphName).toBe('ldbc_sample');
    expect(await vertex.property('id')).toBe(103n);
    expect(await vertex.property('firstName')).toBe('Dan');
    expect(await vertex.isValid('firstName')).toBe(true);
    expect(await edgeIter.source()).toBe(0n);
    expect(await edgeIter.destination()).toBe(1n);
    expect(await edgeIter.property('creationDate')).toBe('2020-01-01');
    expect(await edgeIter.isValid('creationDate')).toBe(true);
  });
});
