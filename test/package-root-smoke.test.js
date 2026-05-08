import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { existsSync } from 'node:fs';
import { beforeAll, describe, expect, it } from 'vitest';

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

async function loadLocalFixtureGraphInfo(GraphInfo) {
  const input = await readFile(path.join(fixtureDir, 'ldbc_sample.graph.yml'), 'utf8');
  return await GraphInfo.load({
    input: input.replace('prefix: ./', `prefix: ${toPosixDirectoryPath(fixtureDir)}`),
    relativeLocation: toPosixDirectoryPath(fixtureDir),
  });
}

describe.skipIf(!existsSync(distEntry))('package-root smoke', () => {
  let pkg;
  let graphInfo;

  beforeAll(async () => {
    pkg = await import('graphar-js');
    await pkg.initWasm({ module_or_path: await readFile(wasmPath) });
    graphInfo = await loadLocalFixtureGraphInfo(pkg.GraphInfo);
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
