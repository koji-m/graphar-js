import { mkdtemp, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { describe, expect, it } from 'vitest';
import { GraphInfo } from '../src/core/graph-info.js';

describe('GraphInfo local filesystem loading', () => {
  it('loads graph metadata from an absolute path when graph prefix is omitted', async () => {
    const tempDir = await mkdtemp(path.join(os.tmpdir(), 'graphar-js-'));
    const graphPath = path.join(tempDir, 'local.graph.yml');

    await writeFile(
      graphPath,
      `name: local_graph
version: gar/v1
vertices: []
edges: []
`,
    );

    const graphInfo = await GraphInfo.load({
      path: graphPath,
    });

    expect(graphInfo.graphName).toBe('local_graph');
    expect(graphInfo.prefix).toBe(`${tempDir}/`);
    expect(graphInfo.vertexInfos).toEqual([]);
    expect(graphInfo.edgeInfos).toEqual([]);
  });

  it('loads graph metadata from a file URI when graph prefix is omitted', async () => {
    const tempDir = await mkdtemp(path.join(os.tmpdir(), 'graphar-js-'));
    const graphPath = path.join(tempDir, 'local.graph.yml');

    await writeFile(
      graphPath,
      `name: local_graph
version: gar/v1
vertices: []
edges: []
`,
    );

    const graphInfo = await GraphInfo.load({
      path: pathToFileURL(graphPath).href,
    });

    expect(graphInfo.graphName).toBe('local_graph');
    expect(graphInfo.prefix).toBe(pathToFileURL(`${tempDir}/`).href);
  });

  it('keeps a relative local graph prefix unchanged, matching the current C++ behavior', async () => {
    const graphInfo = await GraphInfo.load({
      input: `name: local_graph
prefix: ./
version: gar/v1
vertices: []
edges: []
`,
      relativeLocation: '/tmp/graphar/',
    });

    expect(graphInfo.prefix).toBe('./');
  });
});
