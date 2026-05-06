import * as graphar from 'graphar-js';
import {
  AdjListType,
  EdgesCollection,
  GraphInfo,
  VerticesCollection,
  _Equal,
  _Literal,
  _Property,
  initWasm,
} from 'graphar-js';

const statusElement = document.querySelector('#status');

function setResult(result) {
  globalThis.__GRAPHAR_BROWSER_SMOKE__ = result;
  statusElement.textContent = JSON.stringify(result, null, 2);
}

async function main() {
  await initWasm();

  const graphInfo = await GraphInfo.load({
    path: new URL('/test/fixtures/graphar-minimal/parquet/ldbc_sample.graph.yml', window.location.origin).href,
  });
  const vertices = await VerticesCollection.make(graphInfo, 'person');
  const filteredVertices = await VerticesCollection.verticesWithProperty(
    'firstName',
    _Equal(_Property('firstName'), _Literal('Dan')),
    vertices,
  );
  const vertex = await vertices.find(3n);
  const edges = await EdgesCollection.make(
    graphInfo,
    'person',
    'knows',
    'person',
    AdjListType.ORDERED_BY_SOURCE,
  );
  const edgeIter = await edges.findSrc(0n, await edges.getIterator());

  return {
    ok: true,
    exports: {
      hasGraphInfo: typeof GraphInfo === 'function',
      hasVerticesCollection: typeof VerticesCollection === 'function',
      hasEdgesCollection: typeof EdgesCollection === 'function',
      hasAdjListType: typeof AdjListType === 'object',
      hasInitWasm: typeof initWasm === 'function',
      hidesFilesystemHelper: !('fileSystemFromUriOrPath' in graphar),
    },
    graphName: graphInfo.graphName,
    vertexCount: vertices.size().toString(),
    filteredVertexCount: filteredVertices.size().toString(),
    vertex: {
      internalId: vertex.id().toString(),
      id: (await vertex.property('id')).toString(),
      firstName: await vertex.property('firstName'),
      firstNameValid: await vertex.isValid('firstName'),
    },
    edge: {
      source: (await edgeIter.source()).toString(),
      destination: (await edgeIter.destination()).toString(),
      creationDate: await edgeIter.property('creationDate'),
      creationDateValid: await edgeIter.isValid('creationDate'),
    },
  };
}

main()
  .then((result) => {
    setResult(result);
  })
  .catch((error) => {
    const failure = {
      ok: false,
      message: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : '',
    };
    console.error(error);
    setResult(failure);
  });
