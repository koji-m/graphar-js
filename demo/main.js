import {
  AdjListType,
  EdgesCollection,
  GraphInfo,
  initWasm,
  VerticesCollection,
} from '../src/index.js';

const path = 'http://localhost:9000/my-bucket/parquet/ldbc_sample.graph.yml';

await initWasm();

document.querySelector('#open').addEventListener('click', async (_e) => {
  const graphInfo = await GraphInfo.load({ path });

  const vertices = await VerticesCollection.make(graphInfo, 'person');
  const vertexIterator = await vertices.getIterator();
  const outVertices = [];
  for (const vertex of vertexIterator) {
    outVertices.push(
      `id: ${await vertex.property('id')}, firstName: ${await vertex.property('firstName')}, lastName: ${await vertex.property('lastName')}, gender: ${await vertex.property('gender')}`,
    );
  }

  const edges = await EdgesCollection.make(
    graphInfo,
    'person',
    'knows',
    'person',
    AdjListType.ORDERED_BY_SOURCE,
  );
  const edgeIterator = await edges.getIterator();
  const outEdges = [];
  for await (const edge of edgeIterator) {
    outEdges.push(
      `src: ${await edge.source()}, dst: ${await edge.destination()}`,
    );
  }

  let output = `
Name: ${graphInfo.graphName}
Prefix: ${graphInfo.prefix}
Version: ${graphInfo.version.versionStr}
Number of VertexInfos: ${graphInfo.vertexInfos[0].propertyGroups[0].properties[0].propertyName}
Number of EdgeInfos: ${graphInfo.edgeInfos.length}
Labels: ${graphInfo.labels.join(', ')}`;

  output += `
Edges:
${outEdges.join('\n')}`;

  document.querySelector('#app').innerHTML = output;
});
