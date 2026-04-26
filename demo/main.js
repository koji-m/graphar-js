import {
  AdjListType,
  EdgesCollection,
  GraphInfo,
  initWasm,
  VerticesCollection,
} from '../src/index.js';

const defaultGraphInfoPath =
  'http://localhost:9000/my-bucket/parquet/ldbc_sample.graph.yml';

const graphInfoPathInput = document.querySelector('#graph-info-path');
const openButton = document.querySelector('#open');
const outputElement = document.querySelector('#app');

graphInfoPathInput.value = defaultGraphInfoPath;
outputElement.textContent = 'Initializing parquet reader...';

await initWasm();

function formatVertexInfo(vertexInfo) {
  const propertyGroupSummaries = vertexInfo.propertyGroups.map(
    (propertyGroup, index) =>
      `  [${index}] ${propertyGroup.fileType} ${propertyGroup.properties
        .map((property) => property.name)
        .join(', ')}`,
  );
  return [
    `type=${vertexInfo.type}`,
    `chunkSize=${vertexInfo.chunkSize}`,
    `prefix=${vertexInfo.prefix}`,
    ...propertyGroupSummaries,
  ].join('\n');
}

function formatEdgeInfo(edgeInfo) {
  const adjListSummaries = edgeInfo.adjacentList.map(
    (adjacentList) =>
      `  ${adjacentList.prefix} (${adjacentList.fileType}, ${adjacentList.type})`,
  );
  return [
    `${edgeInfo.srcType} -[${edgeInfo.edgeType}]-> ${edgeInfo.dstType}`,
    `chunkSize=${edgeInfo.chunkSize}`,
    `prefix=${edgeInfo.prefix}`,
    ...adjListSummaries,
  ].join('\n');
}

async function readExampleGraph(graphInfoPath) {
  const graphInfo = await GraphInfo.load({ path: graphInfoPath });

  const vertices = await VerticesCollection.make(graphInfo, 'person');
  const vertexIterator = await vertices.getIterator();
  const vertexLines = [];
  let vertexCount = 0;
  for (const vertex of vertexIterator) {
    vertexLines.push(
      `id=${await vertex.property('id')}, firstName=${await vertex.property('firstName')}, lastName=${await vertex.property('lastName')}, gender=${await vertex.property('gender')}`,
    );
    vertexCount += 1;
    if (vertexCount >= 3) {
      break;
    }
  }

  const edges = await EdgesCollection.make(
    graphInfo,
    'person',
    'knows',
    'person',
    AdjListType.ORDERED_BY_SOURCE,
  );
  const edgeIterator = await edges.getIterator();
  const edgeLines = [];
  let edgeCount = 0;
  for await (const edge of edgeIterator) {
    edgeLines.push(`src=${await edge.source()}, dst=${await edge.destination()}`);
    edgeCount += 1;
    if (edgeCount >= 5) {
      break;
    }
  }

  const output = [
    'Graph',
    `name=${graphInfo.graphName}`,
    `prefix=${graphInfo.prefix}`,
    `version=${graphInfo.version?.versionStr ?? '(none)'}`,
    `vertexInfos=${graphInfo.vertexInfos.length}`,
    `edgeInfos=${graphInfo.edgeInfos.length}`,
    '',
    'Vertex Infos',
    ...graphInfo.vertexInfos.map((vertexInfo) => formatVertexInfo(vertexInfo)),
    '',
    'Edge Infos',
    ...graphInfo.edgeInfos.map((edgeInfo) => formatEdgeInfo(edgeInfo)),
    '',
    'Sample Vertices',
    ...vertexLines,
    '',
    'Sample Edges',
    ...edgeLines,
  ];

  return output.join('\n');
}

openButton.addEventListener('click', async () => {
  const graphInfoPath = graphInfoPathInput.value.trim();
  if (graphInfoPath.length === 0) {
    outputElement.textContent = 'Graph info path is required.';
    return;
  }

  openButton.disabled = true;
  outputElement.textContent = `Loading ${graphInfoPath} ...`;
  try {
    outputElement.textContent = await readExampleGraph(graphInfoPath);
  } catch (error) {
    outputElement.textContent = error instanceof Error ? error.stack : String(error);
  } finally {
    openButton.disabled = false;
  }
});
