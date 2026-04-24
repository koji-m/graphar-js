import { fileSystemFromUriOrPath } from './filesystem.js';
import { AdjListType } from './types.js';

function toBigIntId(value) {
  if (typeof value === 'bigint') {
    return value;
  }
  if (typeof value === 'number') {
    return BigInt(value);
  }
  throw new Error(`Unsupported offset value type: ${typeof value}`);
}

async function getVertexNumFromVertex(prefix, vertexInfo) {
  const [fs, noUrlPath] = fileSystemFromUriOrPath(prefix);
  const vertexNumFileSuffix = vertexInfo.getVerticesNumFilePath();
  const vertexNumFilePath = noUrlPath + vertexNumFileSuffix;
  return await fs.readFileAsSingleUint64(vertexNumFilePath);
}

async function getVertexChunkNumFromVertex(prefix, vertexInfo) {
  const vertexNum = await getVertexNumFromVertex(prefix, vertexInfo);
  return (
    (vertexNum + BigInt(vertexInfo.chunkSize) - 1n) /
    BigInt(vertexInfo.chunkSize)
  );
}

async function getVertexNumFromEdge(prefix, edgeInfo, adjListType) {
  const [fs, outPrefix] = fileSystemFromUriOrPath(prefix);
  const vertexNumFileSuffix = edgeInfo.getVerticesNumFilePath(adjListType);
  const vertexNumFilePath = outPrefix + vertexNumFileSuffix;
  return await fs.readFileAsSingleUint64(vertexNumFilePath);
}

async function getVertexChunkNumFromEdge(prefix, edgeInfo, adjListType) {
  const vertexNum = await getVertexNumFromEdge(prefix, edgeInfo, adjListType);
  const chunkSize =
    adjListType === AdjListType.ORDERED_BY_SOURCE ||
    adjListType === AdjListType.UNORDERED_BY_SOURCE
      ? edgeInfo.srcChunkSize
      : edgeInfo.dstChunkSize;
  return (vertexNum + BigInt(chunkSize) - 1n) / BigInt(chunkSize);
}

async function getEdgeNum(prefix, edgeInfo, adjListType, vertexChunkIndex) {
  const [fs, outPrefix] = fileSystemFromUriOrPath(prefix);
  const edgeNumFileSuffix = edgeInfo.getEdgesNumFilePath(
    vertexChunkIndex,
    adjListType,
  );
  const edgeNumFilePath = outPrefix + edgeNumFileSuffix;
  return await fs.readFileAsSingleUint64(edgeNumFilePath);
}

async function getAdjListOffsetOfVertex(prefix, edgeInfo, adjListType, vertexId) {
  let vertexChunkSize;
  if (adjListType === AdjListType.ORDERED_BY_SOURCE) {
    vertexChunkSize = edgeInfo.srcChunkSize;
  } else if (adjListType === AdjListType.ORDERED_BY_DEST) {
    vertexChunkSize = edgeInfo.dstChunkSize;
  } else {
    throw new Error(
      `The adj list type has to be ordered_by_source or ordered_by_dest, but got ${adjListType}`,
    );
  }

  const offsetChunkIndex = vertexId / BigInt(vertexChunkSize);
  const offsetInFile = vertexId % BigInt(vertexChunkSize);
  const [fs, outPrefix] = fileSystemFromUriOrPath(prefix);
  const offsetFilePath = edgeInfo.getAdjListOffsetFilePath(
    Number(offsetChunkIndex),
    adjListType,
  );
  const adjacentList = edgeInfo.getAdjacentList(adjListType);
  if (!adjacentList) {
    throw new Error(
      `The adjacent list is not set for adj list type ${adjListType}`,
    );
  }

  const table = await fs.readFileAsTable(
    outPrefix + offsetFilePath,
    adjacentList.fileType,
  );
  const offsetColumn = table.getChildAt?.(0) ?? table.batches[0]?.getChildAt(0);
  if (!offsetColumn) {
    throw new Error(`Offset column not found in file ${outPrefix + offsetFilePath}`);
  }

  const beginOffset = offsetColumn.get(Number(offsetInFile));
  const endOffset = offsetColumn.get(Number(offsetInFile) + 1);
  if (beginOffset === null || beginOffset === undefined) {
    throw new Error(`Begin offset not found for vertex id ${vertexId}`);
  }
  if (endOffset === null || endOffset === undefined) {
    throw new Error(`End offset not found for vertex id ${vertexId}`);
  }

  return [toBigIntId(beginOffset), toBigIntId(endOffset)];
}

export {
  getAdjListOffsetOfVertex,
  getEdgeNum,
  getVertexChunkNumFromEdge,
  getVertexChunkNumFromVertex,
  getVertexNumFromVertex,
};
