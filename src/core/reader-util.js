import { fileSystemFromUriOrPath } from './filesystem.js';
import { AdjListType } from './types.js';

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

export {
  getEdgeNum,
  getVertexChunkNumFromEdge,
  getVertexChunkNumFromVertex,
  getVertexNumFromVertex,
};
