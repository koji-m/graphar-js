const MAX_INT64 = (1n << 63n) - 1n;

class IndexConverter {
  constructor(edgeChunkNums) {
    Object.assign(this, { edgeChunkNums });
  }

  indexPairToGlobalChunkIndex(vertexChunkIndex, edgeChunkIndex) {
    let globalEdgeChunkIndex = 0;
    for (let i = 0; i < vertexChunkIndex; i++) {
      globalEdgeChunkIndex += this.edgeChunkNums[i];
    }
    return globalEdgeChunkIndex + edgeChunkIndex;
  }

  globalChunkIndexToIndexPair(globalIndex) {
    let indexPair = [];
    for (let i = 0; i < this.edgeChunkNums.length; i++) {
      if (globalIndex < this.edgeChunkNums[i]) {
        indexPair = [i, globalIndex];
        break;
      }
      globalIndex -= this.edgeChunkNums[i];
    }
    return indexPair;
  }
}

export { IndexConverter, MAX_INT64 };
