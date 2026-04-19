import {
  AdjListArrowChunkReader,
  AdjListOffsetArrowChunkReader,
  AdjListPropertyArrowChunkReader,
  VertexPropertyArrowChunkReader,
} from './chunk-reader.js';
import { fileSystemFromUriOrPath } from './filesystem.js';
import { getVertexChunkNumFromEdge } from './reader-util.js';
import { AdjListType } from './types.js';
import { IndexConverter, MAX_INT64 } from './util.js';

class Vertex {
  constructor({ readers, curOffset }) {
    Object.assign(this, { readers, curOffset });
  }

  static async create({ vertexInfo, prefix, offset }) {
    const promiseReaders = vertexInfo.propertyGroups.map(
      async (propertyGroup) =>
        await VertexPropertyArrowChunkReader.create({
          vertexInfo,
          propertyGroup,
          prefix,
        }),
    );
    const readers = await Promise.all(promiseReaders);
    Vertex.curOffset = offset;

    return new Vertex({ readers, curOffset: offset });
  }

  async property(property) {
    let arrowArray = null;
    for (const reader of this.readers) {
      reader.seek(this.curOffset);
      const chunkTable = await reader.getChunk();
      arrowArray = chunkTable.batches[0]?.getChild(property);
      if (arrowArray) {
        break;
      }
    }
    if (arrowArray) {
      return arrowArray.get(0);
    }
    throw new Error(`Vertex property ${property} not found in vertex info.`);
  }
}

class VertexIter {
  constructor(vertex, vertexNum) {
    Object.assign(this, { vertex, vertexNum });
  }

  [Symbol.iterator]() {
    let curOffset = 0;
    const that = this;
    return {
      next() {
        that.vertex.curOffset = curOffset++;
        if (that.vertex.curOffset < that.vertexNum) {
          return {
            value: that.vertex,
            done: false,
          };
        }
        return { done: true };
      },
    };
  }
}

class VerticesCollection {
  constructor(vertexInfo, fs, prefix, vertexNum) {
    Object.assign(this, { vertexInfo, fs, prefix, vertexNum });
  }

  static async init(vertexInfo, prefix) {
    const [fs, noUrlPath] = fileSystemFromUriOrPath(prefix);
    const vertexNumFilePath = vertexInfo.getVerticesNumFilePath();
    const vertexNumPath = noUrlPath + vertexNumFilePath;
    const vertexNum = await fs.readFileAsSingleUint64(vertexNumPath);

    return new VerticesCollection(vertexInfo, fs, prefix, vertexNum);
  }

  static async make(graphInfo, type) {
    const vertexInfo = graphInfo.getVertexInfo(type);
    if (!vertexInfo) {
      throw new Error(`Vertex type ${type} not found in graph info.`);
    }
    return await VerticesCollection.init(vertexInfo, graphInfo.prefix);
  }

  async getIterator() {
    const vertex = await Vertex.create({
      vertexInfo: this.vertexInfo,
      prefix: this.prefix,
      offset: 0,
    });
    return new VertexIter(vertex, this.vertexNum);
  }
}

class EdgeIter {
  constructor({
    edgeInfo,
    prefix,
    adjListType,
    adjListReader,
    globalChunkIndex,
    curOffset,
    chunkSize,
    srcChunkSize,
    dstChunkSize,
    numRowOfChunk,
    chunkBegin,
    chunkEnd,
    indexConverter,
    vertexChunkIndex,
    propertyReaders,
    offsetReader,
  }) {
    Object.assign(this, {
      edgeInfo,
      prefix,
      adjListType,
      adjListReader,
      globalChunkIndex,
      curOffset,
      chunkSize,
      srcChunkSize,
      dstChunkSize,
      numRowOfChunk,
      chunkBegin,
      chunkEnd,
      indexConverter,
      vertexChunkIndex,
      propertyReaders,
      offsetReader,
    });
  }

  static async create({
    edgeInfo,
    prefix,
    adjListType,
    globalChunkIndex,
    offset,
    chunkBegin,
    chunkEnd,
    indexConverter,
  }) {
    const adjListReader = await AdjListArrowChunkReader.create({
      edgeInfo,
      adjListType,
      prefix,
    });
    const curOffset = offset;
    const chunkSize = edgeInfo.chunkSize;
    const srcChunkSize = edgeInfo.srcChunkSize;
    const dstChunkSize = edgeInfo.dstChunkSize;
    const numRowOfChunk = 0;
    const [vertexChunkIndex, _] =
      indexConverter.globalChunkIndexToIndexPair(globalChunkIndex);
    await adjListReader.seekChunkIndex(vertexChunkIndex);
    const promisePropertyReaders = edgeInfo.propertyGroups.map(
      async (propertyGroup) => {
        const propertyReader = await AdjListPropertyArrowChunkReader.create({
          edgeInfo,
          propertyGroup,
          adjListType,
          prefix,
        });
        propertyReader.seekChunkIndex(vertexChunkIndex);
        return propertyReader;
      },
    );
    const propertyReaders = await Promise.all(promisePropertyReaders);
    let offsetReader;
    if (
      adjListType === AdjListType.ORDERED_BY_SOURCE ||
      adjListType === AdjListType.ORDERED_BY_DEST
    ) {
      offsetReader = await AdjListOffsetArrowChunkReader.create({
        edgeInfo,
        adjListType,
        prefix,
      });
    }

    return new EdgeIter({
      edgeInfo,
      prefix,
      adjListType,
      adjListReader,
      globalChunkIndex,
      curOffset,
      chunkSize,
      srcChunkSize,
      dstChunkSize,
      numRowOfChunk,
      chunkBegin,
      chunkEnd,
      indexConverter,
      vertexChunkIndex,
      propertyReaders,
      offsetReader,
    });
  }

  async source() {
    await this.adjListReader.seek(this.curOffset);
    const chunk = await this.adjListReader.getChunk();
    const srcColumn = chunk.batches[0]?.getChildAt(0);
    return srcColumn.get(0);
  }

  async destination() {
    await this.adjListReader.seek(this.curOffset);
    const chunk = await this.adjListReader.getChunk();
    const dstColumn = chunk.batches[0]?.getChildAt(1);
    return dstColumn.get(0);
  }

  async *[Symbol.asyncIterator]() {
    while (this.globalChunkIndex < this.chunkEnd) {
      if (this.numRowOfChunk === 0) {
        await this.adjListReader.seek(this.curOffset);
        this.numRowOfChunk = await this.adjListReader.getRowNumOfChunk();
      }
      let r = await this.adjListReader.seek(this.curOffset);
      if (r.ok && this.numRowOfChunk !== this.chunkSize) {
        const rowOffset = this.curOffset % this.chunkSize;
        if (rowOffset >= this.numRowOfChunk) {
          this.curOffset =
            (this.curOffset / this.chunkSize + 1) * this.chunkSize;
          await this.adjListReader.seek(this.curOffset);
          r = { ok: false, error: { code: 'KeyError' } };
        }
      }
      if (
        r.ok &&
        this.numRowOfChunk === this.chunkSize &&
        this.curOffset % this.chunkSize === 0
      ) {
        this.numRowOfChunk = await this.adjListReader.getRowNumOfChunk();
        this.globalChunkIndex++;
        for (const reader of this.propertyReaders) {
          await reader.nextChunk();
        }
      }
      if (r.error?.code === 'KeyError') {
        r = await this.adjListReader.nextChunk();
        this.globalChunkIndex++;
        this.vertexChunkIndex++;
        if (!r.error?.code === 'IndexError') {
          this.numRowOfChunk = await this.adjListReader.getRowNumOfChunk();
          for (const reader of this.propertyReaders) {
            await reader.nextChunk();
          }
        } else {
          break;
        }
        this.curOffset = 0;
        await this.adjListReader.seek(this.curOffset);
      }
      yield this;
      this.curOffset++;
    }
  }
}

class EdgesCollection {
  constructor(
    edgeInfo,
    prefix,
    chunkBegin,
    chunkEnd,
    edgeNum,
    adjListType,
    indexConverter,
  ) {
    Object.assign(this, {
      edgeInfo,
      prefix,
      chunkBegin,
      chunkEnd,
      edgeNum,
      adjListType,
      indexConverter,
    });
  }

  static async init(
    edgeInfo,
    prefix,
    vertexChunkBegin,
    vertexChunkEnd,
    adjListType,
  ) {
    const vertexChunkNum = await getVertexChunkNumFromEdge(
      prefix,
      edgeInfo,
      adjListType,
    );
    if (vertexChunkEnd === MAX_INT64) {
      vertexChunkEnd = vertexChunkNum;
    }
    let chunkBegin = 0n;
    let chunkEnd = 0n;
    let edgeNum = 0n;
    const edgeChunkNums = [];
    for (let i = 0; i < vertexChunkNum; i++) {
      edgeChunkNums[i] = await edgeInfo.getEdgeChunkNum(prefix, adjListType, i);
      if (i < vertexChunkBegin) {
        chunkBegin += edgeChunkNums[i];
        chunkEnd += edgeChunkNums[i];
      }
      if (i >= vertexChunkBegin && i < vertexChunkEnd) {
        chunkEnd += edgeChunkNums[i];
        const chunkEdgeNum = await edgeInfo.getEdgeNum(prefix, adjListType, i);
        edgeNum += chunkEdgeNum;
      }
    }
    const indexConverter = new IndexConverter(edgeChunkNums);

    return { chunkBegin, chunkEnd, edgeNum, indexConverter };
  }

  static async make(
    graphInfo,
    srcType,
    edgeType,
    dstType,
    adjListType,
    vertexChunkBegin = 0n,
    vertexChunkEnd = MAX_INT64,
  ) {
    const edgeInfo = graphInfo.getEdgeInfo(srcType, edgeType, dstType);
    if (!edgeInfo) {
      throw new Error(
        `Edge srcType: ${srcType}, edgeType: ${edgeType}, dstType: ${dstType} not found in graph info.`,
      );
    }
    if (!edgeInfo.hasAdjacentListType(adjListType)) {
      throw new Error(
        `Adjacent list type ${adjListType} not found in edge info.`,
      );
    }
    switch (adjListType) {
      case AdjListType.ORDERED_BY_SOURCE:
        return await OBSEdgesCollection.create(
          edgeInfo,
          graphInfo.prefix,
          vertexChunkBegin,
          vertexChunkEnd,
        );
      case AdjListType.ORDERED_BY_DEST:
        return await OBDEdgesCollection.create(
          edgeInfo,
          graphInfo.prefix,
          vertexChunkBegin,
          vertexChunkEnd,
        );
      case AdjListType.UNORDERED_BY_SOURCE:
        return await UBSEdgesCollection.create(
          edgeInfo,
          graphInfo.prefix,
          vertexChunkBegin,
          vertexChunkEnd,
        );
      case AdjListType.UNORDERED_BY_DEST:
        return await UBDEdgesCollection.create(
          edgeInfo,
          graphInfo.prefix,
          vertexChunkBegin,
          vertexChunkEnd,
        );
      default:
        throw new Error('Unknown adjacent list type');
    }
  }

  async getIterator() {
    return await EdgeIter.create({
      edgeInfo: this.edgeInfo,
      prefix: this.prefix,
      adjListType: this.adjListType,
      globalChunkIndex: this.chunkBegin,
      offset: 0,
      chunkBegin: this.chunkBegin,
      chunkEnd: this.chunkEnd,
      indexConverter: this.indexConverter,
    });
  }
}

class OBSEdgesCollection extends EdgesCollection {
  static async create(edgeInfo, prefix, vertexChunkBegin, vertexChunkEnd) {
    const { chunkBegin, chunkEnd, edgeNum, indexConverter } =
      await EdgesCollection.init(
        edgeInfo,
        prefix,
        vertexChunkBegin,
        vertexChunkEnd,
        AdjListType.ORDERED_BY_SOURCE,
      );
    return new OBSEdgesCollection(
      edgeInfo,
      prefix,
      chunkBegin,
      chunkEnd,
      edgeNum,
      AdjListType.ORDERED_BY_SOURCE,
      indexConverter,
    );
  }
}

class OBDEdgesCollection extends EdgesCollection {
  static async create(edgeInfo, prefix, vertexChunkBegin, vertexChunkEnd) {
    const { chunkBegin, chunkEnd, edgeNum, indexConverter } =
      await EdgesCollection.init(
        edgeInfo,
        prefix,
        vertexChunkBegin,
        vertexChunkEnd,
        AdjListType.ORDERED_BY_SOURCE,
      );
    return new OBDEdgesCollection(
      edgeInfo,
      prefix,
      chunkBegin,
      chunkEnd,
      edgeNum,
      AdjListType.ORDERED_BY_DEST,
      indexConverter,
    );
  }
}

class UBSEdgesCollection extends EdgesCollection {
  static async create(edgeInfo, prefix, vertexChunkBegin, vertexChunkEnd) {
    const { chunkBegin, chunkEnd, edgeNum, indexConverter } =
      await EdgesCollection.init(
        edgeInfo,
        prefix,
        vertexChunkBegin,
        vertexChunkEnd,
        AdjListType.ORDERED_BY_SOURCE,
      );
    return new UBSEdgesCollection(
      edgeInfo,
      prefix,
      chunkBegin,
      chunkEnd,
      edgeNum,
      AdjListType.UNORDERED_BY_SOURCE,
      indexConverter,
    );
  }
}

class UBDEdgesCollection extends EdgesCollection {
  static async create(edgeInfo, prefix, vertexChunkBegin, vertexChunkEnd) {
    const { chunkBegin, chunkEnd, edgeNum, indexConverter } =
      await EdgesCollection.init(
        edgeInfo,
        prefix,
        vertexChunkBegin,
        vertexChunkEnd,
        AdjListType.ORDERED_BY_SOURCE,
      );
    return new UBDEdgesCollection(
      edgeInfo,
      prefix,
      chunkBegin,
      chunkEnd,
      edgeNum,
      AdjListType.UNORDERED_BY_DEST,
      indexConverter,
    );
  }
}

export { EdgesCollection, VerticesCollection };
