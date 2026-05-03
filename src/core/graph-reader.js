import {
  AdjListArrowChunkReader,
  AdjListOffsetArrowChunkReader,
  AdjListPropertyArrowChunkReader,
  VertexPropertyArrowChunkReader,
} from './chunk-reader.js';
import { evaluateFilterExpression } from './filter.js';
import { fileSystemFromUriOrPath } from './filesystem.js';
import { getVertexChunkNumFromEdge } from './reader-util.js';
import { AdjListType } from './types.js';
import { IndexConverter, MAX_INT64 } from './util.js';

class Vertex {
  constructor({ readers, labelReader, labels, curOffset }) {
    Object.assign(this, { readers, labelReader, labels, curOffset });
  }

  static async create({ vertexInfo, prefix, offset }) {
    const curOffset = typeof offset === 'bigint' ? offset : BigInt(offset);
    const promiseReaders = vertexInfo.propertyGroups.map(
      async (propertyGroup) =>
        await VertexPropertyArrowChunkReader.create({
          vertexInfo,
          propertyGroup,
          prefix,
        }),
    );
    const readers = await Promise.all(promiseReaders);
    const labelReader =
      vertexInfo.labels.length > 0
        ? await VertexPropertyArrowChunkReader.create({
            vertexInfo,
            propertyGroup: vertexInfo.propertyGroups[0],
            prefix,
          })
        : null;
    Vertex.curOffset = offset;

    return new Vertex({
      readers,
      labelReader,
      labels: vertexInfo.labels,
      curOffset,
    });
  }

  id() {
    return this.curOffset;
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

  async hasLabel(label) {
    if (!this.labelReader) {
      throw new Error(`Vertex label ${label} not found in vertex info.`);
    }

    this.labelReader.seek(this.curOffset);
    const chunkTable = await this.labelReader.getLabelChunk();
    const arrowArray = chunkTable.batches[0]?.getChild(label);
    if (!arrowArray) {
      throw new Error(`Vertex label ${label} not found in vertex info.`);
    }
    return arrowArray.get(0);
  }

  async label() {
    if (!this.labelReader || this.labels.length === 0) {
      return [];
    }

    this.labelReader.seek(this.curOffset);
    const chunkTable = await this.labelReader.getLabelChunk();
    const vertexLabels = [];
    for (const label of this.labels) {
      const arrowArray = chunkTable.batches[0]?.getChild(label);
      if (arrowArray?.get(0)) {
        vertexLabels.push(label);
      }
    }
    return vertexLabels;
  }
}

class VertexIter {
  constructor(vertex, vertexNum, filteredIds = null) {
    Object.assign(this, { vertex, vertexNum, filteredIds });
  }

  [Symbol.iterator]() {
    let curOffset = 0n;
    const that = this;
    return {
      next() {
        if (that.filteredIds) {
          if (curOffset >= that.filteredIds.length) {
            return { done: true };
          }
          that.vertex.curOffset = BigInt(that.filteredIds[Number(curOffset++)]);
          return {
            value: that.vertex,
            done: false,
          };
        }

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
  constructor(vertexInfo, fs, prefix, vertexNum, filteredIds = null) {
    Object.assign(this, { vertexInfo, fs, prefix, vertexNum, filteredIds });
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

  static async resolveSource(graphInfoOrCollection, type) {
    if (graphInfoOrCollection instanceof VerticesCollection) {
      return graphInfoOrCollection;
    }
    return await VerticesCollection.make(graphInfoOrCollection, type);
  }

  static async verticesWithLabel(
    filterLabel,
    graphInfoOrCollection,
    type = undefined,
  ) {
    return await VerticesCollection.verticesWithMultipleLabels(
      [filterLabel],
      graphInfoOrCollection,
      type,
    );
  }

  static async verticesWithMultipleLabels(
    filterLabels,
    graphInfoOrCollection,
    type = undefined,
  ) {
    const vertices = await VerticesCollection.resolveSource(
      graphInfoOrCollection,
      type,
    );
    for (const label of filterLabels) {
      if (!vertices.vertexInfo.labels.includes(label)) {
        throw new Error(`Vertex label ${label} not found in vertex info.`);
      }
    }

    const iterator = await vertices.getIterator();
    const filteredIds = [];
    for (const vertex of iterator) {
      let matched = true;
      for (const label of filterLabels) {
        if (!(await vertex.hasLabel(label))) {
          matched = false;
          break;
        }
      }
      if (matched) {
        filteredIds.push(vertex.curOffset);
      }
    }
    return vertices.withFilteredIds(filteredIds);
  }

  static async verticesWithProperty(
    propertyName,
    filter,
    graphInfoOrCollection,
    type = undefined,
  ) {
    const vertices = await VerticesCollection.resolveSource(
      graphInfoOrCollection,
      type,
    );
    const propertyExists = vertices.vertexInfo.propertyGroups.some((group) =>
      group.properties.some((property) => property.name === propertyName),
    );
    if (!propertyExists) {
      throw new Error(`Vertex property ${propertyName} not found in vertex info.`);
    }

    const iterator = await vertices.getIterator();
    const filteredIds = [];
    for (const vertex of iterator) {
      const propertyValue = await vertex.property(propertyName);
      if (
        evaluateFilterExpression(filter, {
          [propertyName]: propertyValue,
        })
      ) {
        filteredIds.push(vertex.curOffset);
      }
    }
    return vertices.withFilteredIds(filteredIds);
  }

  withFilteredIds(filteredIds) {
    return new VerticesCollection(
      this.vertexInfo,
      this.fs,
      this.prefix,
      this.vertexNum,
      filteredIds,
    );
  }

  size() {
    if (this.filteredIds) {
      return BigInt(this.filteredIds.length);
    }
    return this.vertexNum;
  }

  async find(id) {
    return await Vertex.create({
      vertexInfo: this.vertexInfo,
      prefix: this.prefix,
      offset: typeof id === 'bigint' ? id : BigInt(id),
    });
  }

  async getIterator() {
    const vertex = await Vertex.create({
      vertexInfo: this.vertexInfo,
      prefix: this.prefix,
      offset: 0,
    });
    return new VertexIter(vertex, this.vertexNum, this.filteredIds);
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
    const curOffset = typeof offset === 'bigint' ? offset : BigInt(offset);
    const chunkSize = edgeInfo.chunkSize;
    const srcChunkSize = edgeInfo.srcChunkSize;
    const dstChunkSize = edgeInfo.dstChunkSize;
    const numRowOfChunk = 0;
    const [vertexChunkIndex, edgeChunkIndex] =
      indexConverter.globalChunkIndexToIndexPair(globalChunkIndex);
    await adjListReader.seekChunkIndex(vertexChunkIndex, Number(edgeChunkIndex));
    const promisePropertyReaders = edgeInfo.propertyGroups.map(
      async (propertyGroup) => {
        const propertyReader = await AdjListPropertyArrowChunkReader.create({
          edgeInfo,
          propertyGroup,
          adjListType,
          prefix,
        });
        await propertyReader.seekChunkIndex(
          vertexChunkIndex,
          Number(edgeChunkIndex),
        );
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

  syncChunkStateFromAdjListReader() {
    this.vertexChunkIndex = this.adjListReader.vertexChunkIndex;
  }

  isEnd() {
    return this.globalChunkIndex >= this.chunkEnd;
  }

  async clone() {
    const [vertexChunkIndex, edgeChunkIndex] =
      this.indexConverter.globalChunkIndexToIndexPair(this.globalChunkIndex);
    const clonedAdjListReader = await AdjListArrowChunkReader.create({
      edgeInfo: this.edgeInfo,
      adjListType: this.adjListType,
      prefix: this.prefix,
    });
    await clonedAdjListReader.seekChunkIndex(
      vertexChunkIndex,
      Number(edgeChunkIndex),
    );
    await clonedAdjListReader.seek(this.curOffset);

    const clonedPropertyReaders = await Promise.all(
      this.edgeInfo.propertyGroups.map(async (propertyGroup) => {
        const propertyReader = await AdjListPropertyArrowChunkReader.create({
          edgeInfo: this.edgeInfo,
          propertyGroup,
          adjListType: this.adjListType,
          prefix: this.prefix,
        });
        await propertyReader.seekChunkIndex(
          vertexChunkIndex,
          Number(edgeChunkIndex),
        );
        await propertyReader.seek(this.curOffset);
        return propertyReader;
      }),
    );

    let clonedOffsetReader;
    if (this.offsetReader) {
      clonedOffsetReader = await AdjListOffsetArrowChunkReader.create({
        edgeInfo: this.edgeInfo,
        adjListType: this.adjListType,
        prefix: this.prefix,
      });
    }

    return new EdgeIter({
      edgeInfo: this.edgeInfo,
      prefix: this.prefix,
      adjListType: this.adjListType,
      adjListReader: clonedAdjListReader,
      globalChunkIndex: this.globalChunkIndex,
      curOffset: this.curOffset,
      chunkSize: this.chunkSize,
      srcChunkSize: this.srcChunkSize,
      dstChunkSize: this.dstChunkSize,
      numRowOfChunk: this.numRowOfChunk,
      chunkBegin: this.chunkBegin,
      chunkEnd: this.chunkEnd,
      indexConverter: this.indexConverter,
      vertexChunkIndex,
      propertyReaders: clonedPropertyReaders,
      offsetReader: clonedOffsetReader,
    });
  }

  async refresh() {
    const [vertexChunkIndex, edgeChunkIndex] =
      this.indexConverter.globalChunkIndexToIndexPair(this.globalChunkIndex);
    this.vertexChunkIndex = vertexChunkIndex;
    await this.adjListReader.seekChunkIndex(
      vertexChunkIndex,
      Number(edgeChunkIndex),
    );
    await this.adjListReader.seek(this.curOffset);
    for (const reader of this.propertyReaders) {
      await reader.seekChunkIndex(vertexChunkIndex, Number(edgeChunkIndex));
      await reader.seek(this.curOffset);
    }
    this.numRowOfChunk = await this.adjListReader.getRowNumOfChunk();
  }

  async toBegin() {
    this.globalChunkIndex = this.chunkBegin;
    this.curOffset = 0n;
    await this.refresh();
  }

  async moveToNextChunk() {
    const result = await this.adjListReader.nextChunk();
    this.globalChunkIndex += 1n;
    if (result.error?.code === 'IndexError') {
      return false;
    }
    for (const reader of this.propertyReaders) {
      await reader.nextChunk();
    }
    this.syncChunkStateFromAdjListReader();
    this.numRowOfChunk = await this.adjListReader.getRowNumOfChunk();
    this.curOffset = this.adjListReader.seekOffset;
    return true;
  }

  async advance() {
    if (this.numRowOfChunk === 0) {
      await this.adjListReader.seek(this.curOffset);
      this.numRowOfChunk = await this.adjListReader.getRowNumOfChunk();
    }

    const nextOffset = this.curOffset + 1n;
    const seekResult = await this.adjListReader.seek(nextOffset);
    if (!seekResult.ok) {
      return await this.moveToNextChunk();
    }

    if (this.numRowOfChunk !== this.chunkSize) {
      const rowOffset = nextOffset % BigInt(this.chunkSize);
      if (rowOffset >= BigInt(this.numRowOfChunk)) {
        return await this.moveToNextChunk();
      }
    }

    this.curOffset = nextOffset;

    if (
      this.numRowOfChunk === this.chunkSize &&
      this.curOffset % BigInt(this.chunkSize) === 0n
    ) {
      this.globalChunkIndex += 1n;
      for (const reader of this.propertyReaders) {
        await reader.nextChunk();
      }
      this.syncChunkStateFromAdjListReader();
      this.numRowOfChunk = await this.adjListReader.getRowNumOfChunk();
    }

    return true;
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

  async property(property) {
    let arrowArray = null;
    for (const reader of this.propertyReaders) {
      await reader.seek(this.curOffset);
      const chunkTable = await reader.getChunk();
      arrowArray = chunkTable?.batches[0]?.getChild(property) ?? null;
      if (arrowArray) {
        break;
      }
    }
    if (arrowArray) {
      return arrowArray.get(0);
    }
    throw new Error(`Edge property ${property} not found in edge info.`);
  }

  async firstSrc(from, id) {
    const seekId = typeof id === 'bigint' ? id : BigInt(id);
    const fromGlobalChunkIndex = from.globalChunkIndex;
    const fromCurOffset = from.curOffset;
    const fromVertexChunkIndex = from.vertexChunkIndex;

    if (from.isEnd()) {
      return false;
    }

    if (
      this.adjListType === AdjListType.ORDERED_BY_DEST ||
      this.adjListType === AdjListType.UNORDERED_BY_DEST
    ) {
      if (fromGlobalChunkIndex >= this.chunkEnd) {
        return false;
      }
      if (fromGlobalChunkIndex === this.globalChunkIndex) {
        this.curOffset = fromCurOffset;
      } else if (fromGlobalChunkIndex < this.chunkBegin) {
        await this.toBegin();
      } else {
        this.globalChunkIndex = fromGlobalChunkIndex;
        this.curOffset = fromCurOffset;
        this.vertexChunkIndex = fromVertexChunkIndex;
        await this.refresh();
      }
      while (!this.isEnd()) {
        if ((await this.source()) === seekId) {
          return true;
        }
        await this.advance();
      }
      return false;
    }

    if (this.adjListType === AdjListType.UNORDERED_BY_SOURCE) {
      const expectedChunkIndex = this.indexConverter.indexPairToGlobalChunkIndex(
        Number(seekId / BigInt(this.srcChunkSize)),
        0,
      );
      if (expectedChunkIndex > this.chunkEnd || fromGlobalChunkIndex >= this.chunkEnd) {
        return false;
      }
      let needRefresh = false;
      if (fromGlobalChunkIndex === this.globalChunkIndex) {
        this.curOffset = fromCurOffset;
      } else if (fromGlobalChunkIndex < this.chunkBegin) {
        await this.toBegin();
      } else {
        this.globalChunkIndex = fromGlobalChunkIndex;
        this.curOffset = fromCurOffset;
        this.vertexChunkIndex = fromVertexChunkIndex;
        needRefresh = true;
      }
      if (this.globalChunkIndex < expectedChunkIndex) {
        this.globalChunkIndex = expectedChunkIndex;
        this.curOffset = 0n;
        this.vertexChunkIndex = Number(seekId / BigInt(this.srcChunkSize));
        needRefresh = true;
      }
      if (needRefresh) {
        await this.refresh();
      }
      while (!this.isEnd()) {
        if ((await this.source()) === seekId) {
          return true;
        }
        if (this.vertexChunkIndex > Number(seekId / BigInt(this.srcChunkSize))) {
          return false;
        }
        await this.advance();
      }
      return false;
    }

    const seekResult = await this.offsetReader.seek(seekId);
    if (!seekResult.ok) {
      return false;
    }
    const offsetChunk = await this.offsetReader.getChunk();
    const beginOffset = BigInt(offsetChunk.get(0));
    const endOffset = BigInt(offsetChunk.get(1));
    if (beginOffset >= endOffset) {
      return false;
    }
    const vertexChunkIndexOfId = this.offsetReader.chunkIndex;
    const beginGlobalIndex = this.indexConverter.indexPairToGlobalChunkIndex(
      vertexChunkIndexOfId,
      beginOffset / BigInt(this.chunkSize),
    );
    const endGlobalIndex = this.indexConverter.indexPairToGlobalChunkIndex(
      vertexChunkIndexOfId,
      endOffset / BigInt(this.chunkSize),
    );
    if (
      beginGlobalIndex <= fromGlobalChunkIndex &&
      fromGlobalChunkIndex <= endGlobalIndex
    ) {
      if (beginOffset < fromCurOffset && fromCurOffset < endOffset) {
        this.globalChunkIndex = fromGlobalChunkIndex;
        this.curOffset = fromCurOffset;
        this.vertexChunkIndex = fromVertexChunkIndex;
        await this.refresh();
        return true;
      }
      if (fromCurOffset <= beginOffset) {
        this.globalChunkIndex = beginGlobalIndex;
        this.curOffset = beginOffset;
        this.vertexChunkIndex = vertexChunkIndexOfId;
        await this.refresh();
        return true;
      }
      return false;
    }
    if (fromGlobalChunkIndex < beginGlobalIndex) {
      this.globalChunkIndex = beginGlobalIndex;
      this.curOffset = beginOffset;
      this.vertexChunkIndex = vertexChunkIndexOfId;
      await this.refresh();
      return true;
    }
    return false;
  }

  async firstDst(from, id) {
    const seekId = typeof id === 'bigint' ? id : BigInt(id);
    const fromGlobalChunkIndex = from.globalChunkIndex;
    const fromCurOffset = from.curOffset;
    const fromVertexChunkIndex = from.vertexChunkIndex;

    if (from.isEnd()) {
      return false;
    }

    if (
      this.adjListType === AdjListType.ORDERED_BY_SOURCE ||
      this.adjListType === AdjListType.UNORDERED_BY_SOURCE
    ) {
      if (fromGlobalChunkIndex >= this.chunkEnd) {
        return false;
      }
      if (fromGlobalChunkIndex === this.globalChunkIndex) {
        this.curOffset = fromCurOffset;
      } else if (fromGlobalChunkIndex < this.chunkBegin) {
        await this.toBegin();
      } else {
        this.globalChunkIndex = fromGlobalChunkIndex;
        this.curOffset = fromCurOffset;
        this.vertexChunkIndex = fromVertexChunkIndex;
        await this.refresh();
      }
      while (!this.isEnd()) {
        if ((await this.destination()) === seekId) {
          return true;
        }
        await this.advance();
      }
      return false;
    }

    if (this.adjListType === AdjListType.UNORDERED_BY_DEST) {
      const expectedChunkIndex = this.indexConverter.indexPairToGlobalChunkIndex(
        Number(seekId / BigInt(this.dstChunkSize)),
        0,
      );
      if (expectedChunkIndex > this.chunkEnd || fromGlobalChunkIndex >= this.chunkEnd) {
        return false;
      }
      let needRefresh = false;
      if (fromGlobalChunkIndex === this.globalChunkIndex) {
        this.curOffset = fromCurOffset;
      } else if (fromGlobalChunkIndex < this.chunkBegin) {
        await this.toBegin();
      } else {
        this.globalChunkIndex = fromGlobalChunkIndex;
        this.curOffset = fromCurOffset;
        this.vertexChunkIndex = fromVertexChunkIndex;
        needRefresh = true;
      }
      if (this.globalChunkIndex < expectedChunkIndex) {
        this.globalChunkIndex = expectedChunkIndex;
        this.curOffset = 0n;
        this.vertexChunkIndex = Number(seekId / BigInt(this.dstChunkSize));
        needRefresh = true;
      }
      if (needRefresh) {
        await this.refresh();
      }
      while (!this.isEnd()) {
        if ((await this.destination()) === seekId) {
          return true;
        }
        if (this.vertexChunkIndex > Number(seekId / BigInt(this.dstChunkSize))) {
          return false;
        }
        await this.advance();
      }
      return false;
    }

    const seekResult = await this.offsetReader.seek(seekId);
    if (!seekResult.ok) {
      return false;
    }
    const offsetChunk = await this.offsetReader.getChunk();
    const beginOffset = BigInt(offsetChunk.get(0));
    const endOffset = BigInt(offsetChunk.get(1));
    if (beginOffset >= endOffset) {
      return false;
    }
    const vertexChunkIndexOfId = this.offsetReader.chunkIndex;
    const beginGlobalIndex = this.indexConverter.indexPairToGlobalChunkIndex(
      vertexChunkIndexOfId,
      beginOffset / BigInt(this.chunkSize),
    );
    const endGlobalIndex = this.indexConverter.indexPairToGlobalChunkIndex(
      vertexChunkIndexOfId,
      endOffset / BigInt(this.chunkSize),
    );
    if (
      beginGlobalIndex <= fromGlobalChunkIndex &&
      fromGlobalChunkIndex <= endGlobalIndex
    ) {
      if (beginOffset < fromCurOffset && fromCurOffset < endOffset) {
        this.globalChunkIndex = fromGlobalChunkIndex;
        this.curOffset = fromCurOffset;
        this.vertexChunkIndex = fromVertexChunkIndex;
        await this.refresh();
        return true;
      }
      if (fromCurOffset <= beginOffset) {
        this.globalChunkIndex = beginGlobalIndex;
        this.curOffset = beginOffset;
        this.vertexChunkIndex = vertexChunkIndexOfId;
        await this.refresh();
        return true;
      }
      return false;
    }
    if (fromGlobalChunkIndex < beginGlobalIndex) {
      this.globalChunkIndex = beginGlobalIndex;
      this.curOffset = beginOffset;
      this.vertexChunkIndex = vertexChunkIndexOfId;
      await this.refresh();
      return true;
    }
    return false;
  }

  async nextSrc(id = undefined) {
    if (this.isEnd()) {
      return false;
    }
    if (id !== undefined) {
      const hasNext = await this.advance();
      if (!hasNext) {
        return false;
      }
      return await this.firstSrc(this, id);
    }

    const currentId = await this.source();
    const previousVertexChunkIndex = this.vertexChunkIndex;
    if (this.adjListType === AdjListType.ORDERED_BY_SOURCE) {
      const hasNext = await this.advance();
      if (!hasNext || this.isEnd()) {
        return false;
      }
      return (await this.source()) === currentId;
    }
    let hasNext = await this.advance();
    while (hasNext && !this.isEnd()) {
      if ((await this.source()) === currentId) {
        return true;
      }
      if (
        this.adjListType === AdjListType.UNORDERED_BY_SOURCE &&
        this.vertexChunkIndex > previousVertexChunkIndex
      ) {
        return false;
      }
      hasNext = await this.advance();
    }
    return false;
  }

  async nextDst(id = undefined) {
    if (this.isEnd()) {
      return false;
    }
    if (id !== undefined) {
      const hasNext = await this.advance();
      if (!hasNext) {
        return false;
      }
      return await this.firstDst(this, id);
    }

    const currentId = await this.destination();
    const previousVertexChunkIndex = this.vertexChunkIndex;
    if (this.adjListType === AdjListType.ORDERED_BY_DEST) {
      const hasNext = await this.advance();
      if (!hasNext || this.isEnd()) {
        return false;
      }
      return (await this.destination()) === currentId;
    }
    let hasNext = await this.advance();
    while (hasNext && !this.isEnd()) {
      if ((await this.destination()) === currentId) {
        return true;
      }
      if (
        this.adjListType === AdjListType.UNORDERED_BY_DEST &&
        this.vertexChunkIndex > previousVertexChunkIndex
      ) {
        return false;
      }
      hasNext = await this.advance();
    }
    return false;
  }

  async *[Symbol.asyncIterator]() {
    while (this.globalChunkIndex < this.chunkEnd) {
      if (this.numRowOfChunk === 0) {
        await this.adjListReader.seek(this.curOffset);
        this.numRowOfChunk = await this.adjListReader.getRowNumOfChunk();
      }
      yield this;
      const hasNext = await this.advance();
      if (!hasNext) {
        break;
      }
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

  async getEndIterator() {
    const iterator = await this.getIterator();
    iterator.globalChunkIndex = this.chunkEnd;
    iterator.curOffset = 0n;
    iterator.vertexChunkIndex = this.indexConverter.edgeChunkNums.length;
    iterator.numRowOfChunk = 0;
    return iterator;
  }

  size() {
    return this.edgeNum;
  }

  async findSrc(_id, _from) {
    throw new Error('findSrc must be implemented by subclasses.');
  }

  async findDst(_id, _from) {
    throw new Error('findDst must be implemented by subclasses.');
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

  async findSrc(id, from) {
    const iterator = await from.clone();
    if (await iterator.firstSrc(from, id)) {
      return iterator;
    }
    return await this.getEndIterator();
  }

  async findDst(id, from) {
    const iterator = await from.clone();
    if (await iterator.firstDst(from, id)) {
      return iterator;
    }
    return await this.getEndIterator();
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
        AdjListType.ORDERED_BY_DEST,
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

  async findSrc(id, from) {
    const iterator = await from.clone();
    if (await iterator.firstSrc(from, id)) {
      return iterator;
    }
    return await this.getEndIterator();
  }

  async findDst(id, from) {
    const iterator = await from.clone();
    if (await iterator.firstDst(from, id)) {
      return iterator;
    }
    return await this.getEndIterator();
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
        AdjListType.UNORDERED_BY_SOURCE,
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

  async findSrc(id, from) {
    const iterator = await from.clone();
    if (await iterator.firstSrc(from, id)) {
      return iterator;
    }
    return await this.getEndIterator();
  }

  async findDst(id, from) {
    const iterator = await from.clone();
    if (await iterator.firstDst(from, id)) {
      return iterator;
    }
    return await this.getEndIterator();
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
        AdjListType.UNORDERED_BY_DEST,
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

  async findSrc(id, from) {
    const iterator = await from.clone();
    if (await iterator.firstSrc(from, id)) {
      return iterator;
    }
    return await this.getEndIterator();
  }

  async findDst(id, from) {
    const iterator = await from.clone();
    if (await iterator.firstDst(from, id)) {
      return iterator;
    }
    return await this.getEndIterator();
  }
}

export { EdgesCollection, VerticesCollection };
