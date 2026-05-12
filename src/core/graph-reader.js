import {
  AdjListArrowChunkReader,
  AdjListOffsetArrowChunkReader,
  AdjListPropertyArrowChunkReader,
  VertexPropertyArrowChunkReader,
} from './chunk-reader.js';
import { evaluateFilterExpression, getFilterColumns } from './filter.js';
import { fileSystemFromUriOrPath } from './filesystem.js';
import {
  getVertexChunkNumFromEdge,
  getVertexNumFromVertex,
} from './reader-util.js';
import { AdjListType, DataType, Type } from './types.js';
import { IndexConverter, MAX_INT64 } from './util.js';

function isListLikeProperty(propertyDefinition) {
  return (
    propertyDefinition?.type?.id === Type.LIST ||
    (propertyDefinition?.cardinality != null &&
      propertyDefinition.cardinality !== 'single')
  );
}

function dataTypesEqual(left, right) {
  if (!left || !right || left.id !== right.id) {
    return false;
  }
  if (left.id === Type.LIST) {
    return dataTypesEqual(left.child, right.child);
  }
  if (left.id === Type.USER_DEFINED) {
    return left.userDefinedTypeName === right.userDefinedTypeName;
  }
  return true;
}

function resolveRequestedType(type) {
  if (typeof type === 'string') {
    return DataType.typeNameToDataType(type);
  }
  if (type instanceof DataType) {
    return type;
  }
  throw new Error(`Unsupported property type request: ${type}`);
}

function assertPropertyTypeMatches(property, requestedType, propertyDefinition) {
  if (dataTypesEqual(requestedType, propertyDefinition.type)) {
    return;
  }
  throw new Error(
    `Property type of ${property} is not matched. Expected ${requestedType.toTypeName()}, actual ${propertyDefinition.type.toTypeName()}.`,
  );
}

class PropertyList {
  constructor(values) {
    this.values = Array.isArray(values) ? [...values] : Array.from(values ?? []);
  }

  get length() {
    return this.values.length;
  }

  size() {
    return this.values.length;
  }

  at(index) {
    return this.values.at(index);
  }

  toArray() {
    return [...this.values];
  }

  toJSON() {
    return this.toArray();
  }

  [Symbol.iterator]() {
    return this.values[Symbol.iterator]();
  }
}

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
    let propertyDefinition = null;
    for (const reader of this.readers) {
      reader.seek(this.curOffset);
      const chunkTable = await reader.getChunk();
      arrowArray = chunkTable.batches[0]?.getChild(property);
      propertyDefinition = reader.propertyGroup.properties.find(
        (candidate) => candidate.name === property,
      );
      if (arrowArray) {
        break;
      }
    }
    if (arrowArray) {
      const value = arrowArray.get(0);
      if (isListLikeProperty(propertyDefinition)) {
        return new PropertyList(value);
      }
      if (value === null) {
        throw new Error(`The value of the ${property} is null.`);
      }
      return value;
    }
    throw new Error(`Vertex property ${property} not found in vertex info.`);
  }

  async propertyAs(type, property) {
    const requestedType = resolveRequestedType(type);
    let arrowArray = null;
    let propertyDefinition = null;
    for (const reader of this.readers) {
      reader.seek(this.curOffset);
      const chunkTable = await reader.getChunk();
      arrowArray = chunkTable.batches[0]?.getChild(property);
      propertyDefinition = reader.propertyGroup.properties.find(
        (candidate) => candidate.name === property,
      );
      if (arrowArray) {
        break;
      }
    }
    if (!arrowArray) {
      throw new Error(`Vertex property ${property} not found in vertex info.`);
    }

    const value = arrowArray.get(0);
    if (!isListLikeProperty(propertyDefinition) && value === null) {
      throw new Error(`The value of the ${property} is null.`);
    }

    assertPropertyTypeMatches(property, requestedType, propertyDefinition);

    if (isListLikeProperty(propertyDefinition)) {
      return new PropertyList(value);
    }
    return value;
  }

  async isValid(property) {
    for (const reader of this.readers) {
      reader.seek(this.curOffset);
      const chunkTable = await reader.getChunk();
      const arrowArray = chunkTable.batches[0]?.getChild(property);
      if (!arrowArray) {
        continue;
      }
      const propertyDefinition = reader.propertyGroup.properties.find(
        (candidate) => candidate.name === property,
      );
      if (isListLikeProperty(propertyDefinition)) {
        return true;
      }
      return arrowArray.get(0) !== null;
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
  constructor(vertex, vertexNum, filteredIds = null, curOffset = 0n) {
    Object.assign(this, {
      vertex,
      vertexNum,
      filteredIds,
      curOffset: typeof curOffset === 'bigint' ? curOffset : BigInt(curOffset),
    });
  }

  isEnd() {
    const endOffset = this.filteredIds
      ? BigInt(this.filteredIds.length)
      : this.vertexNum;
    return this.curOffset >= endOffset;
  }

  equals(other) {
    return this.curOffset === other.curOffset;
  }

  notEquals(other) {
    return !this.equals(other);
  }

  advance(offset = 1n) {
    this.curOffset += typeof offset === 'bigint' ? offset : BigInt(offset);
    return this;
  }

  currentVertexOffset() {
    if (this.isEnd()) {
      throw new Error('Vertex iterator is at end.');
    }
    if (this.filteredIds) {
      return BigInt(this.filteredIds[Number(this.curOffset)]);
    }
    return this.curOffset;
  }

  id() {
    return this.currentVertexOffset();
  }

  async property(property) {
    this.vertex.curOffset = this.currentVertexOffset();
    return await this.vertex.property(property);
  }

  async propertyAs(type, property) {
    this.vertex.curOffset = this.currentVertexOffset();
    return await this.vertex.propertyAs(type, property);
  }

  async isValid(property) {
    this.vertex.curOffset = this.currentVertexOffset();
    return await this.vertex.isValid(property);
  }

  async hasLabel(label) {
    this.vertex.curOffset = this.currentVertexOffset();
    return await this.vertex.hasLabel(label);
  }

  async label() {
    this.vertex.curOffset = this.currentVertexOffset();
    return await this.vertex.label();
  }

  [Symbol.iterator]() {
    const that = this;
    return {
      next() {
        if (that.isEnd()) {
          return { done: true };
        }

        if (that.filteredIds) {
          that.vertex.curOffset = BigInt(
            that.filteredIds[Number(that.curOffset)],
          );
          that.curOffset += 1n;
          return {
            value: that.vertex,
            done: false,
          };
        }

        that.vertex.curOffset = that.curOffset;
        that.curOffset += 1n;
        return {
          value: that.vertex,
          done: false,
        };
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

    const referencedProperties = [
      propertyName,
      ...getFilterColumns(filter).filter((column) => column !== propertyName),
    ];
    for (const column of referencedProperties) {
      const columnExists = vertices.vertexInfo.propertyGroups.some((group) =>
        group.properties.some((property) => property.name === column),
      );
      if (!columnExists) {
        throw new Error(`Vertex property ${column} not found in vertex info.`);
      }
    }

    const iterator = await vertices.getIterator();
    const filteredIds = [];
    for (const vertex of iterator) {
      const row = {};
      for (const column of referencedProperties) {
        row[column] = await vertex.property(column);
      }
      if (evaluateFilterExpression(filter, row)) {
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
    const vertexId = typeof id === 'bigint' ? id : BigInt(id);
    if (vertexId < 0n || vertexId >= this.vertexNum) {
      throw new Error(
        `Internal vertex id ${vertexId} is out of range: [0, ${this.vertexNum})`,
      );
    }
    return await Vertex.create({
      vertexInfo: this.vertexInfo,
      prefix: this.prefix,
      offset: vertexId,
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

  async getEndIterator() {
    const endOffset = this.size();
    const vertex = await Vertex.create({
      vertexInfo: this.vertexInfo,
      prefix: this.prefix,
      offset: endOffset,
    });
    return new VertexIter(vertex, this.vertexNum, this.filteredIds, endOffset);
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

  ensureNotEnd() {
    if (this.isEnd()) {
      throw new Error('Edge iterator is at end.');
    }
  }

  equals(other) {
    return (
      this.globalChunkIndex === other.globalChunkIndex &&
      this.curOffset === other.curOffset &&
      this.adjListType === other.adjListType
    );
  }

  notEquals(other) {
    return !this.equals(other);
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

  moveToEnd() {
    this.globalChunkIndex = this.chunkEnd;
    this.curOffset = 0n;
    this.vertexChunkIndex = this.indexConverter.edgeChunkNums.length;
    this.numRowOfChunk = 0;
  }

  async moveToNextChunk() {
    const result = await this.adjListReader.nextChunk();
    this.globalChunkIndex += 1n;
    if (result.error?.code === 'IndexError' || this.isEnd()) {
      this.moveToEnd();
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
    if (this.isEnd()) {
      return false;
    }

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
      if (this.isEnd()) {
        this.moveToEnd();
        return false;
      }
      for (const reader of this.propertyReaders) {
        await reader.nextChunk();
      }
      this.syncChunkStateFromAdjListReader();
      this.numRowOfChunk = await this.adjListReader.getRowNumOfChunk();
    }

    return true;
  }

  async source() {
    this.ensureNotEnd();
    await this.adjListReader.seek(this.curOffset);
    const chunk = await this.adjListReader.getChunk();
    const srcColumn = chunk.batches[0]?.getChildAt(0);
    return srcColumn.get(0);
  }

  async destination() {
    this.ensureNotEnd();
    await this.adjListReader.seek(this.curOffset);
    const chunk = await this.adjListReader.getChunk();
    const dstColumn = chunk.batches[0]?.getChildAt(1);
    return dstColumn.get(0);
  }

  async property(property) {
    this.ensureNotEnd();
    let arrowArray = null;
    let propertyDefinition = null;
    for (const reader of this.propertyReaders) {
      await reader.seek(this.curOffset);
      const chunkTable = await reader.getChunk();
      arrowArray = chunkTable?.batches[0]?.getChild(property) ?? null;
      propertyDefinition = reader.propertyGroup.properties.find(
        (candidate) => candidate.name === property,
      );
      if (arrowArray) {
        break;
      }
    }
    if (arrowArray) {
      const value = arrowArray.get(0);
      if (isListLikeProperty(propertyDefinition)) {
        return new PropertyList(value);
      }
      if (value === null) {
        throw new Error(`The value of the ${property} is null.`);
      }
      return value;
    }
    throw new Error(`Edge property ${property} not found in edge info.`);
  }

  async propertyAs(type, property) {
    this.ensureNotEnd();
    const requestedType = resolveRequestedType(type);
    let arrowArray = null;
    let propertyDefinition = null;
    for (const reader of this.propertyReaders) {
      await reader.seek(this.curOffset);
      const chunkTable = await reader.getChunk();
      arrowArray = chunkTable?.batches[0]?.getChild(property) ?? null;
      propertyDefinition = reader.propertyGroup.properties.find(
        (candidate) => candidate.name === property,
      );
      if (arrowArray) {
        break;
      }
    }
    if (!arrowArray) {
      throw new Error(`Edge property ${property} not found in edge info.`);
    }

    const value = arrowArray.get(0);
    if (!isListLikeProperty(propertyDefinition) && value === null) {
      throw new Error(`The value of the ${property} is null.`);
    }

    assertPropertyTypeMatches(property, requestedType, propertyDefinition);

    if (isListLikeProperty(propertyDefinition)) {
      return new PropertyList(value);
    }
    return value;
  }

  async isValid(property) {
    this.ensureNotEnd();
    for (const reader of this.propertyReaders) {
      await reader.seek(this.curOffset);
      const chunkTable = await reader.getChunk();
      const arrowArray = chunkTable?.batches[0]?.getChild(property) ?? null;
      if (!arrowArray) {
        continue;
      }
      const propertyDefinition = reader.propertyGroup.properties.find(
        (candidate) => candidate.name === property,
      );
      if (isListLikeProperty(propertyDefinition)) {
        return true;
      }
      return arrowArray.get(0) !== null;
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
    srcVertexNum,
    dstVertexNum,
  ) {
    Object.assign(this, {
      edgeInfo,
      prefix,
      chunkBegin,
      chunkEnd,
      edgeNum,
      adjListType,
      indexConverter,
      srcVertexNum,
      dstVertexNum,
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
    vertexChunkBegin =
      typeof vertexChunkBegin === 'bigint'
        ? vertexChunkBegin
        : BigInt(vertexChunkBegin);
    vertexChunkEnd =
      typeof vertexChunkEnd === 'bigint'
        ? vertexChunkEnd
        : BigInt(vertexChunkEnd);
    if (vertexChunkEnd === MAX_INT64) {
      vertexChunkEnd = vertexChunkNum;
    }
    if (vertexChunkBegin < 0n || vertexChunkEnd < 0n) {
      throw new Error(
        `Vertex chunk range [${vertexChunkBegin}, ${vertexChunkEnd}) must be non-negative.`,
      );
    }
    if (vertexChunkBegin > vertexChunkEnd) {
      throw new Error(
        `Vertex chunk range [${vertexChunkBegin}, ${vertexChunkEnd}) is invalid: begin must be less than or equal to end.`,
      );
    }
    if (vertexChunkEnd > vertexChunkNum) {
      throw new Error(
        `Vertex chunk range [${vertexChunkBegin}, ${vertexChunkEnd}) is out of range: [0, ${vertexChunkNum})`,
      );
    }
    let chunkBegin = 0n;
    let chunkEnd = 0n;
    let edgeNum = 0n;
    const edgeChunkNums = [];
    for (let i = 0; i < vertexChunkNum; i++) {
      edgeChunkNums[i] = await edgeInfo.getEdgeChunkNum(prefix, adjListType, i);
      const vertexChunkIndex = BigInt(i);
      if (vertexChunkIndex < vertexChunkBegin) {
        chunkBegin += edgeChunkNums[i];
        chunkEnd += edgeChunkNums[i];
      }
      if (
        vertexChunkIndex >= vertexChunkBegin &&
        vertexChunkIndex < vertexChunkEnd
      ) {
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
    const srcVertexInfo = graphInfo.getVertexInfo(srcType);
    const dstVertexInfo = graphInfo.getVertexInfo(dstType);
    const [srcVertexNum, dstVertexNum] = await Promise.all([
      srcVertexInfo
        ? getVertexNumFromVertex(graphInfo.prefix, srcVertexInfo)
        : MAX_INT64,
      dstVertexInfo
        ? getVertexNumFromVertex(graphInfo.prefix, dstVertexInfo)
        : MAX_INT64,
    ]);
    switch (adjListType) {
      case AdjListType.ORDERED_BY_SOURCE:
        return await OBSEdgesCollection.create(
          edgeInfo,
          graphInfo.prefix,
          vertexChunkBegin,
          vertexChunkEnd,
          srcVertexNum,
          dstVertexNum,
        );
      case AdjListType.ORDERED_BY_DEST:
        return await OBDEdgesCollection.create(
          edgeInfo,
          graphInfo.prefix,
          vertexChunkBegin,
          vertexChunkEnd,
          srcVertexNum,
          dstVertexNum,
        );
      case AdjListType.UNORDERED_BY_SOURCE:
        return await UBSEdgesCollection.create(
          edgeInfo,
          graphInfo.prefix,
          vertexChunkBegin,
          vertexChunkEnd,
          srcVertexNum,
          dstVertexNum,
        );
      case AdjListType.UNORDERED_BY_DEST:
        return await UBDEdgesCollection.create(
          edgeInfo,
          graphInfo.prefix,
          vertexChunkBegin,
          vertexChunkEnd,
          srcVertexNum,
          dstVertexNum,
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

  isValidSrcId(id) {
    return id >= 0n && id < this.srcVertexNum;
  }

  isValidDstId(id) {
    return id >= 0n && id < this.dstVertexNum;
  }

  async findSrc(id, from) {
    const seekId = typeof id === 'bigint' ? id : BigInt(id);
    if (!this.isValidSrcId(seekId)) {
      return await this.getEndIterator();
    }
    const iterator = await from.clone();
    if (await iterator.firstSrc(from, seekId)) {
      return iterator;
    }
    return await this.getEndIterator();
  }

  async findDst(id, from) {
    const seekId = typeof id === 'bigint' ? id : BigInt(id);
    if (!this.isValidDstId(seekId)) {
      return await this.getEndIterator();
    }
    const iterator = await from.clone();
    if (await iterator.firstDst(from, seekId)) {
      return iterator;
    }
    return await this.getEndIterator();
  }
}

class OBSEdgesCollection extends EdgesCollection {
  static async create(
    edgeInfo,
    prefix,
    vertexChunkBegin,
    vertexChunkEnd,
    srcVertexNum,
    dstVertexNum,
  ) {
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
      srcVertexNum,
      dstVertexNum,
    );
  }
}

class OBDEdgesCollection extends EdgesCollection {
  static async create(
    edgeInfo,
    prefix,
    vertexChunkBegin,
    vertexChunkEnd,
    srcVertexNum,
    dstVertexNum,
  ) {
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
      srcVertexNum,
      dstVertexNum,
    );
  }
}

class UBSEdgesCollection extends EdgesCollection {
  static async create(
    edgeInfo,
    prefix,
    vertexChunkBegin,
    vertexChunkEnd,
    srcVertexNum,
    dstVertexNum,
  ) {
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
      srcVertexNum,
      dstVertexNum,
    );
  }
}

class UBDEdgesCollection extends EdgesCollection {
  static async create(
    edgeInfo,
    prefix,
    vertexChunkBegin,
    vertexChunkEnd,
    srcVertexNum,
    dstVertexNum,
  ) {
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
      srcVertexNum,
      dstVertexNum,
    );
  }
}

export { EdgesCollection, PropertyList, VerticesCollection };
