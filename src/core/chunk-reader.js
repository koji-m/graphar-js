import * as arrow from 'apache-arrow';
import { fileSystemFromUriOrPath } from './filesystem.js';
import GENERAL_PARAMS from './general-params.js';
import {
  getAdjListOffsetOfVertex,
  getEdgeNum,
  getVertexChunkNumFromEdge,
  getVertexChunkNumFromVertex,
  getVertexNumFromVertex,
} from './reader-util.js';
import { AdjListType, adjListTypeToString, DataType } from './types.js';

function propertyGroupToSchema(propertyGroup, containIndexColumn = false) {
  const fields = [];
  if (containIndexColumn) {
    fields.push(
      new arrow.Field(GENERAL_PARAMS.kVertexIndexCol, new arrow.Int64()),
    );
  }
  for (const prop of propertyGroup.properties) {
    let dataType = DataType.dataTypeToArrowDataType(prop.type);
    const name = prop.name;
    if (prop.cardinality !== 'single') {
      dataType = new arrow.List(new arrow.Field('item', dataType, true));
    }
    fields.push(new arrow.Field(name, dataType));
  }
  return new arrow.Schema(fields);
}

function castTableWithSchema(table, _schema) {
  // TODO
  return table;
}

class VertexPropertyArrowChunkReader {
  constructor({
    vertexInfo,
    propertyGroup,
    propertyNames,
    fs,
    prefix,
    baseDir,
    chunkNum,
    vertexNum,
    schema,
    chunkIndex,
    seekId,
    filterOptions,
    chunkTable,
  }) {
    Object.assign(this, {
      vertexInfo,
      propertyGroup,
      propertyNames,
      fs,
      prefix,
      baseDir,
      chunkNum,
      vertexNum,
      schema,
      chunkIndex,
      seekId,
      filterOptions,
      chunkTable,
    });
  }

  static async create({
    vertexInfo,
    propertyGroup,
    prefix,
    propertyNames = [],
    options = {},
  }) {
    const [fs, noUrlPath] = fileSystemFromUriOrPath(prefix);
    const pgPathPrefix = vertexInfo.getPathPrefix(propertyGroup);
    const baseDir = noUrlPath + pgPathPrefix;
    const chunkNum = await getVertexChunkNumFromVertex(prefix, vertexInfo);
    const vertexNum = await getVertexNumFromVertex(prefix, vertexInfo);
    const schema = propertyGroupToSchema(propertyGroup, true);

    return new VertexPropertyArrowChunkReader({
      vertexInfo,
      propertyGroup,
      propertyNames,
      fs,
      prefix: noUrlPath,
      baseDir,
      chunkNum,
      vertexNum,
      schema,
      chunkIndex: 0,
      seekId: 0,
      filterOptions: options,
      chunkTable: null,
    });
  }

  seek(id) {
    this.seekId = id;
    const preChunkIndex = this.chunkIndex;
    this.chunkIndex = Math.floor(id / this.vertexInfo.chunkSize);
    if (this.chunkIndex >= this.chunkNum) {
      throw new Error(
        `Internal vertex id ${id} is out of range: [0, ${this.chunkNum * BigInt(this.vertexInfo.chunkSize)})`,
      );
    }
    if (this.chunkIndex !== preChunkIndex) {
      this.chunkTable = null;
    }
  }

  async getChunkV2() {
    if (this.chunkTable === null) {
      const chunkFilePath = this.vertexInfo.getFilePath(
        this.propertyGroup,
        this.chunkIndex,
      );
      const columns = [];
      let propertyNames = [];
      if (!this.filterOptions.columns && this.propertyNames.length > 0) {
        propertyNames = this.propertyNames;
      } else {
        if (propertyNames.length > 0) {
          for (const col of this.filterOptions.columns) {
            if (!this.propertyNames.includes(col)) {
              throw new Error(`Column ${col} is not in property group`);
            }
            propertyNames.push(col);
          }
        }
      }
      for (const col of propertyNames) {
        if (!this.schema.fields.find((f) => f.name === col)) {
          throw new Error(`Column ${col} not found in schema`);
        }
        columns.push(col);
      }
      const path = this.prefix + chunkFilePath;
      this.chunkTable = await this.fs.readFileAsTable(
        path,
        this.propertyGroup.fileType,
        columns,
      );
      if (this.schema !== null && this.filterOptions.filter === null) {
        this.chunkTable = castTableWithSchema(this.chunkTable, this.schema);
      }
    }
    const rowOffset = this.seekId - this.chunkIndex * this.vertexInfo.chunkSize;
    return this.chunkTable.slice(rowOffset);
  }

  async getChunk() {
    return await this.getChunkV2();
  }
}

class AdjListArrowChunkReader {
  constructor({
    edgeInfo,
    adjListType,
    fs,
    prefix,
    baseDir,
    vertexChunkNum,
    vertexChunkIndex,
    chunkIndex,
    seekOffset,
    chunkTable,
    chunkNum,
  }) {
    Object.assign(this, {
      edgeInfo,
      adjListType,
      fs,
      prefix,
      baseDir,
      vertexChunkNum,
      vertexChunkIndex,
      chunkIndex,
      seekOffset,
      chunkTable,
      chunkNum,
    });
  }

  static async create({ edgeInfo, adjListType, prefix }) {
    const [fs, noUrlPath] = fileSystemFromUriOrPath(prefix);
    const adjListPathPrefix = edgeInfo.getAdjListPathPrefix(adjListType);
    const baseDir = noUrlPath + adjListPathPrefix;
    const vertexChunkNum = await getVertexChunkNumFromEdge(
      prefix,
      edgeInfo,
      adjListType,
    );
    return new AdjListArrowChunkReader({
      edgeInfo,
      adjListType,
      fs,
      prefix: noUrlPath,
      baseDir,
      vertexChunkNum,
      vertexChunkIndex: 0,
      chunkIndex: 0,
      seekOffset: 0,
      chunkTable: null,
      chunkNum: -1,
    });
  }

  async seekChunkIndex(vertexChunkIndex, chunkIndex = 0) {
    if (this.chunkNum < 0 || this.vertexChunkIndex !== vertexChunkIndex) {
      this.vertexChunkIndex = vertexChunkIndex;
      await this.initOrUpdateEdgeChunkNum();
      this.chunkTable = null;
    }
    if (this.chunkIndex !== chunkIndex) {
      this.chunkIndex = chunkIndex;
      this.seekOffset = chunkIndex * this.edgeInfo.chunkSize;
      this.chunkTable = null;
    }
  }

  async initOrUpdateEdgeChunkNum() {
    this.chunkNum = await this.edgeInfo.getEdgeChunkNum(
      this.prefix,
      this.adjListType,
      this.vertexChunkIndex,
    );
  }

  async seekSrc(id) {
    if (
      this.adjListType !== AdjListType.UNORDERED_BY_SOURCE &&
      this.adjListType !== AdjListType.ORDERED_BY_SOURCE
    ) {
      return {
        ok: false,
        error: {
          code: 'Invalid',
          message: `The seekSrc operation is invalid in edge ${this.edgeInfo.edgeType} reader with ${adjListTypeToString(this.adjListType)} type.`,
        },
      };
    }

    const seekId = typeof id === 'bigint' ? id : BigInt(id);
    const newVertexChunkIndex = Number(seekId / BigInt(this.edgeInfo.srcChunkSize));
    if (BigInt(newVertexChunkIndex) >= this.vertexChunkNum) {
      return {
        ok: false,
        error: {
          code: 'IndexError',
          message: `The source internal id ${seekId} is out of range [0, ${BigInt(this.edgeInfo.srcChunkSize) * this.vertexChunkNum}) of edge ${this.edgeInfo.edgeType} reader.`,
        },
      };
    }
    if (this.chunkNum < 0 || this.vertexChunkIndex !== newVertexChunkIndex) {
      this.vertexChunkIndex = newVertexChunkIndex;
      await this.initOrUpdateEdgeChunkNum();
      this.chunkTable = null;
    }

    if (this.adjListType === AdjListType.UNORDERED_BY_SOURCE) {
      return await this.seek(0);
    }
    const [beginOffset] = await getAdjListOffsetOfVertex(
      this.prefix,
      this.edgeInfo,
      this.adjListType,
      seekId,
    );
    return await this.seek(beginOffset);
  }

  async seekDst(id) {
    if (
      this.adjListType !== AdjListType.UNORDERED_BY_DEST &&
      this.adjListType !== AdjListType.ORDERED_BY_DEST
    ) {
      return {
        ok: false,
        error: {
          code: 'Invalid',
          message: `The seekDst operation is invalid in edge ${this.edgeInfo.edgeType} reader with ${adjListTypeToString(this.adjListType)} type.`,
        },
      };
    }

    const seekId = typeof id === 'bigint' ? id : BigInt(id);
    const newVertexChunkIndex = Number(seekId / BigInt(this.edgeInfo.dstChunkSize));
    if (BigInt(newVertexChunkIndex) >= this.vertexChunkNum) {
      return {
        ok: false,
        error: {
          code: 'IndexError',
          message: `The destination internal id ${seekId} is out of range [0, ${BigInt(this.edgeInfo.dstChunkSize) * this.vertexChunkNum}) of edge ${this.edgeInfo.edgeType} reader.`,
        },
      };
    }
    if (this.chunkNum < 0 || this.vertexChunkIndex !== newVertexChunkIndex) {
      this.vertexChunkIndex = newVertexChunkIndex;
      await this.initOrUpdateEdgeChunkNum();
      this.chunkTable = null;
    }

    if (this.adjListType === AdjListType.UNORDERED_BY_DEST) {
      return await this.seek(0);
    }
    const [beginOffset] = await getAdjListOffsetOfVertex(
      this.prefix,
      this.edgeInfo,
      this.adjListType,
      seekId,
    );
    return await this.seek(beginOffset);
  }

  async seek(offset) {
    const seekOffset = typeof offset === 'bigint' ? offset : BigInt(offset);
    this.seekOffset = seekOffset;
    const preChunkIndex = this.chunkIndex;
    this.chunkIndex = Number(seekOffset / BigInt(this.edgeInfo.chunkSize));
    if (this.chunkIndex !== preChunkIndex) {
      this.chunkTable = null;
    }
    if (this.chunkNum < 0) {
      await this.initOrUpdateEdgeChunkNum();
    }
    if (BigInt(this.chunkIndex) >= this.chunkNum) {
      return {
        ok: false,
        error: {
          code: 'IndexError',
          message: `Seek offset ${seekOffset} is out of range: [0, ${this.chunkNum * BigInt(this.edgeInfo.chunkSize)})`,
        },
      };
    }
    return { ok: true };
  }

  async getChunk() {
    if (this.chunkTable === null) {
      console.log('this.vertexChunkIndex:', this.vertexChunkIndex);
      const edgeNum = await getEdgeNum(
        this.prefix,
        this.edgeInfo,
        this.adjListType,
        this.vertexChunkIndex,
      );
      if (edgeNum === 0) {
        return null;
      }
      const chunkFilePath = this.edgeInfo.getAdjListFilePath(
        this.vertexChunkIndex,
        this.chunkIndex,
        this.adjListType,
      );
      const path = this.prefix + chunkFilePath;
      const fileType = this.edgeInfo.getAdjacentList(
        this.adjListType,
      )?.fileType;
      this.chunkTable = await this.fs.readFileAsTable(path, fileType);
    }
    const rowOffset =
      this.seekOffset - BigInt(this.chunkIndex) * BigInt(this.edgeInfo.chunkSize);
    return this.chunkTable.slice(Number(rowOffset));
  }

  async nextChunk() {
    this.chunkIndex++;
    if (this.chunkNum < 0) {
      await this.initOrUpdateEdgeChunkNum();
    }
    while (this.chunkIndex >= this.chunkNum) {
      this.vertexChunkIndex++;
      if (this.vertexChunkIndex >= this.vertexChunkNum) {
        return {
          ok: false,
          error: {
            code: 'IndexError',
            message: `vertex chunk index ${this.vertexChunkIndex} is out of bounds for vertex chunk num ${this.vertexChunkNum}`,
          },
        };
      }
      this.chunkIndex = 0;
      await this.initOrUpdateEdgeChunkNum();
    }
    this.seekOffset = this.chunkIndex * this.edgeInfo.chunkSize;
    this.chunkTable = null;
    return { ok: true };
  }

  async getRowNumOfChunk() {
    if (this.chunkTable === null) {
      const chunkFilePath = this.edgeInfo.getAdjListFilePath(
        this.vertexChunkIndex,
        this.chunkIndex,
        this.adjListType,
      );
      const path = this.prefix + chunkFilePath;
      const fileType = this.edgeInfo.getAdjacentList(this.adjListType).fileType;
      this.chunkTable = await this.fs.readFileAsTable(path, fileType);
    }
    return this.chunkTable.numRows;
  }
}

class AdjListPropertyArrowChunkReader {
  constructor({
    edgeInfo,
    propertyGroup,
    adjListType,
    fs,
    prefix,
    pgPathPrefix,
    baseDir,
    vertexChunkNum,
    schema,
    vertexChunkIndex,
    seekOffset,
    chunkTable,
    filterOptions,
    chunkNum,
  }) {
    Object.assign(this, {
      edgeInfo,
      propertyGroup,
      adjListType,
      fs,
      prefix,
      pgPathPrefix,
      baseDir,
      vertexChunkNum,
      schema,
      vertexChunkIndex,
      seekOffset,
      chunkTable,
      filterOptions,
      chunkNum,
    });
  }

  static async create({
    edgeInfo,
    propertyGroup,
    adjListType,
    prefix,
    options = {},
  }) {
    const [fs, noUrlPath] = fileSystemFromUriOrPath(prefix);
    const pgPathPrefix = edgeInfo.getPropertyGroupPathPrefix(
      propertyGroup,
      adjListType,
    );
    const baseDir = noUrlPath + pgPathPrefix;
    const vertexChunkNum = await getVertexChunkNumFromEdge(
      prefix,
      edgeInfo,
      adjListType,
    );
    const schema = propertyGroupToSchema(propertyGroup, false);

    return new AdjListPropertyArrowChunkReader({
      edgeInfo,
      propertyGroup,
      adjListType,
      fs,
      prefix: noUrlPath,
      pgPathPrefix,
      baseDir,
      vertexChunkNum,
      chunkIndex: 0,
      vertexChunkIndex: 0,
      seekOffset: 0,
      schema,
      chunkTable: null,
      filterOptions: options,
      chunkNum: -1,
    });
  }

  async seekChunkIndex(vertexChunkIndex, chunkIndex = 0) {
    if (this.chunkNum < 0 || this.vertexChunkIndex !== vertexChunkIndex) {
      this.vertexChunkIndex = vertexChunkIndex;
      await this.initOrUpdateEdgeChunkNum();
      this.chunkTable = null;
    }
    if (this.chunkIndex !== chunkIndex) {
      this.chunkIndex = chunkIndex;
      this.seekOffset = chunkIndex * this.edgeInfo.chunkSize;
      this.chunkTable = null;
    }
  }

  async initOrUpdateEdgeChunkNum() {
    this.chunkNum = await this.edgeInfo.getEdgeChunkNum(
      this.prefix,
      this.adjListType,
      this.vertexChunkIndex,
    );
  }

  async seekSrc(id) {
    if (
      this.adjListType !== AdjListType.UNORDERED_BY_SOURCE &&
      this.adjListType !== AdjListType.ORDERED_BY_SOURCE
    ) {
      return {
        ok: false,
        error: {
          code: 'Invalid',
          message: `The seekSrc operation is invalid in edge ${this.edgeInfo.edgeType} reader with ${adjListTypeToString(this.adjListType)} type.`,
        },
      };
    }

    const seekId = typeof id === 'bigint' ? id : BigInt(id);
    const newVertexChunkIndex = Number(
      seekId / BigInt(this.edgeInfo.srcChunkSize),
    );
    if (BigInt(newVertexChunkIndex) >= this.vertexChunkNum) {
      return {
        ok: false,
        error: {
          code: 'IndexError',
          message: `The source internal id ${seekId} is out of range [0, ${BigInt(this.edgeInfo.srcChunkSize) * this.vertexChunkNum}) of edge ${this.edgeInfo.edgeType} reader.`,
        },
      };
    }
    if (this.chunkNum < 0 || this.vertexChunkIndex !== newVertexChunkIndex) {
      this.vertexChunkIndex = newVertexChunkIndex;
      await this.initOrUpdateEdgeChunkNum();
      this.chunkTable = null;
    }

    if (this.adjListType === AdjListType.UNORDERED_BY_SOURCE) {
      return await this.seek(0);
    }
    const [beginOffset] = await getAdjListOffsetOfVertex(
      this.prefix,
      this.edgeInfo,
      this.adjListType,
      seekId,
    );
    return await this.seek(beginOffset);
  }

  async seekDst(id) {
    if (
      this.adjListType !== AdjListType.UNORDERED_BY_DEST &&
      this.adjListType !== AdjListType.ORDERED_BY_DEST
    ) {
      return {
        ok: false,
        error: {
          code: 'Invalid',
          message: `The seekDst operation is invalid in edge ${this.edgeInfo.edgeType} reader with ${adjListTypeToString(this.adjListType)} type.`,
        },
      };
    }

    const seekId = typeof id === 'bigint' ? id : BigInt(id);
    const newVertexChunkIndex = Number(
      seekId / BigInt(this.edgeInfo.dstChunkSize),
    );
    if (BigInt(newVertexChunkIndex) >= this.vertexChunkNum) {
      return {
        ok: false,
        error: {
          code: 'IndexError',
          message: `The destination internal id ${seekId} is out of range [0, ${BigInt(this.edgeInfo.dstChunkSize) * this.vertexChunkNum}) of edge ${this.edgeInfo.edgeType} reader.`,
        },
      };
    }
    if (this.chunkNum < 0 || this.vertexChunkIndex !== newVertexChunkIndex) {
      this.vertexChunkIndex = newVertexChunkIndex;
      await this.initOrUpdateEdgeChunkNum();
      this.chunkTable = null;
    }

    if (this.adjListType === AdjListType.UNORDERED_BY_DEST) {
      return await this.seek(0);
    }
    const [beginOffset] = await getAdjListOffsetOfVertex(
      this.prefix,
      this.edgeInfo,
      this.adjListType,
      seekId,
    );
    return await this.seek(beginOffset);
  }

  async seek(offset) {
    const seekOffset = typeof offset === 'bigint' ? offset : BigInt(offset);
    const preChunkIndex = this.chunkIndex;
    this.seekOffset = seekOffset;
    this.chunkIndex = Number(seekOffset / BigInt(this.edgeInfo.chunkSize));
    if (this.chunkIndex !== preChunkIndex) {
      this.chunkTable = null;
    }
    if (this.chunkNum < 0) {
      await this.initOrUpdateEdgeChunkNum();
    }
    if (BigInt(this.chunkIndex) >= this.chunkNum) {
      return {
        ok: false,
        error: {
          code: 'IndexError',
          message: `The edge offset ${seekOffset} is out of range [0, ${BigInt(this.edgeInfo.chunkSize) * this.chunkNum}), edge type: ${this.edgeInfo.edgeType}`,
        },
      };
    }
    return { ok: true };
  }

  async getChunk() {
    if (this.chunkTable === null) {
      const edgeNum = await getEdgeNum(
        this.prefix,
        this.edgeInfo,
        this.adjListType,
        this.vertexChunkIndex,
      );
      if (edgeNum === 0n) {
        return null;
      }
      const chunkFilePath = this.edgeInfo.getPropertyFilePath(
        this.propertyGroup,
        this.adjListType,
        this.vertexChunkIndex,
        this.chunkIndex,
      );
      const path = this.prefix + chunkFilePath;
      this.chunkTable = await this.fs.readFileAsTable(
        path,
        this.propertyGroup.fileType,
      );
      if (this.schema !== null && this.filterOptions.filter === null) {
        this.chunkTable = castTableWithSchema(this.chunkTable, this.schema);
      }
    }
    const rowOffset =
      this.seekOffset - BigInt(this.chunkIndex) * BigInt(this.edgeInfo.chunkSize);
    return this.chunkTable.slice(Number(rowOffset));
  }

  async nextChunk() {
    this.chunkIndex++;
    if (this.chunkNum < 0) {
      await this.initOrUpdateEdgeChunkNum();
    }
    while (this.chunkIndex >= this.chunkNum) {
      this.vertexChunkIndex++;
      if (this.vertexChunkIndex >= this.vertexChunkNum) {
        return {
          ok: false,
          error: {
            code: 'IndexError',
            message: `vertex chunk index ${this.vertexChunkIndex} is out of bounds for vertex chunk num ${this.vertexChunkNum} of edge ${this.edgeInfo.edgeType} of adj list type ${adjListTypeToString(this.adjListType)}, property group ${this.propertyGroup}`,
          },
        };
      }
      this.chunkIndex = 0;
      await this.initOrUpdateEdgeChunkNum();
    }
    this.seekOffset = BigInt(this.chunkIndex) * BigInt(this.edgeInfo.chunkSize);
    this.chunkTable = null;
    return { ok: true };
  }
}

class AdjListOffsetArrowChunkReader {
  constructor({
    edgeInfo,
    adjListType,
    fs,
    prefix,
    baseDir,
    vertexChunkNum,
    vertexChunkSize,
    chunkIndex,
    seekId,
    chunkTable,
  }) {
    Object.assign(this, {
      edgeInfo,
      adjListType,
      fs,
      prefix,
      baseDir,
      vertexChunkNum,
      vertexChunkSize,
      chunkIndex,
      seekId,
      chunkTable,
    });
  }

  static async create({ edgeInfo, adjListType, prefix }) {
    const [fs, noUrlPath] = fileSystemFromUriOrPath(prefix);
    const dirPath = edgeInfo.getOffsetPathPrefix(adjListType);
    const baseDir = noUrlPath + dirPath;
    let vertexChunkNum = 0;
    if (
      adjListType === AdjListType.ORDERED_BY_SOURCE ||
      adjListType === AdjListType.ORDERED_BY_DEST
    ) {
      vertexChunkNum = await getVertexChunkNumFromEdge(
        prefix,
        edgeInfo,
        adjListType,
      );
    } else {
      throw new Error(
        `Invalid adjacent list type ${adjListType} to construct AdjListOffsetArrowChunkReader`,
      );
    }
    const vertexChunkSize =
      adjListType === AdjListType.ORDERED_BY_SOURCE
        ? edgeInfo.srcChunkSize
        : edgeInfo.dstChunkSize;

    return new AdjListOffsetArrowChunkReader({
      edgeInfo,
      adjListType,
      fs,
      prefix: noUrlPath,
      baseDir,
      vertexChunkNum,
      vertexChunkSize,
      chunkIndex: 0,
      seekId: 0,
      chunkTable: null,
    });
  }

  async seek(id) {
    const seekId = typeof id === 'bigint' ? id : BigInt(id);
    this.seekId = seekId;
    const preChunkIndex = this.chunkIndex;
    this.chunkIndex = Number(seekId / BigInt(this.vertexChunkSize));
    if (this.chunkIndex !== preChunkIndex) {
      this.chunkTable = null;
    }
    if (BigInt(this.chunkIndex) >= BigInt(this.vertexChunkNum)) {
      return {
        ok: false,
        error: {
          code: 'IndexError',
          message: `Internal vertex id ${seekId} is out of range [0, ${BigInt(this.vertexChunkNum) * BigInt(this.vertexChunkSize)}), of edge ${this.edgeInfo.edgeType} of adj list type ${adjListTypeToString(this.adjListType)}.`,
        },
      };
    }
    return { ok: true };
  }

  async getChunk() {
    if (this.chunkTable === null) {
      const chunkFilePath = this.edgeInfo.getAdjListOffsetFilePath(
        this.chunkIndex,
        this.adjListType,
      );
      const path = this.prefix + chunkFilePath;
      const fileType = this.edgeInfo.getAdjacentList(this.adjListType).fileType;
      this.chunkTable = await this.fs.readFileAsTable(path, fileType);
    }
    const rowOffset =
      this.seekId - BigInt(this.chunkIndex) * BigInt(this.vertexChunkSize);
    const slicedTable = this.chunkTable.slice(Number(rowOffset));
    const offsetColumn =
      slicedTable.getChildAt?.(0) ?? slicedTable.batches[0]?.getChildAt(0);
    if (!offsetColumn) {
      throw new Error(
        `Offset column not found for edge ${this.edgeInfo.edgeType} of adj list type ${adjListTypeToString(this.adjListType)}.`,
      );
    }
    return offsetColumn;
  }

  async nextChunk() {
    this.chunkIndex++;
    if (BigInt(this.chunkIndex) >= this.vertexChunkNum) {
      return {
        ok: false,
        error: {
          code: 'IndexError',
          message: `vertex chunk index ${this.chunkIndex} is out-of-bounds for vertex chunk num ${this.vertexChunkNum} of edge ${this.edgeInfo.edgeType} of adj list type ${adjListTypeToString(this.adjListType)}.`,
        },
      };
    }
    this.seekId = BigInt(this.chunkIndex) * BigInt(this.vertexChunkSize);
    this.chunkTable = null;
    return { ok: true };
  }
}

export {
  AdjListArrowChunkReader,
  AdjListOffsetArrowChunkReader,
  AdjListPropertyArrowChunkReader,
  VertexPropertyArrowChunkReader,
};
