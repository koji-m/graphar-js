import * as arrow from 'apache-arrow';
import { fileSystemFromUriOrPath } from './filesystem.js';
import GENERAL_PARAMS from './general-params.js';
import {
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

  async seek(offset) {
    this.seekOffset = offset;
    const preChunkIndex = this.chunkIndex;
    this.chunkIndex = Math.floor(offset / this.edgeInfo.chunkSize);
    if (this.chunkIndex !== preChunkIndex) {
      this.chunkTable = null;
    }
    if (this.chunkNum < 0) {
      await this.initOrUpdateEdgeChunkNum();
    }
    if (this.chunkIndex >= this.chunkNum) {
      return {
        ok: false,
        error: {
          code: 'IndexError',
          message: `Seek offset ${offset} is out of range: [0, ${this.chunkNum * BigInt(this.edgeInfo.chunkSize)})`,
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
      this.seekOffset - this.chunkIndex * this.edgeInfo.chunkSize;
    return this.chunkTable.slice(rowOffset);
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
    // TODO
    // const schema = propertyGroupToSchema(propertyGroup, false);

    return new AdjListPropertyArrowChunkReader({
      edgeInfo,
      propertyGroup,
      adjListType,
      fs,
      prefix: noUrlPath,
      pgPathPrefix,
      baseDir,
      vertexChunkNum,
      vertexChunkIndex: 0,
      seekOffset: 0,
      schema: null,
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

  async nextChunk() {
    this.chunkIndex++;
    if (this.chunkNum < 0) {
      this.initOrUpdateEdgeChunkNum();
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
      this.initOrUpdateEdgeChunkNum();
    }
    this.seekOffset = this.chunkIndex * this.edgeInfo.chunkSize;
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

    return new AdjListOffsetArrowChunkReader({
      edgeInfo,
      adjListType,
      fs,
      prefix: noUrlPath,
      baseDir,
      vertexChunkNum,
      chunkIndex: 0,
      seekId: 0,
      chunkTable: null,
    });
  }
}

export {
  AdjListArrowChunkReader,
  AdjListOffsetArrowChunkReader,
  AdjListPropertyArrowChunkReader,
  VertexPropertyArrowChunkReader,
};
