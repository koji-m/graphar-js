import yaml from 'js-yaml';
import { fileSystemFromUriOrPath } from './filesystem.js';
import { InfoVersion } from './info-version.js';
import {
  adjListTypeToString,
  DataType,
  orderedAlignedToAdjListType,
} from './types.js';

function pathToDirectory(path) {
  if (path.startsWith('http://') || path.startsWith('https://')) {
    const url = new URL(path);
    const pathname = url.pathname;
    const lastSlashIndex = pathname.lastIndexOf('/');
    if (lastSlashIndex === -1) {
      return path; // No slash found, return the original path
    }
    const directoryPath = pathname.substring(0, lastSlashIndex + 1);
    return url.origin + directoryPath;
  } else {
    const lastSlashIndex = path.lastIndexOf('/');
    if (lastSlashIndex === -1) {
      return path; // No slash found, return the original path
    }
    return path.substring(0, lastSlashIndex + 1);
  }
}

class Property {
  constructor({ name, type, isPrimary, isNullable, cardinality }) {
    Object.assign(this, { name, type, isPrimary, cardinality });
    this.isNullable = !isPrimary && isNullable;
  }
}

class PropertyGroup {
  constructor({ properties, fileType, prefix }) {
    Object.assign(this, { properties, fileType, prefix });
    if (this.prefix === undefined && this.properties.length > 0) {
      this.prefix = `${this.properties.map((prop) => prop.name).join('_')}/`;
    }
  }
}

class AdjacentList {
  constructor(type, fileType, prefix) {
    Object.assign(this, { type, fileType, prefix });
    if (this.prefix.length === 0) {
      this.prefix = `${adjListTypeToString(this.type)}/`;
    }
  }
}

class VertexInfo {
  constructor({ type, chunkSize, prefix, version, labels, propertyGroups }) {
    Object.assign(this, {
      type,
      chunkSize,
      prefix,
      version,
      labels,
      propertyGroups,
    });
  }

  static load(vertexMeta) {
    const type = vertexMeta.type;
    const chunkSize = vertexMeta.chunk_size;
    const prefix = vertexMeta.prefix;
    const version = InfoVersion.parse(vertexMeta.version);
    const labels = Array.isArray(vertexMeta.labels) ? vertexMeta.labels : [];
    const propertyGroups = [];
    for (const propertyGroupMeta of vertexMeta.property_groups ?? []) {
      const prefix = propertyGroupMeta.prefix;
      const fileType = propertyGroupMeta.file_type;
      const properties = [];
      for (const propertyMeta of propertyGroupMeta.properties ?? []) {
        const name = propertyMeta.name;
        const type = DataType.typeNameToDataType(propertyMeta.data_type);
        const isPrimary = propertyMeta.is_primary ?? false;
        const isNullable = propertyMeta.is_nullable ?? true;
        const cardinality = propertyMeta.cardinality ?? 'single';
        properties.push(
          new Property({ name, type, isPrimary, isNullable, cardinality }),
        );
      }
      propertyGroups.push(new PropertyGroup({ properties, fileType, prefix }));
    }
    return new VertexInfo({
      type,
      chunkSize,
      prefix,
      version,
      labels,
      propertyGroups,
    });
  }

  getVerticesNumFilePath() {
    return `${this.prefix}vertex_count`;
  }

  getPathPrefix(propertyGroup) {
    return `${this.prefix}${propertyGroup.prefix}`;
  }

  getFilePath(propertyGroup, chunkIndex) {
    return `${this.prefix}${propertyGroup.prefix}chunk${chunkIndex}`;
  }
}

class EdgeInfo {
  constructor({
    srcType,
    edgeType,
    dstType,
    chunkSize,
    srcChunkSize,
    dstChunkSize,
    directed,
    prefix,
    version,
    adjacentList,
    propertyGroups,
  }) {
    Object.assign(this, {
      srcType,
      edgeType,
      dstType,
      chunkSize,
      srcChunkSize,
      dstChunkSize,
      directed,
      prefix,
      version,
      adjacentList,
      propertyGroups,
    });
    this.adjacentListTypeToIndex = Object.fromEntries(
      this.adjacentList.map((adjList, index) => [adjList.type, index]),
    );
  }

  static load(edgeMeta) {
    const srcType = edgeMeta.src_type;
    const edgeType = edgeMeta.edge_type;
    const dstType = edgeMeta.dst_type;
    const chunkSize = edgeMeta.chunk_size;
    const srcChunkSize = edgeMeta.src_chunk_size;
    const dstChunkSize = edgeMeta.dst_chunk_size;
    const directed = edgeMeta.directed;
    const prefix = edgeMeta.prefix ?? '';
    const version = InfoVersion.parse(edgeMeta.version);

    const adjacentList = Array.isArray(edgeMeta.adj_lists)
      ? edgeMeta.adj_lists.map((adjList) => {
          const ordered = adjList.ordered;
          const aligned = adjList.aligned_by;
          const adjListType = orderedAlignedToAdjListType(ordered, aligned);
          const fileType = adjList.file_type;
          const adjListPrefix = adjList.prefix ?? '';
          return new AdjacentList(adjListType, fileType, adjListPrefix);
        })
      : [];

    const propertyGroups = Array.isArray(edgeMeta.property_groups)
      ? edgeMeta.property_groups.map((propertyGroupMeta) => {
          const prefix = propertyGroupMeta.prefix;
          const fileType = propertyGroupMeta.file_type;
          const properties = propertyGroupMeta.properties
            ? propertyGroupMeta.properties.map((propertyMeta) => {
                const name = propertyMeta.name;
                const type = DataType.typeNameToDataType(
                  propertyMeta.data_type,
                );
                const isPrimary = propertyMeta.is_primary ?? false;
                const isNullable = propertyMeta.is_nullable ?? true;
                const cardinality = propertyMeta.cardinality;
                if (cardinality !== undefined && cardinality !== 'single') {
                  throw new Error(
                    `Unsupported cardinality: ${cardinality} for edge property`,
                  );
                }
                return new Property({ name, type, isPrimary, isNullable });
              })
            : [];
          return new PropertyGroup({ properties, fileType, prefix });
        })
      : [];

    return new EdgeInfo({
      srcType,
      edgeType,
      dstType,
      chunkSize,
      srcChunkSize,
      dstChunkSize,
      directed,
      prefix,
      version,
      adjacentList,
      propertyGroups,
    });
  }

  getVerticesNumFilePath(adjListType) {
    const adjListIndex = this.adjacentListTypeToIndex[adjListType];
    if (adjListIndex === undefined) {
      throw new Error(
        `Adjacent list type ${adjListType} not found in edge info.`,
      );
    }
    return `${this.prefix}${this.adjacentList[adjListIndex].prefix}vertex_count`;
  }

  getEdgesNumFilePath(vertexChunkIndex, adjListType) {
    const adjListIndex = this.adjacentListTypeToIndex[adjListType];
    if (adjListIndex === undefined) {
      throw new Error(
        `Adjacent list type ${adjListType} not found in edge info.`,
      );
    }
    return (
      this.prefix +
      this.adjacentList[adjListIndex].prefix +
      'edge_count' +
      vertexChunkIndex
    );
  }

  async getEdgeNum(prefix, adjListType, vertexChunkIndex) {
    const [fs, outPrefix] = fileSystemFromUriOrPath(prefix);
    const edgeNumFileSuffix = this.getEdgesNumFilePath(
      vertexChunkIndex,
      adjListType,
    );
    const edgeNumFilePath = outPrefix + edgeNumFileSuffix;
    return await fs.readFileAsSingleUint64(edgeNumFilePath);
  }

  async getEdgeChunkNum(prefix, adjListType, vertexChunkIndex) {
    const edgeNum = await this.getEdgeNum(
      prefix,
      adjListType,
      vertexChunkIndex,
    );
    return (edgeNum + BigInt(this.chunkSize) - 1n) / BigInt(this.chunkSize);
  }

  hasAdjacentListType(adjListType) {
    return this.adjacentList.some((adjList) => adjList.type === adjListType);
  }

  getAdjacentList(adjListType) {
    return this.adjacentList.find((adjList) => adjList.type === adjListType);
  }

  getAdjListPathPrefix(adjListType) {
    const adjList = this.adjacentList.find(
      (adjList) => adjList.type === adjListType,
    );
    if (!adjList) {
      throw new Error(
        `Adjacent list type ${adjListType} not found in edge info.`,
      );
    }
    return `${this.prefix}${adjList.prefix}adj_list/`;
  }

  getAdjListFilePath(vertexChunkIndex, edgeChunkIndex, adjListType) {
    const adjListPathPrefix = this.getAdjListPathPrefix(adjListType);
    return `${adjListPathPrefix}part${vertexChunkIndex}/chunk${edgeChunkIndex}`;
  }

  getPropertyGroupPathPrefix(propertyGroup, adjListType) {
    const adjList = this.adjacentList.find(
      (adjList) => adjList.type === adjListType,
    );
    return `${this.prefix}${adjList.prefix}${propertyGroup.prefix}`;
  }

  getOffsetPathPrefix(adjListType) {
    const adjList = this.adjacentList.find(
      (adjList) => adjList.type === adjListType,
    );
    return `${this.prefix}${adjList.prefix}offset/`;
  }
}

class GraphInfo {
  constructor(
    graphName = '',
    vertexInfos = [],
    edgeInfos = [],
    labels = [],
    prefix = './',
    version = null,
    extraInfo = {},
  ) {
    Object.assign(this, {
      graphName,
      vertexInfos,
      edgeInfos,
      labels,
      prefix,
      version,
      extraInfo,
    });
  }

  static async constructGraphInfo(
    graphMeta,
    defaultName,
    defaultPrefix,
    fs,
    noUrlPathPrefix,
  ) {
    const name = graphMeta.name ?? defaultName;
    const prefix = graphMeta.prefix ?? defaultPrefix;
    const version = InfoVersion.parse(graphMeta.version);

    // TODO: Load extra_info

    const vertexInfos = Array.isArray(graphMeta.vertices)
      ? await (async () =>
          await Promise.all(
            graphMeta.vertices.map(async (vertexMetaFile) => {
              const input = await fs.readFileAsText(
                noUrlPathPrefix + vertexMetaFile,
              );
              const vertexMeta = yaml.load(input);
              return VertexInfo.load(vertexMeta);
            }),
          ))()
      : [];

    const edgeInfos = Array.isArray(graphMeta.edges)
      ? await (async () =>
          await Promise.all(
            graphMeta.edges.map(async (edgeMetaFile) => {
              const input = await fs.readFileAsText(
                noUrlPathPrefix + edgeMetaFile,
              );
              const edgeMeta = yaml.load(input);
              return EdgeInfo.load(edgeMeta);
            }),
          ))()
      : [];

    const labels = Array.isArray(graphMeta.labels) ? graphMeta.labels : [];

    return new GraphInfo(name, vertexInfos, edgeInfos, labels, prefix, version);
  }

  static async load({ path, input, _relativeLocation }) {
    if (path) {
      const [fs, noUrlPath] = fileSystemFromUriOrPath(path);
      const input = await fs.readFileAsText(noUrlPath);
      const graphMeta = yaml.load(input);
      const defaultName = 'graph';
      const defaultPrefix = pathToDirectory(path);
      const noUrlPathPrefix = pathToDirectory(noUrlPath);
      return await GraphInfo.constructGraphInfo(
        graphMeta,
        defaultName,
        defaultPrefix,
        fs,
        noUrlPathPrefix,
      );
    }
    if (input) {
      // TODO
    }
  }

  getVertexInfo(type) {
    return this.vertexInfos.find((vertexInfo) => vertexInfo.type === type);
  }

  getEdgeInfo(srcType, edgeType, dstType) {
    return this.edgeInfos.find(
      (edgeInfo) =>
        edgeInfo.srcType === srcType &&
        edgeInfo.edgeType === edgeType &&
        edgeInfo.dstType === dstType,
    );
  }
}

export { EdgeInfo, GraphInfo, VertexInfo };
