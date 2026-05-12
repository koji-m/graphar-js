export { initWasm } from './core/filesystem.js';
export {
  _And,
  _Equal,
  _GreaterEqual,
  _GreaterThan,
  _LessEqual,
  _LessThan,
  _Literal,
  _Not,
  _NotEqual,
  _Or,
  _Property,
} from './core/expression.js';
export {
  AdjacentList,
  EdgeInfo,
  GraphInfo,
  PropertyGroup,
  VertexInfo,
} from './core/graph-info.js';
export {
  EdgesCollection,
  PropertyList,
  VerticesCollection,
} from './core/graph-reader.js';
export { AdjListType, FileType } from './core/types.js';
