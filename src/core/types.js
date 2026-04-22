import * as arrow from 'apache-arrow';

const Type = Object.freeze({
  BOOL: 0,
  INT32: 1,
  INT64: 2,
  FLOAT: 3,
  DOUBLE: 4,
  STRING: 5,
  LIST: 6,
  DATE: 7,
  TIMESTAMP: 8,
  USER_DEFINED: 9,
});

class DataType {
  constructor({ id, child, userDefinedTypeName = '' }) {
    Object.assign(this, { id, child, userDefinedTypeName });
  }

  toTypeName() {
    switch (this.id) {
      case Type.BOOL:
        return 'bool';
      case Type.INT32:
        return 'int32';
      case Type.INT64:
        return 'int64';
      case Type.FLOAT:
        return 'float';
      case Type.DOUBLE:
        return 'double';
      case Type.STRING:
        return 'string';
      case Type.LIST:
        return `list<${this.child.toTypeName()}>`;
      case Type.DATE:
        return 'date';
      case Type.TIMESTAMP:
        return 'timestamp';
      case Type.USER_DEFINED:
        return this.userDefinedTypeName;
      default:
        return 'unknown';
    }
  }

  static dataTypeToArrowDataType(type) {
    switch (type.id) {
      case Type.BOOL:
        return new arrow.Bool();
      case Type.INT32:
        return new arrow.Int32();
      case Type.INT64:
        return new arrow.Int64();
      case Type.FLOAT:
        return new arrow.Float32();
      case Type.DOUBLE:
        return new arrow.Float64();
      case Type.STRING:
        return new arrow.LargeUtf8();
      case Type.LIST: {
        if (!type.child) {
          throw new Error('List type must have child type');
        }
        const childDataType = DataType.dataTypeToArrowDataType(type.child);
        return new arrow.List(new arrow.Field('item', childDataType, true));
      }
      case Type.DATE:
        return new arrow.DateDay();
      case Type.TIMESTAMP:
        return new arrow.TimestampMillisecond();
      case Type.USER_DEFINED:
        throw new Error(
          'User defined type is not supported in this implementation',
        );
      default:
        throw new Error(`Unsupported data type id: ${type.id}`);
    }
  }

  static typeNameToDataType(typeStr) {
    switch (typeStr) {
      case 'bool':
        return new DataType({ id: Type.BOOL });
      case 'int32':
        return new DataType({ id: Type.INT32 });
      case 'int64':
        return new DataType({ id: Type.INT64 });
      case 'float':
        return new DataType({ id: Type.FLOAT });
      case 'double':
        return new DataType({ id: Type.DOUBLE });
      case 'string':
        return new DataType({ id: Type.STRING });
      case 'list<int32>':
        return new DataType({
          id: Type.LIST,
          child: DataType.typeNameToDataType('int32'),
        });
      case 'list<int64>':
        return new DataType({
          id: Type.LIST,
          child: DataType.typeNameToDataType('int64'),
        });
      case 'list<float>':
        return new DataType({
          id: Type.LIST,
          child: DataType.typeNameToDataType('float'),
        });
      case 'list<double>':
        return new DataType({
          id: Type.LIST,
          child: DataType.typeNameToDataType('double'),
        });
      case 'list<string>':
        return new DataType({
          id: Type.LIST,
          child: DataType.typeNameToDataType('string'),
        });
      case 'date':
        return new DataType({ id: Type.DATE });
      case 'timestamp':
        return new DataType({ id: Type.TIMESTAMP });
      default:
        throw new Error(`Unsupported data type ${typeStr}`);
    }
  }
}

const AdjListType = Object.freeze({
  UNORDERED_BY_SOURCE: 1,
  UNORDERED_BY_DEST: 2,
  ORDERED_BY_SOURCE: 4,
  ORDERED_BY_DEST: 8,
});

function orderedAlignedToAdjListType(ordered, aligned) {
  if (ordered) {
    return aligned === 'src'
      ? AdjListType.ORDERED_BY_SOURCE
      : AdjListType.ORDERED_BY_DEST;
  }
  return aligned === 'src'
    ? AdjListType.UNORDERED_BY_SOURCE
    : AdjListType.UNORDERED_BY_DEST;
}

function adjListTypeToString(adjListType) {
  switch (adjListType) {
    case AdjListType.UNORDERED_BY_SOURCE:
      return 'unordered_by_source';
    case AdjListType.UNORDERED_BY_DEST:
      return 'unordered_by_dest';
    case AdjListType.ORDERED_BY_SOURCE:
      return 'ordered_by_source';
    case AdjListType.ORDERED_BY_DEST:
      return 'ordered_by_dest';
    default:
      throw new Error(`Unsupported adjacent list type: ${adjListType}`);
  }
}

export {
  AdjListType,
  adjListTypeToString,
  DataType,
  orderedAlignedToAdjListType,
  Type,
};
