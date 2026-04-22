import { describe, expect, it } from 'vitest';
import { DataType, Type } from '../src/core/types.js';

describe('DataType', () => {
  it.each([
    ['bool', Type.BOOL],
    ['int32', Type.INT32],
    ['int64', Type.INT64],
    ['float', Type.FLOAT],
    ['double', Type.DOUBLE],
    ['string', Type.STRING],
    ['date', Type.DATE],
    ['timestamp', Type.TIMESTAMP],
  ])('converts primitive type name %s to DataType', (typeName, typeId) => {
    const dataType = DataType.typeNameToDataType(typeName);

    expect(dataType.id).toBe(typeId);
    expect(dataType.toTypeName()).toBe(typeName);
  });

  it.each([
    ['list<int32>', Type.INT32],
    ['list<int64>', Type.INT64],
    ['list<float>', Type.FLOAT],
    ['list<double>', Type.DOUBLE],
    ['list<string>', Type.STRING],
  ])('converts nested type name %s to list DataType', (typeName, childTypeId) => {
    const dataType = DataType.typeNameToDataType(typeName);

    expect(dataType.id).toBe(Type.LIST);
    expect(dataType.child).toBeInstanceOf(DataType);
    expect(dataType.child.id).toBe(childTypeId);
    expect(dataType.toTypeName()).toBe(typeName);
  });

  it('rejects unsupported type names', () => {
    expect(() => DataType.typeNameToDataType('date32')).toThrow(
      /Unsupported data type date32/,
    );
  });
});
