import { describe, expect, it } from 'vitest';
import {
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
} from '../src/index.js';
import {
  evaluateFilterExpression,
  getFilterColumns,
} from '../src/core/filter.js';

describe('Expression builders', () => {
  it('builds property and literal expressions with C++-style helpers', () => {
    expect(_Property('name')).toEqual({
      type: 'property',
      name: 'name',
    });
    expect(_Literal('Ann')).toEqual({
      type: 'literal',
      value: 'Ann',
    });
  });

  it('collects referenced property columns from nested expressions', () => {
    const filter = _And(
      _Equal(_Property('firstName'), _Literal('Ann')),
      _Not(_LessThan(_Property('id'), _Literal(100n))),
    );

    expect(getFilterColumns(filter)).toEqual(['firstName', 'id']);
  });

  it('evaluates binary comparison helpers', () => {
    const row = { id: 100n, firstName: 'Ann', active: true };

    expect(
      evaluateFilterExpression(
        _Equal(_Property('firstName'), _Literal('Ann')),
        row,
      ),
    ).toBe(true);
    expect(
      evaluateFilterExpression(
        _NotEqual(_Property('firstName'), _Literal('Bob')),
        row,
      ),
    ).toBe(true);
    expect(
      evaluateFilterExpression(_GreaterThan(_Property('id'), _Literal(99n)), row),
    ).toBe(true);
    expect(
      evaluateFilterExpression(_GreaterEqual(_Property('id'), _Literal(100n)), row),
    ).toBe(true);
    expect(
      evaluateFilterExpression(_LessThan(_Property('id'), _Literal(101n)), row),
    ).toBe(true);
    expect(
      evaluateFilterExpression(_LessEqual(_Property('id'), _Literal(100n)), row),
    ).toBe(true);
  });

  it('evaluates logical helpers recursively', () => {
    const row = { id: 100n, firstName: 'Ann', active: true };
    const filter = _Or(
      _And(
        _Equal(_Property('firstName'), _Literal('Ann')),
        _Equal(_Property('active'), _Literal(true)),
      ),
      _Not(_Equal(_Property('id'), _Literal(100n))),
    );

    expect(evaluateFilterExpression(filter, row)).toBe(true);
  });

  it('rejects unsupported helper inputs', () => {
    expect(() => _Property('')).toThrow(/Property name must be a non-empty string/);
    expect(() => _Literal({ value: 1 })).toThrow(/Unsupported literal type/);
    expect(() => _Equal('lhs', _Literal(1))).toThrow(/Left-hand side must be an expression/);
    expect(() => _Not(null)).toThrow(/Expression must be an expression/);
  });
});
