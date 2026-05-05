function assertExpressionOperand(value, name) {
  if (value == null || typeof value !== 'object') {
    throw new Error(`${name} must be an expression.`);
  }
}

function assertLiteralType(value) {
  const valueType = typeof value;
  if (
    valueType !== 'boolean' &&
    valueType !== 'number' &&
    valueType !== 'bigint' &&
    valueType !== 'string'
  ) {
    throw new Error(
      `Unsupported literal type: ${valueType}. Only boolean, number, bigint, and string are supported.`,
    );
  }
}

function _Property(name) {
  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('Property name must be a non-empty string.');
  }
  return {
    type: 'property',
    name,
  };
}

function _Literal(value) {
  assertLiteralType(value);
  return {
    type: 'literal',
    value,
  };
}

function _Not(expr) {
  assertExpressionOperand(expr, 'Expression');
  return {
    type: 'not',
    expr,
  };
}

function makeBinaryExpression(type, lhs, rhs) {
  assertExpressionOperand(lhs, 'Left-hand side');
  assertExpressionOperand(rhs, 'Right-hand side');
  return {
    type,
    lhs,
    rhs,
  };
}

function _Equal(lhs, rhs) {
  return makeBinaryExpression('eq', lhs, rhs);
}

function _NotEqual(lhs, rhs) {
  return makeBinaryExpression('ne', lhs, rhs);
}

function _GreaterThan(lhs, rhs) {
  return makeBinaryExpression('gt', lhs, rhs);
}

function _GreaterEqual(lhs, rhs) {
  return makeBinaryExpression('gte', lhs, rhs);
}

function _LessThan(lhs, rhs) {
  return makeBinaryExpression('lt', lhs, rhs);
}

function _LessEqual(lhs, rhs) {
  return makeBinaryExpression('lte', lhs, rhs);
}

function _And(lhs, rhs) {
  return makeBinaryExpression('and', lhs, rhs);
}

function _Or(lhs, rhs) {
  return makeBinaryExpression('or', lhs, rhs);
}

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
};
