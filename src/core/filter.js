import * as arrow from 'apache-arrow';

function unique(items) {
  return [...new Set(items)];
}

function isStructuredExpression(filter) {
  return (
    filter != null &&
    typeof filter === 'object' &&
    typeof filter.type === 'string'
  );
}

function makePropertyExpression(name) {
  return {
    type: 'property',
    name,
  };
}

function makeLiteralExpression(value) {
  return {
    type: 'literal',
    value,
  };
}

function makeBinaryExpression(type, lhs, rhs) {
  return {
    type,
    lhs,
    rhs,
  };
}

function normalizeStructuredExpression(filter) {
  switch (filter.type) {
    case 'property':
    case 'literal':
      return filter;
    case 'not':
      return {
        type: 'not',
        expr: normalizeFilterExpression(filter.expr),
      };
    case 'and':
    case 'or':
    case 'eq':
    case 'ne':
    case 'gt':
    case 'gte':
    case 'lt':
    case 'lte':
      return {
        type: filter.type,
        lhs: normalizeFilterExpression(filter.lhs),
        rhs: normalizeFilterExpression(filter.rhs),
      };
    default:
      throw new Error(`Unsupported expression type: ${filter.type}`);
  }
}

function normalizeLegacyFilter(filter) {
  switch (filter.op) {
    case 'and':
    case 'or': {
      const normalizedChildren = (filter.filters ?? []).map(
        normalizeFilterExpression,
      );
      if (normalizedChildren.length === 0) {
        return makeLiteralExpression(filter.op === 'and');
      }
      let expression = normalizedChildren[0];
      for (const child of normalizedChildren.slice(1)) {
        expression = makeBinaryExpression(filter.op, expression, child);
      }
      return expression;
    }
    case 'not':
      return {
        type: 'not',
        expr: normalizeFilterExpression(filter.filter),
      };
    case 'eq':
    case 'ne':
    case 'gt':
    case 'gte':
    case 'lt':
    case 'lte':
      return makeBinaryExpression(
        filter.op,
        makePropertyExpression(filter.column),
        makeLiteralExpression(filter.value),
      );
    case 'in': {
      const values = filter.values ?? [];
      if (values.length === 0) {
        return makeLiteralExpression(false);
      }
      let expression = makeBinaryExpression(
        'eq',
        makePropertyExpression(filter.column),
        makeLiteralExpression(values[0]),
      );
      for (const value of values.slice(1)) {
        expression = makeBinaryExpression(
          'or',
          expression,
          makeBinaryExpression(
            'eq',
            makePropertyExpression(filter.column),
            makeLiteralExpression(value),
          ),
        );
      }
      return expression;
    }
    default:
      throw new Error(`Unsupported filter operator: ${filter.op}`);
  }
}

function normalizeFilterExpression(filter) {
  if (filter == null) {
    return null;
  }
  if (isStructuredExpression(filter)) {
    return normalizeStructuredExpression(filter);
  }
  return normalizeLegacyFilter(filter);
}

function getStructuredExpressionColumns(filter) {
  switch (filter.type) {
    case 'property':
      return [filter.name];
    case 'literal':
      return [];
    case 'not':
      return getStructuredExpressionColumns(filter.expr);
    case 'and':
    case 'or':
    case 'eq':
    case 'ne':
    case 'gt':
    case 'gte':
    case 'lt':
    case 'lte':
      return unique([
        ...getStructuredExpressionColumns(filter.lhs),
        ...getStructuredExpressionColumns(filter.rhs),
      ]);
    default:
      throw new Error(`Unsupported expression type: ${filter.type}`);
  }
}

function getFilterColumns(filter) {
  const normalizedFilter = normalizeFilterExpression(filter);
  if (normalizedFilter == null) {
    return [];
  }
  return getStructuredExpressionColumns(normalizedFilter);
}

function evaluateStructuredExpression(filter, row) {
  switch (filter.type) {
    case 'property':
      return row[filter.name];
    case 'literal':
      return filter.value;
    case 'not':
      return !evaluateStructuredExpression(filter.expr, row);
    case 'and':
      return (
        evaluateStructuredExpression(filter.lhs, row) &&
        evaluateStructuredExpression(filter.rhs, row)
      );
    case 'or':
      return (
        evaluateStructuredExpression(filter.lhs, row) ||
        evaluateStructuredExpression(filter.rhs, row)
      );
    case 'eq':
      return (
        evaluateStructuredExpression(filter.lhs, row) ===
        evaluateStructuredExpression(filter.rhs, row)
      );
    case 'ne':
      return (
        evaluateStructuredExpression(filter.lhs, row) !==
        evaluateStructuredExpression(filter.rhs, row)
      );
    case 'gt':
      return (
        evaluateStructuredExpression(filter.lhs, row) >
        evaluateStructuredExpression(filter.rhs, row)
      );
    case 'gte':
      return (
        evaluateStructuredExpression(filter.lhs, row) >=
        evaluateStructuredExpression(filter.rhs, row)
      );
    case 'lt':
      return (
        evaluateStructuredExpression(filter.lhs, row) <
        evaluateStructuredExpression(filter.rhs, row)
      );
    case 'lte':
      return (
        evaluateStructuredExpression(filter.lhs, row) <=
        evaluateStructuredExpression(filter.rhs, row)
      );
    default:
      throw new Error(`Unsupported expression type: ${filter.type}`);
  }
}

function evaluateFilterExpression(filter, row) {
  const normalizedFilter = normalizeFilterExpression(filter);
  if (normalizedFilter == null) {
    return true;
  }
  return Boolean(evaluateStructuredExpression(normalizedFilter, row));
}

function applyFilterToTable(table, filter) {
  if (filter == null || table.numRows === 0) {
    return table;
  }

  const normalizedFilter = normalizeFilterExpression(filter);
  if (normalizedFilter == null) {
    return table;
  }

  const rows = [];
  for (let rowIndex = 0; rowIndex < table.numRows; rowIndex++) {
    const row = table.get(rowIndex);
    if (evaluateStructuredExpression(normalizedFilter, row)) {
      rows.push(row);
    }
  }

  if (rows.length === 0) {
    return table.slice(0, 0);
  }

  const columns = Object.fromEntries(
    table.schema.fields.map((field) => [
      field.name,
      rows.map((row) => row[field.name]),
    ]),
  );
  return arrow.tableFromArrays(columns);
}

function validateSelectedColumns(columns, schema, propertyNames = []) {
  if (columns == null) {
    return;
  }

  for (const column of columns) {
    if (propertyNames.length > 0 && !propertyNames.includes(column)) {
      throw new Error(`Column ${column} is not in select properties.`);
    }
    if (!schema.fields.find((field) => field.name === column)) {
      throw new Error(`Column ${column} not found in schema.`);
    }
  }
}

function prepareReadOptions({
  schema,
  propertyNames = [],
  selectedColumns = null,
  filter = null,
}) {
  validateSelectedColumns(selectedColumns, schema, propertyNames);

  const normalizedFilter = normalizeFilterExpression(filter);
  const projectionColumns =
    selectedColumns ?? (propertyNames.length > 0 ? propertyNames : null);
  const filterColumns = getFilterColumns(normalizedFilter);
  validateSelectedColumns(filterColumns, schema, propertyNames);

  const readColumns =
    projectionColumns == null
      ? undefined
      : unique([...projectionColumns, ...filterColumns]);

  return {
    projectionColumns,
    readColumns,
  };
}

export {
  applyFilterToTable,
  evaluateFilterExpression,
  getFilterColumns,
  normalizeFilterExpression,
  prepareReadOptions,
};
