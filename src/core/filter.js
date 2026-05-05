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

function getStructuredExpressionColumns(filter) {
  switch (filter.type) {
    case 'property':
      return [filter.name];
    case 'literal':
      return [];
    case 'not':
      return getFilterColumns(filter.expr);
    case 'and':
    case 'or':
    case 'eq':
    case 'ne':
    case 'gt':
    case 'gte':
    case 'lt':
    case 'lte':
      return unique([
        ...getFilterColumns(filter.lhs),
        ...getFilterColumns(filter.rhs),
      ]);
    default:
      throw new Error(`Unsupported expression type: ${filter.type}`);
  }
}

function getFilterColumns(filter) {
  if (filter == null) {
    return [];
  }

  if (isStructuredExpression(filter)) {
    return getStructuredExpressionColumns(filter);
  }

  switch (filter.op) {
    case 'and':
    case 'or':
      return unique((filter.filters ?? []).flatMap(getFilterColumns));
    case 'not':
      return getFilterColumns(filter.filter);
    case 'eq':
    case 'ne':
    case 'gt':
    case 'gte':
    case 'lt':
    case 'lte':
    case 'in':
      return [filter.column];
    default:
      throw new Error(`Unsupported filter operator: ${filter.op}`);
  }
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
  if (filter == null) {
    return true;
  }

  if (isStructuredExpression(filter)) {
    return Boolean(evaluateStructuredExpression(filter, row));
  }

  switch (filter.op) {
    case 'and':
      return (filter.filters ?? []).every((child) =>
        evaluateFilterExpression(child, row),
      );
    case 'or':
      return (filter.filters ?? []).some((child) =>
        evaluateFilterExpression(child, row),
      );
    case 'not':
      return !evaluateFilterExpression(filter.filter, row);
    case 'eq':
      return row[filter.column] === filter.value;
    case 'ne':
      return row[filter.column] !== filter.value;
    case 'gt':
      return row[filter.column] > filter.value;
    case 'gte':
      return row[filter.column] >= filter.value;
    case 'lt':
      return row[filter.column] < filter.value;
    case 'lte':
      return row[filter.column] <= filter.value;
    case 'in':
      return (filter.values ?? []).includes(row[filter.column]);
    default:
      throw new Error(`Unsupported filter operator: ${filter.op}`);
  }
}

function applyFilterToTable(table, filter) {
  if (filter == null || table.numRows === 0) {
    return table;
  }

  const rows = [];
  for (let rowIndex = 0; rowIndex < table.numRows; rowIndex++) {
    const row = table.get(rowIndex);
    if (evaluateFilterExpression(filter, row)) {
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

  const projectionColumns =
    selectedColumns ?? (propertyNames.length > 0 ? propertyNames : null);
  const filterColumns = getFilterColumns(filter);
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
  prepareReadOptions,
};
