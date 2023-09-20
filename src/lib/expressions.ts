type Value = string | number | boolean;
type Operator = "=" | "<" | "<=" | ">" | ">=" | "begins_with";
export type Expression = Value | [Operator, Value] | ["between", Value, Value];

export function toKeyConditionExpression(keys: Record<string, Expression>) {
  const values = Object.entries(keys)
    .map(([key, expr]) => toExpressionValue(key, expr))
    .reduce((values, current) => ({ ...values, ...current }), {});
  const expression = Object.entries(keys)
    .map(([key, expr]) => toExpression(key, expr))
    .join(" and ");

  return {
    values,
    expression,
  };
}

export function toFilterConditionExpression(filter?: Record<string, Expression>) {
  if (!filter) {
    return {
      expression: undefined,
      values: {},
    };
  }

  return toKeyConditionExpression(filter);
}

function toExpression(key: string, expr: Expression) {
  const operator = expr instanceof Array ? expr[0] : "=";
  switch (operator) {
    case "between":
      return `${key} between :${key}min and :${key}max`;
    case "begins_with":
      return `begins_with(${key}, :${key})`;
    default:
      return `${key} ${operator} :${key}`;
  }
}

function toExpressionValue(key: string, expr: Expression): Record<string, Value> {
  const operator = expr instanceof Array ? expr[0] : "=";
  const value = expr instanceof Array ? expr[1] : expr;
  switch (operator) {
    case "between":
      return {
        [`:${key}min`]: (expr as Value[])[1],
        [`:${key}max`]: (expr as Value[])[2],
      };
    default:
      return {
        [`:${key}`]: value,
      };
  }
}

export function toExpressionAttributeValues(params: Record<string, any>) {
  return Object.entries(params)
    .map(([key, value]) => [`:${key}`, value])
    .reduce((hash, [key, value]) => ({ ...hash, [key]: value }), {});
}
