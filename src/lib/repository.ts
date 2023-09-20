import { DynamoDBDocument, TransactWriteCommandInput, paginateQuery, paginateScan } from "@aws-sdk/lib-dynamodb";
import { z } from "zod";
import { Expression, toFilterConditionExpression, toKeyConditionExpression } from "./expressions";

type QueryParams = {
  index?: string;
};

export class NotFoundError extends Error {}

export class Repository<T extends z.ZodTypeAny> {
  constructor(
    public dynamodb: DynamoDBDocument,
    public readonly tableName: string,
    protected readonly schema: T,
  ) {}

  private parse(data: z.input<T>): z.output<T> {
    return this.schema.parse(data);
  }

  public async put(data: z.input<T>) {
    const parsed = this.parse(data);

    await this.dynamodb.put({
      Item: parsed,
      TableName: this.tableName,
    });

    return parsed;
  }

  public async get(key: Record<string, string | number>) {
    const { Item } = await this.dynamodb.get({
      TableName: this.tableName,
      Key: key,
    });

    if (!Item) {
      throw new NotFoundError(`Item not found in ${this.tableName} for keys ${JSON.stringify(key)}`);
    }

    return this.parse(Item);
  }

  public async *scan() {
    const paginator = paginateScan(
      {
        client: this.dynamodb,
      },
      {
        TableName: this.tableName,
      },
    );

    for await (const page of paginator) {
      for (const item of page.Items ?? []) {
        yield this.parse(item);
      }
    }
  }

  public async *query(keys: Record<string, Expression>, params?: QueryParams) {
    const { expression, values } = toKeyConditionExpression(keys);

    const paginator = paginateQuery(
      {
        client: this.dynamodb,
      },
      {
        TableName: this.tableName,
        KeyConditionExpression: expression,
        ExpressionAttributeValues: values,
        IndexName: params?.index,
      },
    );

    for await (const page of paginator) {
      for (const item of page.Items ?? []) {
        yield this.parse(item);
      }
    }
  }
}
