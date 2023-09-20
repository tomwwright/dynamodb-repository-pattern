import { BatchWriteCommandInput, DynamoDBDocument, paginateQuery } from "@aws-sdk/lib-dynamodb";
import { DynamoDB, DynamoDBServiceException } from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";

const config = {
  endpoint: "http://localhost:4567",
  region: "local",
  credentials: { accessKeyId: "local", secretAccessKey: "local" },
};

const tableName = "testing";

describe("using DynamoDB and DynamoDBDocument", () => {
  const dynamodb = new DynamoDB(config);
  const document = DynamoDBDocument.from(dynamodb);

  beforeAll(async () => {
    await dynamodb.createTable({
      TableName: tableName,
      AttributeDefinitions: [
        {
          AttributeName: "pk",
          AttributeType: "S",
        },
        {
          AttributeName: "sk",
          AttributeType: "S",
        },
        {
          AttributeName: "userId",
          AttributeType: "S",
        },
      ],
      KeySchema: [
        {
          AttributeName: "pk",
          KeyType: "HASH", // partition key
        },
        {
          AttributeName: "sk",
          KeyType: "RANGE", // sort key
        },
      ],
      GlobalSecondaryIndexes: [
        {
          IndexName: "ByUser",
          KeySchema: [
            {
              AttributeName: "userId",
              KeyType: "HASH",
            },
            {
              AttributeName: "sk",
              KeyType: "RANGE",
            },
          ],
          Projection: {
            ProjectionType: "ALL",
          },
        },
      ],
      BillingMode: "PAY_PER_REQUEST",
    });

    const batchOfItems: BatchWriteCommandInput = {
      RequestItems: {
        [tableName]: [
          ["post1", "post", "user1", "my cool post"],
          ["post1", "comment#1", "user2", "blah"],
          ["post1", "comment#2", "user3", "hello"],
          ["post2", "post", "user2", "i also post stuff"],
          ["post2", "comment#1", "user1", ":)"],
          ["post2", "comment#2", "user3", ":D"],
          ["post2", "comment#3", "user1", ":D"],
          ["post2", "comment#4", "user1", ":("],
        ].map(([pk, sk, userId, content]) => ({
          PutRequest: {
            Item: {
              pk,
              sk,
              userId,
              content,
            },
          },
        })),
      },
    };

    await document.batchWrite(batchOfItems);
  });

  describe("using DynamoDB", () => {
    it("does not handle marshalling and unmarshalling for you", async () => {
      await dynamodb.putItem({
        TableName: tableName,
        Item: {
          pk: { S: "test" },
          sk: { S: "test" },
          userId: { S: "test" },
          somethingElse: { N: "3" },
        },
      });

      const { Item } = await dynamodb.getItem({
        TableName: tableName,
        Key: {
          pk: { S: "test" },
          sk: { S: "test" },
        },
      });

      expect(Item).toEqual({
        pk: { S: "test" },
        sk: { S: "test" },
        userId: { S: "test" },
        somethingElse: { N: "3" },
      });
    });

    it("can be made a bit easier with marshall() and unmarshall()", () => {
      expect(
        marshall({
          pk: "test",
          sk: "test",
          userId: "test",
          somethingElse: 3,
        }),
      ).toEqual({
        pk: { S: "test" },
        sk: { S: "test" },
        userId: { S: "test" },
        somethingElse: { N: "3" },
      });

      expect(
        unmarshall({
          pk: { S: "test" },
          sk: { S: "test" },
          userId: { S: "test" },
          somethingElse: { N: "3" },
        }),
      ).toEqual({
        pk: "test",
        sk: "test",
        userId: "test",
        somethingElse: 3,
      });
    });

    it("like so", async () => {
      await dynamodb.putItem({
        TableName: tableName,
        Item: marshall({
          pk: "test",
          sk: "test",
          userId: "test",
          somethingElse: 3,
        }),
      });

      const { Item } = await dynamodb.getItem({
        TableName: tableName,
        Key: marshall({
          pk: "test",
          sk: "test",
        }),
      });

      expect(unmarshall(Item ?? {})).toEqual({
        pk: "test",
        sk: "test",
        userId: "test",
        somethingElse: 3,
      });
    });
  });

  describe("using DynamoDBDocument", () => {
    it("handles marshalling and unmarshalling for you", async () => {
      await document.put({
        TableName: tableName,
        Item: {
          pk: "test",
          sk: "test",
          userId: "test",
          somethingElse: 3,
        },
      });

      const { Item } = await document.get({
        TableName: tableName,
        Key: {
          pk: "test",
          sk: "test",
        },
      });

      expect(Item).toEqual({
        pk: "test",
        sk: "test",
        userId: "test",
        somethingElse: 3,
      });
    });

    it("doesn't enforce the existence of fields beyond the keys", async () => {
      await document.put({
        TableName: tableName,
        Item: {
          pk: "test",
          sk: "test",
        },
      });

      await document.put({
        TableName: tableName,
        Item: {
          pk: "test",
          sk: "test",
          userId: "test",
        },
      });

      await document.put({
        TableName: tableName,
        Item: {
          pk: "test",
          sk: "test",
          somethingElse: "hello",
        },
      });

      const { Item } = await document.get({
        TableName: tableName,
        Key: {
          pk: "test",
          sk: "test",
        },
      });

      // no typing on retrieved items

      expect(Item?.userId).toBeUndefined();
      expect(Item?.thing).toBeUndefined();
      expect(Item?.somethingElse).toBe("hello");
      expect(Item?.noWhyTypes).toBeUndefined();
      expect(Item?.ohDear).toBeUndefined();
    });

    it("makes querying for items pretty whack", async () => {
      const { Items } = await document.query({
        TableName: tableName,
        KeyConditionExpression: "pk = :pk",
        ExpressionAttributeValues: {
          ":pk": "post1",
        },
      });

      expect(Items).toEqual(
        expect.arrayContaining([
          {
            pk: "post1",
            sk: "post",
            userId: "user1",
            content: "my cool post",
          },
          {
            pk: "post1",
            sk: "comment#1",
            userId: "user2",
            content: "blah",
          },
          {
            pk: "post1",
            sk: "comment#2",
            userId: "user3",
            content: "hello",
          },
        ]),
      );
    });

    it("especially for more complex queries", async () => {
      const { Items } = await document.query({
        TableName: tableName,
        KeyConditionExpression: "pk = :pk and begins_with(sk, :prefix)",
        ExpressionAttributeValues: {
          ":pk": "post1",
          ":prefix": "comment",
        },
      });

      expect(Items).toEqual(
        expect.arrayContaining([
          {
            pk: "post1",
            sk: "comment#1",
            userId: "user2",
            content: "blah",
          },
          {
            pk: "post1",
            sk: "comment#2",
            userId: "user3",
            content: "hello",
          },
        ]),
      );
    });

    it("and no you can't just inline the values", async () => {
      await expect(() =>
        document.query({
          TableName: tableName,
          KeyConditionExpression: "pk = 'test' and sk = 'comment#2'",
        }),
      ).rejects.toBeInstanceOf(DynamoDBServiceException);
    });

    it('and "single table" queries by index', async () => {
      const { Items } = await document.query({
        TableName: tableName,
        KeyConditionExpression: "userId = :userId and begins_with(sk, :prefix)",
        ExpressionAttributeValues: {
          ":userId": "user3",
          ":prefix": "comment",
        },
        IndexName: "ByUser",
      });

      expect(Items).toEqual(
        expect.arrayContaining([
          {
            pk: "post1",
            sk: "comment#2",
            userId: "user3",
            content: "hello",
          },
          {
            pk: "post2",
            sk: "comment#2",
            userId: "user3",
            content: ":D",
          },
        ]),
      );
    });

    it("especially because you *should* consider pagination", async () => {
      const paginator = paginateQuery(
        {
          client: document,
          pageSize: 2,
        },
        {
          TableName: tableName,
          KeyConditionExpression: "pk = :pk and begins_with(sk, :prefix)",
          ExpressionAttributeValues: {
            ":pk": "post2",
            ":prefix": "comment",
          },
        },
      );

      const itemsPerPage = [];
      const items = [];

      for await (const page of paginator) {
        itemsPerPage.push(page.Items?.length);
        for (const item of page.Items ?? []) {
          items.push(item);
        }
      }

      expect(items).toHaveLength(4);
      expect(itemsPerPage).toEqual([2, 2, 0]);
    });
  });

  afterAll(async () => {
    await dynamodb.deleteTable({
      TableName: tableName,
    });
  });
});
