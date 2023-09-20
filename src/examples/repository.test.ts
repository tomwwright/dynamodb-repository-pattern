import { BatchWriteCommandInput, DynamoDBDocument, paginateQuery } from "@aws-sdk/lib-dynamodb";
import { DynamoDB, DynamoDBServiceException } from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import { Repository } from "../lib";
import z, { ZodError } from "zod";
import { v4 as uuid } from "uuid";

const config = {
  endpoint: "http://localhost:4567",
  region: "local",
  credentials: { accessKeyId: "local", secretAccessKey: "local" },
};

const tableName = "repository";

describe("using a repository pattern with DynamoDB", () => {
  const dynamodb = new DynamoDB(config);
  const document = DynamoDBDocument.from(dynamodb);

  const schema = z.object({
    pk: z.string(),
    sk: z.string(),
    userId: z.string(),
    content: z.string(),
    likes: z.number(),
  });
  const repository = new Repository(document, tableName, schema);

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
          KeyType: "HASH",
        },
        {
          AttributeName: "sk",
          KeyType: "RANGE",
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

    await document.batchWrite(batchOfItems);
  });

  it("handles marshalling and unmarshalling for you", async () => {
    await repository.put({
      pk: "post5",
      sk: "post",
      userId: "user5",
      content: "#postlyf",
      likes: 3,
    });

    const item = await repository.get({
      pk: "post5",
      sk: "post",
    });

    expect(item).toEqual({
      pk: "post5",
      sk: "post",
      userId: "user5",
      content: "#postlyf",
      likes: 3,
    });
  });

  it("enforces the existence of fields beyond the keys on write", async () => {
    await expect(() =>
      repository.put({
        pk: "post5",
        sk: 3,
        likes: "oopsie",
      } as any),
    ).rejects.toBeInstanceOf(ZodError);
  });

  it("provides types on read", async () => {
    await repository.put({
      pk: "post5",
      sk: "post",
      userId: "user5",
      content: "#postlyf",
      likes: 3,
    });

    const item = await repository.get({
      pk: "post5",
      sk: "post",
    });

    // expect(item.thing).toBeUndefined();
    // expect(item.somethingElse).toBeUndefined();
    // expect(item.noWhyTypes).toBeUndefined();
    // expect(item.ohDear).toBeUndefined();
  });

  it("makes querying for items more sensible", async () => {
    const iterator = repository.query({
      pk: "post1",
    });
    const items = await toArray(iterator);

    expect(items).toEqual(
      expect.arrayContaining([
        {
          pk: "post1",
          sk: "post",
          userId: "user1",
          content: "my cool post",
          likes: 1,
        },
        {
          pk: "post1",
          sk: "comment#1",
          userId: "user2",
          content: "blah",
          likes: 2,
        },
        {
          pk: "post1",
          sk: "comment#2",
          userId: "user3",
          content: "hello",
          likes: 5,
        },
      ]),
    );
  });

  it("even for more complex queries", async () => {
    const iterator = repository.query({
      pk: "post2",
      sk: ["begins_with", "comment"],
    });
    const items = await toArray(iterator);

    expect(items).toHaveLength(4);
  });

  describe("using classes to capture logic", () => {
    /**
     * Encapsulate the DynamoDB logic in a "write once" way
     */
    class PostsRepository extends Repository<typeof schema> {
      constructor(dynamodb: DynamoDBDocument) {
        super(dynamodb, tableName, schema);
      }

      public getPost(postId: string) {
        return this.get({
          pk: postId,
          sk: "post",
        });
      }

      public getCommentsByPostId(postId: string) {
        return this.query({
          pk: postId,
          sk: ["begins_with", "comment"],
        });
      }

      public getCommentsByUserId(userId: string) {
        return this.query(
          {
            userId,
            sk: ["begins_with", "comment"],
          },
          {
            index: "ByUser",
          },
        );
      }
    }

    const posts = new PostsRepository(document);

    it("makes the things we did before even easier", async () => {
      const iterator = posts.getCommentsByPostId("post2");
      const items = await toArray(iterator);

      expect(items).toHaveLength(4);
    });

    it("but writing items still leaks our DynamoDB implementation", async () => {
      await posts.put({
        pk: "post1",
        sk: "post",
        userId: "user1",
        content: "my cool post",
        likes: 1,
      });
    });
  });

  describe("using Zod to handle key transforms for our items", () => {
    /**
     * Capture each of our domain objects with their own
     * schema and repository -- still using the same table
     *
     * Use .default() and .transform() to power the keys in our items
     */
    const CommentSchema = z
      .object({
        postId: z.string(),
        commentId: z.string().default(() => uuid()),
        userId: z.string(),
        content: z.string(),
        likes: z.number(),
      })
      .transform((item) => ({
        ...item,
        pk: item.postId,
        sk: `comment#${item.commentId}`,
      }));

    class CommentRepository extends Repository<typeof CommentSchema> {
      constructor(dynamodb: DynamoDBDocument) {
        super(dynamodb, tableName, CommentSchema);
      }

      public getCommentsByPostId(postId: string) {
        return this.query({
          pk: postId,
          sk: ["begins_with", "comment"],
        });
      }

      public getCommentsByUserId(userId: string) {
        return this.query(
          {
            userId,
            sk: ["begins_with", "comment"],
          },
          {
            index: "ByUser",
          },
        );
      }
    }

    const PostSchema = z
      .object({
        postId: z.string().default(() => uuid()),
        userId: z.string(),
        content: z.string(),
        likes: z.number(),
      })
      .transform((item) => ({
        ...item,
        pk: item.postId,
        sk: "post",
      }));

    class PostsRepository extends Repository<typeof PostSchema> {
      constructor(dynamodb: DynamoDBDocument) {
        super(dynamodb, tableName, PostSchema);
      }

      public getPost(postId: string) {
        return this.get({
          pk: postId,
          sk: "post",
        });
      }

      public getPostsByUserId(userId: string) {
        return this.query(
          {
            userId,
            sk: "post",
          },
          {
            index: "ByUser",
          },
        );
      }
    }

    const posts = new PostsRepository(document);
    const comments = new CommentRepository(document);

    it("allows for an API for items without considering pk and sk keys", async () => {
      const { postId } = await posts.put({
        userId: "user3",
        content: "my content",
        likes: 4,
      });

      expect(postId).toBeDefined();

      const item = await posts.getPost(postId);

      expect(item).toEqual({
        pk: postId, // keys present in parsed output still
        sk: "post",
        postId,
        userId: "user3",
        content: "my content",
        likes: 4,
      });
    });

    it("handles updating and creating items", async () => {
      // creates item and assigns postId
      const { postId } = await posts.put({
        userId: "user3",
        content: "my content",
        likes: 4,
      });

      // updates the existing item by postId
      await posts.put({
        postId,
        userId: "user3",
        content: "my content - updated!",
        likes: 4,
      });
    });
  });

  describe("modelling the User as an additional domain object", () => {
    /**
     *
     * Add the User using another key usage schema
     *
     * | Type    | `pk`       | `sk`                  |
     * | ------- | ---------- | --------------------- |
     * | Post    | `{postId}` | `post`                |
     * | Comment | `{postId}` | `comment#{commentId}` |
     * | User    | `{userId}` | `user`                |
     *
     * | Access mode            | Query                                          | Index    |
     * | ---------------------- | ---------------------------------------------- | -------- |
     * | Get Post               | `pk = {postId} and sk = "post`                 | -        |
     * | Get Comment            | `pk = {postId} and sk = "comment#{commentId}"` | -        |
     * | Get User [New!]        | `pk = {userId} and sk = "user"`                | -        |
     * | List Comments for Post | `pk = {postId} and sk = "comment#*"`           | -        |
     * | List Posts for User    | `userId = {userId} and sk = "post"`            | `ByUser` |
     * | List Comments for User | `userId = {userId} and sk = "comment#*"`       | `ByUser` |
     * | List Comments for User | `userId = {userId} and sk = "comment#*"`       | `ByUser` |
     */

    const UserSchema = z
      .object({
        userId: z.string().default(() => uuid()),
        name: z.string(),
        isAdmin: z.boolean().default(false),
      })
      .transform((item) => ({
        ...item,
        pk: item.userId,
        sk: "user",
      }));

    class UsersRepository extends Repository<typeof UserSchema> {
      constructor(dynamodb: DynamoDBDocument) {
        super(dynamodb, tableName, UserSchema);
      }

      public getUser(userId: string) {
        return this.get({
          pk: userId,
          sk: "user",
        });
      }
    }

    const users = new UsersRepository(document);

    it("coexists in the same table happily", async () => {
      const { userId } = await users.put({
        name: "tom",
      });

      const user = await users.getUser(userId);
      expect(user).toEqual({
        pk: userId,
        sk: "user",
        userId,
        name: "tom",
        isAdmin: false,
      });
    });
  });

  afterAll(async () => {
    await dynamodb.deleteTable({
      TableName: tableName,
    });
  });
});

const toArray = async <T>(iterator: AsyncGenerator<T>): Promise<T[]> => {
  const items = [];
  for await (const i of iterator) {
    items.push(i);
  }
  return items;
};

const batchOfItems: BatchWriteCommandInput = {
  RequestItems: {
    [tableName]: [
      ["post1", "post", "user1", "my cool post", 1],
      ["post1", "comment#1", "user2", "blah", 2],
      ["post1", "comment#2", "user3", "hello", 5],
      ["post2", "post", "user2", "i also post stuff", 10],
      ["post2", "comment#1", "user1", ":)", 7],
      ["post2", "comment#2", "user3", ":D", 3],
      ["post2", "comment#3", "user1", ":D", 4],
      ["post2", "comment#4", "user1", ":(", 1],
    ].map(([pk, sk, userId, content, likes]) => ({
      PutRequest: {
        Item: {
          pk,
          sk,
          userId,
          content,
          likes,
        },
      },
    })),
  },
};
