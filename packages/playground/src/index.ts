import { z } from 'zod'
import { DynamoDB, default as AWS } from 'aws-sdk'
import { dynamoTs } from 'dynamodb-ts'

AWS.config.update({
  region: 'us-east-1',
  accessKeyId: 'accessKeyId',
  secretAccessKey: 'secretAccessKey',
})

const {
  model,
  table,
  client: docClient,
  query,
} = dynamoTs({
  TableName: 'test-table',
  dynamo: {
    DynamoDB: new DynamoDB({
      endpoint: 'http://localhost:8000',
      region: 'us-east-1',
    }),
    DocumentClient: new DynamoDB.DocumentClient({
      endpoint: 'http://localhost:8000',
      region: 'us-east-1',
    }),
  },
  schema: {
    models: {
      user: {
        pk: { type: z.string(), value: () => 'user:' },
        sk: { type: z.string(), value: ({ userID }: { userID: string }) => userID },
        userID: { type: z.string(), generate: 'cuid' },
        username: { type: z.string(), required: true },
        name: { type: z.string(), required: true },
        profilePicture: { type: z.string(), required: true },
      },
      post: {
        pk: { type: z.string(), value: () => 'post:' },
        sk: { type: z.string(), value: ({ postID }: { postID: string }) => postID },
        postID: { type: z.string(), generate: 'cuid' },
        userID: { type: z.string(), required: true },
        content: { type: z.string(), required: true },
        createdAt: { type: z.string().datetime(), required: true },
      },
    },
    globalIndexes: [{ hashKey: 'userID', rangeKey: 'pk', name: 'userID' }],
  },
})

export const main = async () => {
  await table()
    .deleteTable()
    .catch(() => {
      console.log('Table does not exist')
    })
  await table().createTable()
  const user = await model('user').create({
    profilePicture: 'https://cloudflare-ipfs.com/ipfs/Qmd3W5DuhgHirLHGVixi6V76LhCkZUz6pnFt5AJBiyvHye/avatar/76.jpg',
    name: 'nicole',
    username: 'nicolesmileyface',
  })
  const posts = await model('post').createMany(
    Array.from({ length: 10 }).map(() => ({
      userID: user.userID,
      content: 'lorem ipsum sit dolor',
      createdAt: new Date().toISOString(),
    }))
  )
  const postsQuery = await model('post').query({
    beginsWith: 'clf',
  })
  const all = await model('post').all()
  const getPost = await model('post').get({
    sk: postsQuery.Items?.[0].sk,
  })
  const deleted = await model('post').delete({ sk: postsQuery.Items?.[0].sk })
  const postsQuery2 = await model('post').query({
    beginsWith: 'clf',
  })

  const postsByUser = await query({
    indexName: 'userID',
    pk: {
      keyName: 'userID',
      value: user.userID,
    },
    sk: {
      keyName: 'pk',
      conditions: {
        '=': 'post:',
      },
    },
  })
  console.log(postsByUser.Items)
  console.log(postsQuery.Items?.length)
  console.log(postsQuery2.Items?.length)
  console.log(deleted)
}

main()
