import cuid from 'cuid'
import { DynamoDB } from 'aws-sdk'

type ValueGetter<T = string> = (...args: any[]) => T
type MaybeZodSchema = { parse: ValueGetter<any>; _input: any; _def?: { typeName?: string } }

type FilteredKeys<T> = { [P in keyof T]: T[P] extends never ? never : P }[keyof T]
type CreateParams<T extends Schema['models'][string]> = {
  [Key in FilteredKeys<CreateParamsFull<T>>]: CreateParamsFull<T>[Key]
}
type CreateParamsFull<T extends Schema['models'][string]> = {
  [RequiredKey in keyof T]-?: T[RequiredKey] extends {
    required: true
  }
    ? T[RequiredKey]['type']['_input']
    : never
}
type CreateResult<T extends Schema['models'][string]> = {
  [Key in keyof T]: ReturnType<T[Key]['type']['parse']>
}

// look into determining dynamo type from https://dynamoosejs.com/guide/Schema#attribute-types
type Schema = {
  models: {
    [ModelName: string]: {
      pk: {
        value: ValueGetter
        type: MaybeZodSchema
      }
      sk: {
        value: ValueGetter
        type: MaybeZodSchema
      }
      [attrs: string]:
        | { value: ValueGetter; type: MaybeZodSchema }
        | { generate: 'cuid'; type: MaybeZodSchema }
        | { required: true; type: MaybeZodSchema }
    }
  }
  globalIndexes?: Readonly<
    {
      hashKey: string
      rangeKey?: string
      name?: string
    }[]
  >
  localIndexes?: {
    rangeKey: string
    name?: string
  }[]
}

const AttributeTypeMap = {
  string: 'S',
  date: 'S',
  number: 'N',
  boolean: 'BOOL',
  object: 'M',
  array: 'L',
  null: 'NULL',
  buffer: 'B',
}
const zodFirstPartyTypeKindToAttributeTypeMap = {
  ZodString: 'S',
  ZodNumber: 'N',
  ZodNaN: 'N',
  ZodBigInt: 'N',
  ZodBoolean: 'BOOL',
  ZodDate: 'S',
  ZodSymbol: 'S',
  ZodUndefined: 'NULL',
  ZodNull: 'NULL',
  ZodAny: 'M',
  ZodUnknown: 'M',
  ZodNever: 'NULL',
  ZodVoid: 'NULL',
  ZodArray: 'L',
  ZodObject: 'M',
  ZodUnion: 'NULL',
  ZodDiscriminatedUnion: 'NULL',
  ZodIntersection: 'NULL',
  ZodTuple: 'L',
  ZodRecord: 'M',
  ZodMap: 'M',
  ZodSet: 'M',
  ZodFunction: 'NULL',
  ZodLazy: 'NULL',
  ZodLiteral: 'S',
  ZodEnum: 'M',
  ZodEffects: 'NULL',
  ZodNativeEnum: 'NULL',
  ZodOptional: 'NULL',
  ZodNullable: 'NULL',
  ZodDefault: 'NULL',
  ZodCatch: 'NULL',
  ZodPromise: 'NULL',
  ZodBranded: 'NULL',
  ZodPipeline: 'NULL',
} as const

const zodToDynamoType = (
  zodSchema: MaybeZodSchema
): (typeof zodFirstPartyTypeKindToAttributeTypeMap)[keyof typeof zodFirstPartyTypeKindToAttributeTypeMap] => {
  if (!zodSchema?._def?.typeName) {
    return 'S'
  }
  return (
    zodFirstPartyTypeKindToAttributeTypeMap[
      zodSchema._def.typeName as unknown as keyof typeof zodFirstPartyTypeKindToAttributeTypeMap
    ] ?? 'S'
  )
}

export const dynamoTs = <DynamoSchema extends Schema>({
  TableName,
  schema,
  dynamo,
}: {
  TableName: string
  schema: DynamoSchema
  dynamo: {
    DynamoDB: DynamoDB
    DocumentClient: DynamoDB.DocumentClient
  }
}) => {
  type KeyConditionOp<T> = Partial<{
    '=': T
    '<': T
    '<=': T
    '>': T
    '>=': T
    between: [T, T]
    beginsWith: T
  }>

  const table = () => {
    const createTable = async () => {
      const GlobalSecondaryIndexes = schema.globalIndexes?.map((gsi) => {
        return {
          IndexName: gsi.name ?? gsi.hashKey,
          Projection: { ProjectionType: 'ALL' },
          KeySchema: [
            { AttributeName: gsi.hashKey, KeyType: 'HASH' },
            ...(gsi.rangeKey ? [{ AttributeName: gsi.rangeKey, KeyType: 'RANGE' }] : []),
          ],
          ProvisionedThroughput: {
            ReadCapacityUnits: 10,
            WriteCapacityUnits: 10,
          },
        }
      })
      const LocalSecondaryIndexes = schema.localIndexes?.map((lsi) => {
        return {
          IndexName: lsi.name ?? lsi.rangeKey,
          Projection: { ProjectionType: 'ALL' },
          KeySchema: [
            { AttributeName: 'pk', KeyType: 'HASH' },
            { AttributeName: lsi.rangeKey, KeyType: 'RANGE' },
          ],
          ProvisionedThroughput: {
            ReadCapacityUnits: 10,
            WriteCapacityUnits: 10,
          },
        }
      })
      const flattenedModels = Object.values(schema.models).reduce(
        (acc, curr) => ({ ...acc, ...curr }),
        {}
      ) as Schema['models'][string]
      const AttributeDefinitions = [
        { AttributeName: 'pk', AttributeType: zodToDynamoType(flattenedModels.pk.type) },
        { AttributeName: 'sk', AttributeType: zodToDynamoType(flattenedModels.sk.type) },
      ]
      if (GlobalSecondaryIndexes) {
        AttributeDefinitions.push(
          ...GlobalSecondaryIndexes.map((gsi) =>
            gsi.KeySchema.map((keySchema) => ({
              AttributeName: keySchema.AttributeName,
              AttributeType: zodToDynamoType(flattenedModels[keySchema.AttributeName]?.type),
            }))
          )
            .flat()
            .filter((attr) => !AttributeDefinitions.some((def) => def.AttributeName === attr.AttributeName))
        )
      }
      if (LocalSecondaryIndexes) {
        AttributeDefinitions.push(
          ...LocalSecondaryIndexes.map((lsi) =>
            lsi.KeySchema.map((keySchema) => ({
              AttributeName: keySchema.AttributeName,
              AttributeType: zodToDynamoType(flattenedModels[keySchema.AttributeName]?.type),
            }))
          )
            .flat()
            .filter((attr) => !AttributeDefinitions.some((def) => def.AttributeName === attr.AttributeName))
        )
      }
      await dynamo.DynamoDB.createTable({
        TableName,
        KeySchema: [
          { AttributeName: 'pk', KeyType: 'HASH' },
          { AttributeName: 'sk', KeyType: 'RANGE' },
        ],
        AttributeDefinitions,
        GlobalSecondaryIndexes,
        LocalSecondaryIndexes,
        ProvisionedThroughput: {
          ReadCapacityUnits: 10,
          WriteCapacityUnits: 10,
        },
      }).promise()
    }

    const deleteTable = async () => {
      const result = await dynamo.DynamoDB.deleteTable({ TableName }).promise()
      return result
    }

    return { createTable, deleteTable }
  }

  const createItem = <ModelKey extends keyof DynamoSchema['models'], T>(modelName: ModelKey, params: T) => {
    type Model = (typeof schema)['models'][ModelKey]
    const init = Object.fromEntries(
      Object.entries((schema['models'] as any)[modelName] as Model)
        .filter(([key, value]) => !!(value as any).required || !!((value as any).generate === 'cuid'))
        .map(([key, value]) => {
          if ((value as any).required) {
            const val = params[key as any as keyof typeof params]
            return [key, value.type.parse(val)]
          }
          if ((value as any).generate === 'cuid') {
            const val = cuid()
            return [key, value.type.parse(val)]
          }
          return [key, undefined]
        })
    )
    const res = {
      ...init,
      ...Object.fromEntries(
        Object.entries((schema['models'] as any)[modelName] as Model)
          .filter(([key, value]) => !!(value as any).value)
          .map(([key, value]) => {
            const val = (value as any)?.value?.({ ...params, ...init })
            return [key, value.type.parse(val)]
          })
      ),
    }
    return res
  }

  const updateItem = <ModelKey extends keyof DynamoSchema['models'], T>(
    modelName: ModelKey,
    params: T,
    existing?: DynamoSchema['models'][ModelKey]
  ) => {
    type Model = (typeof schema)['models'][ModelKey]
    const init = Object.fromEntries(
      Object.entries((schema['models'] as any)[modelName] as Model)
        .filter(([key, value]) => !!(value as any).required || !!((value as any).generate === 'cuid'))
        .map(([key, value]) => {
          if (existing && key in existing) {
            return [key, existing[key]]
          }
          if ((value as any).required) {
            const val = params[key as any as keyof typeof params]
            return [key, value.type.parse(val)]
          }
          if ((value as any).generate === 'cuid') {
            const val = cuid()
            return [key, value.type.parse(val)]
          }
          return [key, undefined]
        })
    )
    const res = {
      ...init,
      ...Object.fromEntries(
        Object.entries((schema['models'] as any)[modelName] as Model)
          .filter(([key, value]) => !!(value as any).value)
          .map(([key, value]) => {
            const val = (value as any)?.value?.({ ...params, ...init })
            return [key, value.type.parse(val)]
          })
      ),
    }
    return res
  }

  const client = dynamo.DocumentClient

  const get = async (params: {
    pk: {
      keyName: string
      value: unknown
    }
    sk: {
      keyName: string
      value: unknown
    }
    indexName?: string
  }) => {
    const result = await dynamo.DocumentClient.get({
      TableName,
      Key: {
        [`:${params.pk.keyName}`]: params.pk.value,
        [`:${params.sk.keyName}`]: params.sk.value,
      },
    }).promise()
    return result.Item
  }

  const query = async <T = string>(params: {
    pk: {
      keyName: string
      value: unknown
    }
    sk: {
      keyName: string
      conditions: KeyConditionOp<T>
    }
    indexName?: DynamoSchema['globalIndexes'] extends any[]
      ? NonNullable<DynamoSchema['globalIndexes']>[number]['hashKey']
      : string
  }) => {
    const KeyConditionExpressions: string[] = []
    const ExpressionAttributeValues: { [k: string]: T } = {}
    Object.entries(params.sk.conditions).forEach(([op, value]) => {
      if (op === '=') {
        KeyConditionExpressions.push(`${params.sk.keyName} = :eq`)
        ExpressionAttributeValues[':eq'] = value as T
      }
      if (op === '<') {
        KeyConditionExpressions.push(`${params.sk.keyName} < :lessThan`)
        ExpressionAttributeValues[':lessThan'] = value as T
      }
      if (op === '<=') {
        KeyConditionExpressions.push(`${params.sk.keyName} <= :lessThanEqual`)
        ExpressionAttributeValues[':lessThanEqual'] = value as T
      }
      if (op === '>') {
        KeyConditionExpressions.push(`${params.sk.keyName} > :greaterThan`)
        ExpressionAttributeValues[':greaterThan'] = value as T
      }
      if (op === '>=') {
        KeyConditionExpressions.push(`${params.sk.keyName} >= :greaterThanEqual`)
        ExpressionAttributeValues[':greaterThanEqual'] = value as T
      }
      if (op === 'between') {
        KeyConditionExpressions.push(`${params.sk.keyName} between :betweenA and :betweenB`)
        ExpressionAttributeValues[':betweenA'] = (value as [T, T])[0] as T
        ExpressionAttributeValues[':betweenB'] = (value as [T, T])[1] as T
      }
      if (op === 'beginsWith') {
        KeyConditionExpressions.push(`begins_with(${params.sk.keyName}, :beginsWith)`)
        ExpressionAttributeValues[':beginsWith'] = value as T
      }
    })

    const result = await dynamo.DocumentClient.query({
      TableName,
      ExpressionAttributeValues: {
        [`:${params.pk.keyName}`]: params.pk.value,
        ...ExpressionAttributeValues,
      },
      KeyConditionExpression: `${params.pk.keyName} = :${params.pk.keyName} and ${KeyConditionExpressions.join(
        ' and '
      )}`,
      IndexName: params.indexName,
    }).promise()
    return result
  }

  const model = <ModelKey extends keyof (typeof schema)['models']>(modelName: ModelKey) => {
    type Model = (typeof schema)['models'][ModelKey]
    const pk = ((schema['models'] as any)[modelName] as Model).pk.value()
    type SK = ReturnType<(typeof schema)['models'][ModelKey]['sk']['type']['parse']>

    const table = () => {
      return { pk, TableName }
    }

    /**
     * all
     * - gets all items in table
     */
    const all = async () => {
      const result = await dynamo.DocumentClient.scan({ TableName }).promise()
      return result
    }

    /**
     * get
     * - get item by key
     */
    const get = async (Key: { sk: string }) => {
      const result = await dynamo.DocumentClient.get({
        TableName,
        Key: {
          sk: Key.sk,
          pk,
        },
      }).promise()
      return result.Item
    }

    const _query = async (params: KeyConditionOp<SK>) => {
      const result = await query({
        pk: {
          keyName: 'pk',
          value: pk,
        },
        sk: {
          keyName: 'sk',
          conditions: params,
        },
      })
      return result
    }

    const create = async (
      params: CreateParams<(typeof schema)['models'][ModelKey]>
    ): Promise<CreateResult<(typeof schema)['models'][ModelKey]>> => {
      const Item = createItem(modelName, params) as CreateResult<(typeof schema)['models'][ModelKey]>
      const dynamoResult = await dynamo.DocumentClient.put({
        TableName,
        Item,
      }).promise()
      return Item
    }

    const update = async (
      params: CreateParams<(typeof schema)['models'][ModelKey]> & { sk: string },
      options?: { upsert?: boolean }
    ): Promise<CreateResult<(typeof schema)['models'][ModelKey]>> => {
      let { sk, ...ItemParams } = params
      const currentItem = await get({ sk })
      const Item = updateItem(
        modelName,
        ItemParams,
        currentItem as (typeof schema)['models'][ModelKey]
      ) as CreateResult<(typeof schema)['models'][ModelKey]>
      const dynamoResult = await dynamo.DocumentClient.put({
        TableName,
        Item: {
          ...Item,
          sk,
          pk,
        },
      }).promise()
      return Item
    }

    const _delete = async (Key: { sk: string }) => {
      const result = await dynamo.DocumentClient.delete({
        TableName,
        Key: {
          sk: Key.sk,
          pk,
        },
      }).promise()
      return result
    }

    const createMany = async (params: CreateParams<(typeof schema)['models'][ModelKey]>[]) => {
      const Items = params.map((item) => ({
        PutRequest: {
          Item: createItem(modelName, item) as CreateResult<(typeof schema)['models'][ModelKey]>,
        },
      }))

      const dynamoResult = await dynamo.DocumentClient.batchWrite({
        RequestItems: {
          [TableName]: Items,
        },
      }).promise()
      return Items
    }

    return { create, table, createMany, get, query: _query, all, delete: _delete, update }
  }

  return { model, table, client, query, get }
}
