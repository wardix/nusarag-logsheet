import { JSONCodec, type Consumer } from 'nats'
import { JWT } from 'google-auth-library'
import { google } from 'googleapis'
import {
  NUSARAG_LOG_DOC_ID,
  NUSARAG_LOG_SHEET_NAME,
  SERVICE_ACCOUNT_EMAIL,
  SERVICE_ACCOUNT_PRIVATE_KEY,
} from './config'

const jwt = new JWT({
  email: SERVICE_ACCOUNT_EMAIL,
  key: SERVICE_ACCOUNT_PRIVATE_KEY,
  scopes: ['https://www.googleapis.com/auth/spreadsheets'],
})

const sheets = google.sheets({ version: 'v4', auth: jwt })
const jc = JSONCodec()

export async function processEventMessages(consumer: Consumer) {
  const eventMessages = await consumer.fetch({
    max_messages: 2,
    expires: 1000,
  })

  const spreadsheet = await sheets.spreadsheets.get({
    spreadsheetId: NUSARAG_LOG_DOC_ID,
  })

  const sheet = spreadsheet.data.sheets?.find(
    (s) => s.properties?.title === NUSARAG_LOG_SHEET_NAME,
  )

  if (!sheet) {
    console.error(`Sheet with name "${NUSARAG_LOG_SHEET_NAME}" not found`)
    return
  }

  for await (const message of eventMessages) {
    const data: any = jc.decode(message.data)
    const insertedData = [
      [
        getJakartaFormattedTime(new Date(data.time)),
        data.question,
        data.mostSimilarQuestion,
        data.similarity,
        data.context,
        data.systemInstruction,
        data.response,
      ],
    ]

    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: NUSARAG_LOG_DOC_ID,
      requestBody: {
        requests: [
          {
            insertDimension: {
              range: {
                sheetId: sheet?.properties?.sheetId,
                dimension: 'ROWS',
                startIndex: 1,
                endIndex: 2,
              },
              inheritFromBefore: false,
            },
          },
        ],
      },
    })

    await sheets.spreadsheets.values.update({
      spreadsheetId: NUSARAG_LOG_DOC_ID,
      range: `${NUSARAG_LOG_SHEET_NAME}!R2C1:R2C${insertedData[0].length}`,
      valueInputOption: 'RAW',
      requestBody: {
        values: insertedData,
      },
    })
    message.ack()
  }
}

function getJakartaFormattedTime(date: Date): string {
  const dateFormatter = new Intl.DateTimeFormat('en-GB', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  })

  const [formattedDate, formattedTime] = dateFormatter.format(date).split(', ')
  const [day, month, year] = formattedDate.split('/')
  return `${year}-${month}-${day} ${formattedTime}`
}
