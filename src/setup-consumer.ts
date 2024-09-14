import { AckPolicy, connect } from 'nats'
import { NATS_SERVERS, NATS_TOKEN } from './config'

const nc = await connect({
  servers: NATS_SERVERS,
  token: NATS_TOKEN,
})
const js = nc.jetstream()
const jsm = await js.jetstreamManager()

await jsm.consumers.add('EVENTS', {
  durable_name: 'nusarag_logsheet_processor',
  ack_policy: AckPolicy.Explicit,
  filter_subject: 'events.nusarag_retrieval_complete',
})

await nc.close()
