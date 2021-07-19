const kafka = require('kafka-node');
const Rx = require('rxjs/Rx');
const appConfig = require('./config/jsons/appConfig');
const TOPICS = require('./config/kafkaTopics');

let consumerGroup;
let kafkaStream;

function deChunkJson(chunks) {
	chunks = chunks.sort((a, b) => a.value.chunkIndex - b.value.chunkIndex);
	const payload = JSON.parse(chunks.map(chunk => chunk.value.payload).join(''));
	payload.id = chunks[0].value.id;
	return { topic: chunks[0].topic, value: payload };
}

try {
	const client = new kafka.KafkaClient({ kafkaHost: appConfig.kafka.host });
	const topicsToCreate = TOPICS.map(topic => ({
		topic,
		partitions: 1,
		replicationFactor: 1,
	}));
	client.createTopics(topicsToCreate, (error, result) => {
		console.log("Kafka createTopics Result:", result);
		console.log("ERROR />", error);
	});

	const options = {
		kafkaHost: appConfig.kafka.host, // connect directly to kafka broker (instantiates a KafkaClient)
		groupId: appConfig.kafka.groupId,
		// An array of partition assignment protocols ordered by preference.
		// 'roundrobin' or 'range' string for built ins (see below to pass in custom assignment protocol)
		protocol: ["roundrobin"],
		// Offsets to use for new groups other options could be 'earliest' or 'none' (none will emit an error if no offsets were saved)
		// equivalent to Java client's auto.offset.reset
		fromOffset: "latest", // default
		commitOffsetsOnFirstJoin: true, // on the very first time this consumer group subscribes to a topic, record the offset returned in fromOffset (latest/earliest)
		// how to recover from OutOfRangeOffset error (where save offset is past server retention) accepts same value as fromOffset
		outOfRangeOffset: "earliest", // default
		fetchMaxBytes: 10000 * 10000,
	};

	consumerGroup = new kafka.ConsumerGroup(options, TOPICS);

	consumerGroup.on("error", err => console.log("error", err));
	consumerGroup.on("offsetOutOfRange", err => console.log("offset out of range!", err));

	const inputStream = Rx.Observable
		.fromEvent(consumerGroup, 'message')
		.filter(message => JSON.parse(message.value).appCountry === appConfig.country)
		.map(message => ({ topic: message.topic, value: JSON.parse(message.value) }));

	const [chunkedStream, nonChunkedStream] = inputStream.partition(message => message.value.chunked);

	kafkaStream = nonChunkedStream.merge(
		chunkedStream
			.groupBy(message => `${message.value.chunkId}_${message.value.chunkCount}`)
			// eslint-disable-next-line comma-dangle
			.mergeMap(group => group.bufferCount(group.key.split('_')[1]).map(chunks => deChunkJson(chunks)))
	)
		.map((payload) => {
			delete payload.appCountry;
			return { topic: payload.topic, value: JSON.stringify(payload.value) };
		});

	kafkaStream.subscribe(e => console.log(e));
} catch (e) {
	console.log(e);
}

const listener = id => new Promise((resolve, reject) => {
	kafkaStream
		.map(message => JSON.parse(message.value))
		.filter(payload => payload.id === id)
		.take(1)
		.timeout(appConfig.TIMEOUT_MS)
		.subscribe(
			val => resolve(val),
			err => reject(err), // timeout error
		);
});

module.exports = { consumerGroup, kafkaStream, listener };