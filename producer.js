const kafka = require('kafka-node');
const { randomBytes } = require('crypto');
const appConfig = require('./config/jsons/appConfig');

const CHUNK_SIZE = 100000;
let producer;

try {
	const { Producer } = kafka;
	const client = new kafka.KafkaClient({ kafkaHost: appConfig.kafka.host });
	producer = new Producer(client);

	producer.on('ready', () => {
		console.log('Producer Ready');
	});

	producer.on('error', (err) => {
		console.log(err);
	});
} catch (e) {
	console.log(e);
}

function emit(id = null, topic, payload) {
	const message = { id, ...payload, appCountry: appConfig.country };
	const payloads = [
		{
			topic,
			messages: JSON.stringify(message),
		},
	];

	// console.log("Producer -> ", payload);

	return new Promise((resolve, reject) => {
		producer.send(payloads, (err, data) => {
			if (err) reject(err);
			// console.log("Producer Callback -> ", data);
			resolve(data);
		});
	});
}

function emitChunkedJson(id = null, topic, json) {
	const promises = [];
	const chunkId = randomBytes(16).toString('hex');

	if (!json) return emit(id, topic, {});
	const payloadString = JSON.stringify(json);
	const chunkCount = Math.ceil(payloadString.length / CHUNK_SIZE);
	if (chunkCount === 1) return emit(id, topic, json);
	for (let start = 0, chunkIndex = 0; start < payloadString.length; start += CHUNK_SIZE, chunkIndex += 1) {
		const chunkedPayload = {
			chunkId, chunkCount, chunkIndex, chunked: true, payload: payloadString.slice(start, start + CHUNK_SIZE),
		};
		promises.push(emit(id, topic, chunkedPayload));
	}
	return promises.length > 1 ? Promise.all(promises) : promises[0];
}

function emitEvent(id = null, topic, payload) {
	return emitChunkedJson(id, topic, payload);
}

module.exports = { producer, emitEvent };