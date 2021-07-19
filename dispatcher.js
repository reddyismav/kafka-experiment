const { kafkaStream } = require("./consumer");

try {
	kafkaStream.subscribe((message) => {
		console.log("message ", message);
		const value = JSON.parse(message.value);
		switch (message.topic) {
		case "RANDOM_TEST": {
			return "received"
		}
		default:
			return null;
		}
	});
} catch (err) {
	console.log(err);
}