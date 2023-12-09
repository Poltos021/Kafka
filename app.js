const express = require('express');
const bodyParser = require('body-parser');
const { Kafka } = require('kafkajs');

const app = express();
const port = 3000;

// Создайте подключение к Kafka
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
});

// Создайте продюсера
const producer = kafka.producer();

// Запустите продюсера
producer.connect().then(() => {
  console.log('Kafka connected');
}).catch((error) => {
  console.error('Error connecting Kafka:', error);
  process.exit(1);
});

app.use(bodyParser.json());

app.post('/send_message', async (req, res) => {
  try {
    const { message } = req.body;

    // Отправьте сообщение в Kafka
    await producer.send({
      topic: 'test-topic',
      messages: [{ value: message }]
    });

    return res.json({ status: 'success', message: 'Сообщение в кафку!' });
  } catch (error) {
    return res.status(500).json({ status: 'error', message: error.message });
  }
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
