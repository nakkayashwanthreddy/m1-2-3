require("dotenv").config();

const { Pool } = require("pg");
const Redis = require("ioredis");
const { Kafka } = require("kafkajs");

const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT),
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD
});

const redis = new Redis({
  host: process.env.REDIS_HOST,
  port: Number(process.env.REDIS_PORT)
});

const kafka = new Kafka({
  clientId: "horizon-bank",
  brokers: [process.env.KAFKA_BROKER]
});

const producer = kafka.producer();
const consumer = kafka.consumer({
  groupId: "horizon-notification-group"
});

module.exports = {
  pool,
  redis,
  kafka,
  producer,
  consumer
};