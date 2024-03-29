import { Kafka, Producer } from "kafkajs"
import fs from 'fs'
import path from 'path'
import prismaClient from "./prisma";

const kafka = new Kafka({
    brokers: ['kafka-2dda8afd-rishabh9803451416-a557.a.aivencloud.com:24125'],
    ssl: {
        ca: [fs.readFileSync(path.resolve('./ca.pem'), "utf-8")],
    },
    sasl: {
        username: 'avnadmin',
        password: 'AVNS_HrQKa9Nn9ME_QB7KldV',
        mechanism: 'plain'

    }
})

let producer: null | Producer = null;

export async function CreateProducer() {
    if (producer) return producer;

    const _producer = kafka.producer()
    await _producer.connect();
    producer=_producer
    return producer;
}

export async function produceMessage(message: string) {
    const producer = await CreateProducer();
    await producer.send({
        messages: [{ key: `message-${Date.now()}`, value: message}],
        topic: 'MESSAGES',
    })
    return true;
} 

export async function startConsumer()  {
    console.log('consumer started')
    const consumer = kafka.consumer({groupId: "default"});
    await consumer.connect();
    await consumer.subscribe({topic: "MESSAGES", fromBeginning: true});

    await consumer.run({
        autoCommit: true,
        eachMessage: async ({message, pause}) => {
           
            if (!message.value) return;
            console.log(`New message received..`)
            try{
            await prismaClient.messages.create({
                data: {
                    text: message.value?.toString()
                }
            })
        } catch(err){
            console.log('something is wrong')
            pause()
            setTimeout(() =>{consumer.resume([{topic: 'MESSAGES'}])}, 60*1000)
        }
        }
    })
    
}


export default kafka