import {Server} from 'socket.io'
import Redis from 'ioredis'
import prismaClient from './prisma'
import { produceMessage } from './kafka'


const pub = new Redis({
    host: 'redis-e7c6571-rishabh9803451416-a557.a.aivencloud.com',
    port: 24112,
    username: 'default',
    password: 'AVNS_y6taLR7lbgks__VWBol'
});
const sub = new Redis({
    host: 'redis-e7c6571-rishabh9803451416-a557.a.aivencloud.com',
    port: 24112,
    username: 'default',
    password: 'AVNS_y6taLR7lbgks__VWBol'
});

class SocketService {
    private _io: Server;
    constructor() {
        console.log('Init Socket Service..');
        this._io = new Server({
            cors: {
                allowedHeaders: ["*"],
                origin:"*"
            }
        })
        sub.subscribe('MESSAGES')
    }

    public initListeners() {
        const io = this._io
        console.log('init Socket Listners...');
        io.on('connect', socket =>{
            console.log(`New Socket Connected`, socket.id);
            
            socket.on(`event:message`, async ({message}: {message: string}) => {
                console.log("New message received", message);
               // publish mesage to redis
               await pub.publish('MESSAGES', JSON.stringify({message}))
             })
        });

        sub.on('message', async (channel, message) => {
            if (channel === 'MESSAGES') {
                io.emit("message", message)
                await produceMessage(message);
                console.log('Message Produced to Kafka Broker');
            }
        })
    }


    get io() {
        return this._io;
    }
}

export default SocketService;