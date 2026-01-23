import {createClient, RedisClientType} from 'redis';
import { randomUUID } from "crypto"; 

export interface QueueOptions {
    redisUrl? :string
}

export interface Job<T= any> {
    id: string;
    data: T;
    attempts: number;
}

export class Queue<T = any> {
    private client: RedisClientType;
    private waitKey: string;

    constructor(
        private name: string,
        private options: QueueOptions = {}
    ) {
        this.waitKey = `${name}:wait`;

        this.client = createClient({
            url : options.redisUrl
        });

        this.client.on("error", (err)=>{
            console.log("Redis Error", err);
        })
    }

    private async connect(){
        if(!this.client.isOpen){
            await this.client.connect();
        }
    }

    async add(data: T): Promise<Job<T>>{
        await this.connect();
        
        const job = {
            id: randomUUID(),
            data,
            attempts: 0
        }

        await this.client.lPush(this.waitKey, JSON.stringify(job));
        return job;
    }

    async close(){
        if(this.client.isOpen){
            await this.client.quit();
        }
    }

}