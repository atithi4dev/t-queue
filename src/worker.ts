import { createClient, RedisClientType } from 'redis';
import { Job } from './queue.js';

export interface WorkerOptions{
    redisUrl? : string;
    pollTimeOut?: number;
}

type Processor<T> = (job: Job<T>) => Promise<void>;


function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}


export class Worker<T = any> {
    private client: RedisClientType;
    private running:boolean = false;
    private waitKey: string;
    private activeKey: string;
    private failedKey: string;

    constructor(
        private queueName: string,
        private processor: Processor<T>,
        private options: WorkerOptions = {}
    ){
        this.waitKey = `${queueName}:wait`;
        this.activeKey = `${queueName}:active`;
        this.failedKey = `${queueName}:failed`;
        

        this.client = createClient({
            url: options.redisUrl
        })

        this.client.on("error", (err)=>{
            console.log(`Redis Error: `, err);
        })
    }

    private async connect(){
        if(!this.client.isOpen){
            await this.client.connect();
        }
    }

    async start(){
        await this.connect();

        this.running = true;
        const timeout = this.options.pollTimeOut ?? 5;

        while(this.running){
            const jobData = await this.client.brPopLPush(
                this.waitKey,
                this.activeKey,
                timeout
            );

            if(!jobData) continue;

            const job: Job<T> = JSON.parse(jobData);

            try {
                await sleep(4000);
                await this.processor(job);
                // ACK
                await this.client.lRem(this.activeKey,1, jobData);
            } catch (error) {
                job.attempts++;
                await this.client.lRem(this.activeKey, 1, jobData);
                await this.client.lPush(this.failedKey, JSON.stringify(job));

                console.log(`Job ${job.id} failed: `, error);
            }

        }
    }

    async stop(){
        this.running = false;
        if(this.client.isOpen){
            await this.client.quit();
        }
    }
}