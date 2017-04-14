import {Readable} from 'stream';
import Message from './Message';
import debug from './debug';
import * as amqp from 'amqplib';
import * as debugModule from 'debug';
import promise2callback from './promise2callback';
import {ReadableOptions} from "stream";
import Bluebird = require("bluebird");

declare module "stream" {
    export class Readable {
        isPaused(): boolean
    }
}

abstract class ConsumerPolicy {
}

export class QueueConsumerPolicy extends ConsumerPolicy {
    constructor(public queue: string, public consumerOptions?: amqp.Options.Consume) {
    }
}

export class ExchangeConsumerPolicy extends ConsumerPolicy {
    constructor(public exchange: string,
                public pattern?: string,
                public assertQueueOptions?: amqp.Options.AssertQueue,
                public consumerOptions?: amqp.Options.Consume,
                public bindArgs?: any) {
    }
}

export default class ReadableStream extends Readable {

    public channel: amqp.Channel;
    public consumerTag: string;

    private debug: debugModule.IDebugger;
    private currentQueueName: string;


    static defaultConsumerOptions: amqp.Options.Consume = {
        noAck: false
    };

    static defaultAssertQueueOptions: amqp.Options.AssertQueue = {
        exclusive: true,
        autoDelete: true
    };

    constructor(private consumerPolicy: ConsumerPolicy, streamOptions: ReadableOptions) {
        super(Object.assign({}, streamOptions, {
            objectMode: true,
            read: () => {
            }
        }));

        this.debug = debug('stream:__no-consumer-tag__');
    }

    get queue(): string {
        return this.currentQueueName;
    }

    /**
     * Sets channel and starts consumption if stream is paused
     *
     * @param {amqp.Channel} channel
     * @param {Function} [callback]
     * @returns {Promise}
     */
    setChannel(channel: amqp.Channel, callback?: Function): Promise<void> {
        this.channel = channel;

        if (this.isPaused()) {
            return promise2callback(this.startConsumption(), callback);
        } else {
            return promise2callback(Promise.resolve(), callback);
        }
    }

    private startConsumption(): Promise<void> {
        if (this.consumerPolicy instanceof QueueConsumerPolicy) {

            const consumerPolicy = <QueueConsumerPolicy>this.consumerPolicy;
            return this.startQueueConsumption(
                consumerPolicy.queue,
                consumerPolicy.consumerOptions
            );

        } else if (this.consumerPolicy instanceof ExchangeConsumerPolicy) {

            const consumerPolicy = <ExchangeConsumerPolicy>this.consumerPolicy;
            return this.createQueueBoundToExchange(
                consumerPolicy.exchange,
                consumerPolicy.pattern,
                consumerPolicy.assertQueueOptions,
                consumerPolicy.bindArgs
            )
                .then((queueName: string) => {
                    return this.startQueueConsumption(
                        queueName,
                        consumerPolicy.consumerOptions
                    );
                });
        } else {
            throw new Error('Invalid consumer policy');
        }
    }

    private startQueueConsumption(queue: string,
                                  consumerOptions: amqp.Options.Consume): Promise<void> {

        const options = Object.assign({}, ReadableStream.defaultConsumerOptions, consumerOptions);
        const promise = <Promise<void>>this.channel.consume(queue, (msg) => {
            this.push(new Message(
                msg,
                queue,
                this.channel.ack.bind(this.channel, msg),
                this.channel.nack.bind(this.channel, msg)
            ));
        }, options)
            .then((result: amqp.Replies.Consume) => {
                this.consumerTag = result.consumerTag;
                this.debug = debug('stream:' + this.consumerTag);
                this.debug(`Queue "${queue}" consumption has started`);
                this.currentQueueName = queue;

                this.emit('readable');
                return undefined;
            });

        promise.catch((e) => {
            this.debug(`Consumption start has failed: ${e.message}`);
        });

        return promise;
    }

    private createQueueBoundToExchange(exchange: string,
                                       pattern: string,
                                       assertQueueOptions: amqp.Options.AssertQueue,
                                       bindArgs: any): Promise<string> {
        const assertQueueOptions = Object.assign({}, ReadableStream.defaultAssertQueueOptions, assertQueueOptions);
        let queueName: string;
        return <Promise<string>>this.channel
            .assertQueue('', assertQueueOptions)
            .then((result: amqp.Replies.AssertQueue) => {
                queueName = result.queue;
                return this.channel.bindQueue(result.queue, exchange, pattern, bindArgs);
            })
            .then(() => queueName);
    }

    /**
     * Stops further queue consumption.
     *
     * @param {Function} [callback]
     * @returns {Promise}
     */
    stopConsumption(callback?: Function) {
        if (this.isPaused()) {
            return promise2callback(
                Promise.reject(new Error('Stream already stopped')),
                callback
            );
        }

        if (!this.channel || !this.consumerTag) {
            super.pause();
            return promise2callback(
                Promise.resolve(),
                callback
            );
        }

        const promise = <Promise>this.channel.cancel(this.consumerTag);

        promise.then(() => {
            super.pause();
            this.debug('Stream paused');
        }, (e) => {
            this.debug(`Stream failed to pause ${e.message}`);
        });

        return promise2callback(promise, callback);
    }

    pause() {
        if (!this.isPaused()) {
            //noinspection JSIgnoredPromiseFromCall
            this.stopConsumption();
        }
    }

    /**
     * @param {Function} [callback]
     * @returns {Promise}
     */
    resumeConsumption(callback?: Function) {
        if (!this.isPaused()) {
            return promise2callback(Promise.reject(new Error('The stream is not paused')), callback);
        }

        if (!this.channel) {
            return promise2callback(Promise.reject(new Error('Stream has no channel')), callback);
        }

        const promise = this.startConsumption();
        promise.then(() => {
            super.resume();
            this.debug('Consumption resumed');
        }, (e) => {
            this.debug(`Consumption resume has failed: ${e.message}`);
        });
        return promise2callback(promise, callback);
    }

    resume() {
        if (this.isPaused()) {
            //noinspection JSIgnoredPromiseFromCall
            this.resumeConsumption();
        }
    }

}
