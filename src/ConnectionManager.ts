import ReadableStream, {ExchangeConsumerPolicy, QueueConsumerPolicy} from './ReadableStream';
import * as amqp from 'amqplib';
import debugFn from './debug';
import {EventEmitter} from 'events';
import promise2Callback from './promise2callback';
import * as backoff from 'backoff';
import {ReadableOptions} from "stream";

const debug = debugFn('connection-manager');

export interface ConnectionManagerOptions {
    connection: any,
    reconnect: ReconnectOptions
}

export interface ReconnectOptions {
    failAfter: number,
    backoffStrategy: backoff.BackoffStrategy
}

export default class ConnectionManager extends EventEmitter {

    private streams: ReadableStream[] = [];

    private connection: amqp.Connection;
    private channel: amqp.Channel;

    static defaultConnectionOptions: any = {};
    static defaultReconnectOptions: ReconnectOptions = {
        backoffStrategy: new backoff.ExponentialStrategy({
            initialDelay: 1000,
            maxDelay: 30000,
            randomisationFactor: Math.random()
        }),
        failAfter: 0,
    };

    constructor(private connectionURL: string, private options: ConnectionManagerOptions) {
        super();
    }

    connect(callback?: Function) {
        const promise = this.connectWithBackoff();

        promise.then(() => {
            this.connection.on('close', (err) => {
                if (err) {
                    debug('Disconnected - reconnect attempt');
                    this.connect(callback);
                } else {
                    debug('Disconnected');
                }
            })
        })
            .catch((err) => {
                debug('Failed to connect');
                this.emit('error', err);
            });

        return promise2Callback(this.connectWithBackoff(), callback);
    }

    private connectWithBackoff() {
        const connectionOptions = Object.assign({}, ConnectionManager.defaultConnectionOptions, this.options.connection);

        return new Promise((resolve, reject) => {
            const call = backoff.call((url, options, cb) => {
                this.emit('retry', call.getNumRetries());
                debug('Connecting to queue ... ' + (call.getNumRetries() ? '- Retry #' + call.getNumRetries() : ''));

                amqp.connect(url, options)
                    .then((connection) => {
                        this.connection = connection;

                        debug('Connected!');
                        this.emit('connected', connection);

                        connection.createChannel(cb);
                    })
                    .catch((err) => {
                        debug('Connection failed: ' + err.message);
                        cb(err);
                    });

            }, this.connectionURL, connectionOptions, (err, channel) => {
                if (err) {
                    reject(err);
                    return;
                }

                this.onChannel(channel);
                resolve();
            });

            const reconnectOptions = <ReconnectOptions>Object.assign({}, ConnectionManager.defaultReconnectOptions, this.options.reconnect);

            call.failAfter('failAfter' in reconnectOptions ? reconnectOptions.failAfter : 1);
            call.setStrategy(reconnectOptions.backoffStrategy ? reconnectOptions.backoffStrategy : new backoff.FibonacciStrategy());
            call.start();
        });
    }

    private onChannel(channel: amqp.Channel) {
        this.channel = channel;
        debug('New channel created');
        this.emit('channel', channel);

        return Promise.all(
            this.streams.map((s) => s.setChannel(channel))
        );
    }

    /**
     * Stops consumptions for all readable streams
     *
     * @param callback
     * @returns {Promise}
     */
    stop(callback?: Function) {
        const promise = Promise.all(this.streams.map(
            (stream) => {
                debug(`Stopping consumption for queue ${stream.queue}...`);
                return stream.stopConsumption();
            }
        ));

        return promise2Callback(promise, callback);
    }


    /**
     * Creates readable stream that that consumes given queue
     *
     * @param queue
     * @param [consumerOptions] options provided to channel.consume
     * @param [streamOptions] options provided to readable stream constructor
     * @returns {ReadableStream}
     */
    createStreamForQueue(queue: string, consumerOptions?: amqp.Options.Consume, streamOptions?: ReadableOptions) {
        return this.registerStream(
            new ReadableStream(
                new QueueConsumerPolicy(queue, consumerOptions),
                streamOptions
            )
        );
    }

    /**
     * Creates readable stream that creates queue bound to given exchange with a pattern.
     *
     * @param exchange
     * @param [pattern]
     * @param [assertQueueOptions]
     * @param [consumerOptions]
     * @param [bindArgs]
     * @param [streamOptions]
     * @returns {ReadableStream}
     */
    createStreamForExchange(exchange: string,
                            pattern?: string,
                            assertQueueOptions?: amqp.Options.AssertQueue,
                            consumerOptions?: amqp.Options.Consume,
                            bindArgs?: any,
                            streamOptions?: ReadableOptions) {
        return this.registerStream(
            new ReadableStream(
                new ExchangeConsumerPolicy(
                    exchange, pattern, assertQueueOptions, consumerOptions, bindArgs
                ),
                streamOptions
            )
        );
    }

    private registerStream(stream: ReadableStream) {
        if (this.channel) {
            stream.setChannel(this.channel);
        }
        this.streams.push(stream);
        this.emit('stream', stream);
        return stream;
    }

}