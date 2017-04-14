import ReadableStream from './ReadableStream';
import * as amqp from 'amqplib';
import debugFn from './debug';
import {EventEmitter} from 'events';
import promise2Callback from './promise2callback';

const backoff = require('backoff');

const debug = debugFn('connection-manager');

export interface ConnectionManagerOptions {
    connectionOptions: any,
    reconnectOptions: any
}

export default class ConnectionManager extends EventEmitter {

    private streams: ReadableStream[] = [];

    private connection: amqp.Connection;
    private channel: amqp.Channel;

    static defaultConnectionOptions: any = {};
    static defaultReconnectOptions: any = {
        backoffStrategy: new backoff.ExponentialStrategy({
            initialDelay: 1000,
            maxDelay: 30000,
            randomisationFactor: Math.random()
        }),
        failAfter: 0,
    };

    constructor(private connectionURL: string, options: ConnectionManagerOptions) {

    }

    connect(callback?: Function) {

    }

    private connectWithBackoff() {
        let call = backoff.call((url, options, cb) => {
            this.emit('retry', call.getNumRetries());
            debug('Connecting to queue ... ' + (call.getNumRetries() ? '- Retry #' + call.getNumRetries() : ''));

            let alreadyCalled = false;
            let connectCallback = (err, connection) => {
                // this is necessary to prevent double call of connection callback
                if (alreadyCalled) {
                    return;
                }
                alreadyCalled = true;

                if (err) {
                    debug('Connection failed: ' + err.message);
                    cb(err);
                    return;
                }

                this._connection = connection;

                debug('Connected!');
                this.emit('connected', connection);

                var createChannelFunc = this.options.useConfirmChannel ? connection.createConfirmChannel : connection.createChannel;
                createChannelFunc.call(connection, cb);
            };

            amqp.connect(url, options, connectCallback);
        }, this.url, this.options.connection, (err, channel) => {
            if (err) {
                debug('Failed to connect: ' + err.message);
                callback(err);
                return;
            }

            this._channel = channel;
            debug('New channel created');
            onChannel.call(this, channel);

            this.emit('channel', channel);
            callback();
        });

        call.failAfter(this.options.reconnect ? this.options.reconnect.failAfter : 1);
        call.setStrategy(this.options.reconnect ? this.options.reconnect.backoffStrategy : new backoff.fibonacci());
        call.start();
    }

    private onChannel(channel: amqp.Channel) {
        this.channel = channel;

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
}