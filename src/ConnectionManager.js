n'use strict';

const ReadableStream = require('./ReadableStream');
const backoff = require('backoff');
const amqp = require('amqplib/callback_api');
const debug = require('./debug')('connection-manager');
const EventEmitter = require('events').EventEmitter;
const async = require('async');


const connect = function (callback) {
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
};

const onChannel = function (channel) {
    this.streams.forEach(stream => stream.setChannel(channel));
};

class ConnectionManager extends EventEmitter {
    constructor(url, options) {
        super();

        this.streams = [];

        this.options = Object.assign({}, {
            useConfirmChannel: true,
            connection: {},
            reconnect: {
                backoffStrategy: new backoff.ExponentialStrategy({
                    initialDelay: 1000,
                    maxDelay: 30000,
                    randomisationFactor: Math.random()
                }),
                failAfter: 0,
            }
        }, options || {});

        this.url = url;
        this._connection = undefined;
        this._channel = undefined;
    }

    /**
     * Connects to the queue
     *
     * @param {Function} [callback]
     */
    connect(callback) {
        connect.call(this, (err) => {
            if (err) {
                debug('Failed to connect');
                this.emit('error', err);
                callback && callback(err);
                return;
            }

            this._connection.on('close', (err) => {
                if (err) {
                    debug('Disconnected - Attempt to connect');
                    connect.call(this);
                } else {
                    debug('Disconnected');
                }
            });

            callback && callback(null, this._connection);
        })
    }


    /**
     * @param {string} [queue] is empty then new queue will be created for the stream
     * @param {string} [exchange]
     * @param {string} [pattern]
     * @param {ReadableStreamOptions} [options]
     *
     * @returns {ReadableStream}
     */
    createStream(queue, exchange, pattern, options) {
        let stream = new ReadableStream(queue, exchange, pattern, options);
        if (this._channel) {
            stream.setChannel(this._channel);
        }
        this.streams.push(stream);
        this.emit('stream', stream);
        return stream;
    }

    /**
     * Stops consuming message for all streams
     *
     * @param {Function} [callback]
     */
    stop(callback) {
        async.each(this.streams, (stream, cb) => {
            debug('Stopping consumption for stream: ' + (stream.consumerTag ? stream.consumerTag : '[not consuming yet]'));
            stream.stopConsumption(cb);
        }, callback);
    }
}

module.exports = ConnectionManager;