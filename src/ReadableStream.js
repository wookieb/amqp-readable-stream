'use strict';

const Readable = require('stream').Readable;
const Message = require('./Message');
const debug = require('./debug');


/**
 * @typedef {Object} ReadableStreamOptions
 * @property {Object} [queue] Check http://www.squaremobius.net/amqp.node/channel_api.html#channel_assertQueue options
 * @property {Object] [consumer] Check http://www.squaremobius.net/amqp.node/channel_api.html#channel_consume options
 * @property {number} [prefetch=5] Amount of prefetched messages
 */

const readFromQueue = function (callback) {
    if (this._queue && !this.exchange && !this.pattern) {
        consumeQueue.call(this, this._queue, callback)
    } else {
        createNewQueue.call(this, (err, queue) => {
            if (err) {
                callback(err);
                return;
            }
            this._debug('New queue created ' + queue.queue);
            this._createdQueue = queue.queue;
            consumeQueue.call(this, queue.queue, callback);
        });
    }
};

const consumeQueue = function (queue, callback) {
    if (this.options.prefetch) {
        this._channel.prefetch(this.options.prefetch);
    }
    this._channel.consume(
        queue,
        (msg) => {
            const message = new Message(
                msg,
                this._channel.ack.bind(this._channel, msg),
                this._channel.nack.bind(this._channel, msg)
            );
            this.emit('data', message);
        },
        Object.assign({}, this.options.consumer, {
            _consumerTag: this._consumerTag
        }),
        (err, consumerInfo) => {
            if (err) {
                callback(err);
                return;
            }

            this.emit('readable');
            this._consumerTag = consumerInfo.consumerTag;
            this._debug = debug('stream_' + this._consumerTag);
            this._debug('Queue "' + queue + '" consumption has started');
            callback();
        })
};


const createNewQueue = function (callback) {
    this._channel.assertQueue(
        '',
        this.options.queue,
        (err, queue) => {
            if (err) {
                callback(err);
                return;
            }
            this._channel.bindQueue(queue.name, this.exchange, this.pattern, (err) => {
                if (err) {
                    callback(err);
                    return;
                }
                callback(null, queue);
            });
        }
    );
};

class ReadableStream extends Readable {
    /**
     * @param {string} [queue] name - if empty then new queue will be created
     * @param {string} [exchange]
     * @param {string} [pattern]
     * @param {ReadableStreamOptions} [options]
     */
    constructor(queue, exchange, pattern, options) {
        super({
            objectMode: true,
            read: function () {
            }
        });

        this._createdQueue = undefined;
        this._channel = undefined;
        this._consumerTag = undefined;

        this._queue = queue;
        this.exchange = exchange;
        this.pattern = pattern;
        this.options = Object.assign({}, {
            queue: {
                exclusive: true,
                autoDelete: true
            },
            consumer: {
                noAck: false
            },
            prefetch: 5
        }, options);

        this._debug = debug('stream_non_consuming');
    }

    get queue() {
        return this._createdQueue || this._queue;
    }

    get consumerTag() {
        return this._consumerTag;
    }

    setChannel(channel, callback) {
        this._channel = channel;

        if (!this.isPaused()) {
            readFromQueue.call(this, (err) => {
                if (err) {
                    this._debug('Consumption start has failed: ' + err.message);
                    callback && callback(err);
                    return;
                }
                this._debug('Consumption has started');
                callback && callback();
            });
        } else {
            callback && process.nextTick(callback);
        }
    }

    stop(callback) {
        if (this.isPaused()) {
            callback && callback(new Error('The stream is already paused'));
            return;
        }

        if (!this._consumerTag || !this._channel) {
            // No channel, no consumer tag but stream can still be paused
            super.pause();
            callback && callback();
            return;
        }

        this._channel.cancel(this._consumerTag, (err) => {
            if (err) {
                this._debug('Failed to pause the stream: ' + err.message);
                callback && callback(err);
                return;
            }
            super.pause();
            this._debug('Stream paused');
            callback && callback();
        });
    }

    pause() {
        if (!this.isPaused()) {
            this.stop();
        }
    }

    resumeConsumption(callback) {
        if (!this.isPaused()) {
            callback && callback(new Error('The stream is not paused'));
            return;
        }

        if (!this._channel) {
            callback && callback(new Error('Stream has no channel'));
            return;
        }

        readFromQueue.call(this, (err) => {
            if (err) {
                this._debug('Consumption resume has failed: ' + err.message);
                callback && callback(err);
                return;
            }
            super.resume();
            this._debug('Consumption resumed');
            callback && callback();
        });
    }

    resume() {
        if (this.isPaused()) {
            this.resumeConsumption();
        }
    }
}

module.exports = ReadableStream;