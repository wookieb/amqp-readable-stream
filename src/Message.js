'use strict';

class Message {
    constructor(message, queue, ack, reject) {
        this._message = message;
        this._ack = ack;
        this._reject = reject;
        this._queue = queue;
    }

    get message() {
        return this._message;
    }

    get content() {
        return this._message.content;
    }

    get headers() {
        return this._message.headers;
    }

    get queue() {
        return this._queue;
    }

    get exchange() {
        return this._message.fields.exchange;
    }

    get routingKey() {
        return this._message.fields.routingKey;
    }

    ack() {
        this._ack();
    }

    reject() {
        this._reject();
    }
}

module.exports = Message;