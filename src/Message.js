'use strict';

class Message {
    constructor(message, ack, reject) {
        this._message = message;
        this._ack = ack;
        this._reject = reject;
        Object.freeze(this);
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

    ack() {
        this._ack();
    }

    reject() {
        this._reject();
    }
}

module.exports = Message;