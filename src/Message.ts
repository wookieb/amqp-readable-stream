import * as amqp from 'amqplib';

export default class Message {

    constructor(public message: amqp.Message,
                public queue: string,
                public ack: (allUpTo?: boolean) => void,
                public reject: (allUpTo?: boolean, requeue?: boolean) => void) {
    }

    get message() {
        return this.message;
    }

    get content() {
        return this.message.content;
    }

    get headers() {
        return this.message.headers;
    }

    get exchange() {
        return this.message.fields.exchange;
    }

    get routingKey() {
        return this.message.fields.routingKey;
    }
}