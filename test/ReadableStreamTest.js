'use strict';

const assert = require('chai').assert;
const sinon = require('sinon');

const ReadableStream = require('../src/ReadableStream');

const createChannel = () => {
    return {
        prefetch: () => {
        },
        consume: () => {
        }
    }
};

describe('Readable stream', () => {
    it('setChannel does not start consumption if stream is paused', (done) => {
        const channel = createChannel();
        channel.consume = () => {
            assert.fail('Queue consumption should not start');
        };

        const stream = new ReadableStream('queue', '', '');
        stream.pause();
        stream.setChannel(channel, () => {
            done();
        });
    });

    it('setChannel starts consumption is stream is not paused', (done) => {
        const channel = createChannel();
        channel.consume = sinon.stub()
            .callsArgWith(3, null, {consumerTag: 'test'});

        const stream = new ReadableStream('queue', '', '');
        stream.setChannel(channel, () => {
            assert.ok(channel.consume.calledOnce);
            done();
        });
    });
});