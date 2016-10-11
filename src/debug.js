'use strict';

module.exports = (suffix) => {
    return require('debug')('amqp-readable-stream' + (suffix ? ':' + suffix : ''));
};