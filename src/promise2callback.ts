import Bluebird = require("bluebird");
export default function (promise: Promise, callback: Function): Promise {
    if (callback) {
        promise.then((result) => {
            callback(undefined, result);
        }, callback);
    }
    return promise;
}