/* global varialbes */
export const MESSAGE_DURATION = 5;


/* global functions */
export const callbackHandler = (response, callback) => {
    callback && callback(
        response.status === 200?true:false,
        response.status === 200?response.data:response.message
    );
};
export const always = (response) => {
    return Promise.resolve(response);
};
export const json = (response) => {
    if (!response || !response.json || response.status == 404) {
        throw new Error('no response');
    } else {
        return response.json();
    }
};