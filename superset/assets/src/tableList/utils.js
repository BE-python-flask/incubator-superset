import {PILOT_PREFIX} from './../utils/utils'

export default {
    getAbsUrl: relativePath => window.location.origin + relativePath
}

export function getPublishTableUrl(record) {
    let url = window.location.origin + PILOT_PREFIX + "release/dataset/";
    if(record.online) {
        url += "offline/" + record.id;
    }else {
        url += "online/" + record.id;
    }
    return url;
}