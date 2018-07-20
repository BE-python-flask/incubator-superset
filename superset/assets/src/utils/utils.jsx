import ReactDOM, {render} from 'react-dom';
import React from 'react';
import {Alert, message} from 'antd';
import {LoadingModal, ConfirmModal, PermPopup} from '../common/components';
import fetch from 'isomorphic-fetch';

import intl from "../../intl";
import http from "axios";
import _ from "lodash";
import SUPPOER_LOCALES from '../support_locales';

// S: global
/* global varialbes */
export const MESSAGE_DURATION = 5;

/* global functions */
export const callbackHandler = (response, callback) => {
    callback && callback(
        response.status === 200 ? true : false,
        response.status === 200 ? response.data : response.message
    );
};
export const always = (response) => {
    return Promise.resolve(response);
};
export const json = (response) => {
    return response.json();
};
// E: global

const config = require('../../package.json');

export const PILOT_PREFIX = '/p/';

export const OBJECT_TYPE = {
    DASHBOARD: 'dashboard',
    SLICE: 'slice',
    DATABASE: 'database',
    DATASET: 'dataset',
    HDFSCONNECTION: 'hdfsconnection',
    TABLE: 'table'
};

export function renderLoadingModal() {
    const loadingModal = render(
        <LoadingModal />,
        document.getElementById('loading_root'));

    return loadingModal;
}

export function renderAlertTip(response, mountId, width='100%') {
    render(
        <Alert
            style={{ width: width }}
            type={response.type}
            message={response.message}
            closable={true}
            showIcon
        />,
        document.getElementById(mountId)
    );
    setTimeout(function() {
        if(document.getElementById(mountId)) {
            ReactDOM.unmountComponentAtNode(document.getElementById(mountId));
        }
    }, 5000);
}

export function renderAlertErrorInfo(description, mountId, width='100%', _this) {
    render(
        <Alert
            style={{width: width}}
            type='error'
            message={description}
            onClose={_this.closeAlert(mountId)}
            closable={true}
            showIcon
        />,
        document.getElementById(mountId)
    );
}

export function renderGlobalErrorMsg(errorMsg) {
    message.error(errorMsg, MESSAGE_DURATION);
}

export function renderConfirmModal(msg, type='warning', callback=false) {
    render(
        <ConfirmModal
            needCallback={callback}
            confirmMessage={msg}
            confirmType={type}
        />,
        document.getElementById('popup_root')
    );
}

export function renderPermModal(id, name, type) {
    render(
        <PermPopup
            objectType={type}
            objectName={name}
            objectId={id}
        />,
        document.getElementById('popup_root')
    );
}

export function getEleOffsetLeft(element) {
    if(element === null || element === undefined) {
        return 0;
    }
    var actualLeft = element.offsetLeft;
    var current = element.offsetParent;
    while (current !== null){
        actualLeft += current.offsetLeft;
        current = current.offsetParent;
    }
    return actualLeft;
}

export function getEleOffsetTop(element) {
    if(element === null || element === undefined) {
        return 0;
    }
    var actualTop = element.offsetTop;
    var current = element.offsetParent;
    while (current !== null){
        actualTop += current.offsetTop;
        current = current.offsetParent;
    }
    return actualTop;
}

export function getUrlParam(name, url) {
    let reg = new RegExp("[^\?&]?" + encodeURI(name) + "=[^&]+");
    url = url || window.location.href;
    let str = url.match(reg);
    if(str&&str.length > 0) {
        return str[0].substring(str[0].indexOf('=') + 1);
    }else {
        return '';
    }
}

export function getAjaxErrorMsg(error) {
    const respJSON = error.responseJSON;
    return (respJSON && respJSON.message) ? respJSON.message :
        error.responseText;
}

/**
* deprecated
*/
export function sortByInitials(a, b) {//add null check
    if((!a && !b) || (a && !b)) {
        return -1;
    } else if (!a && b) {
        return 1;
    }else {
        return a.charCodeAt(0) - b.charCodeAt(0);
    }
}

export function fetchDatabaseList(callback) {
    const url = window.location.origin + '/database/listdata/?page_size=1000';
    return fetch(url, {
        credentials: "same-origin"
    }).then(always).then(json).then(
        response => {
            callbackHandler(response, callback);
        }
    );
}

export function viewObjectDetail(url, callback) {
    return fetch(url, {
        credentials: "same-origin"
    })
    .then(res => res.json())
    .catch(error => {
        console.log('Error:', error);
    })
    .then(response => {
        if(!response) {
            callbackHandler({status: 200}, callback);
        }else {
            callbackHandler(response, callback);
        }
    });
}

export function getAntdLocale(zhCN, enUS) {
    const locale = localStorage.getItem('pilot:currentLocale') || 'zh-CN';
    return locale === 'zh-CN' ? zhCN : enUS;
}

//load intl resources at the very beginning or from cache
export function loadIntlResources(callback, module) {
    let  localePath = '/static/assets/intl/locales/';
    if(module) {
        localePath = localePath + module + '/';
    }
    let currentLocale = localStorage.getItem('pilot:currentLocale');
    if(!currentLocale || !_.find(SUPPOER_LOCALES, { value: currentLocale })) {
        currentLocale = "zh-CN";
    }
    const version = config.version;
    //because in the project setting , front end source are only located under assets folder
    http.get(`${localePath}${currentLocale}.json?v=${version}`)
        .then(res => {
            const localeResource = Object.assign(intl.options.locales[currentLocale] || {}, res.data);
            return intl.init({
                currentLocale,
                locales: {
                    [currentLocale]: localeResource
                }
            });
        })
        .then(() => {
            // After loading CLDR locale data, start to render
            callback && callback();
        });
}

export const checkConfig = (key, callback) => {
    const version = config.version;
    let  localePath = '/static/assets/config/';
    http.get(`${localePath}container.json?v=${version}`)
        .then(res => {
            const data = res.data;
            if (data && key && data[key] && callback) {
                callback(data[key]);
            }
        });
}

export const replaceAppName = () => {
    const appName = checkConfig('appName');
    checkConfig('appName', function(res) {
        document.querySelector('title').innerHTML = res;
    })
}

export const sorterFn = function sorterFn(a, b) {
    if(a != null && b==null ) {
        return true;
    } else if (a ==null && b!=null ) {
        return false
    } else if (a==null && b==null) {
        return ;
    }
    let length = a.length < b.length ? a.length : b.length;
    let idx = 0;
    let flag = true;
    let aCode = a.substr(idx, 1).charCodeAt();
    let bCode = b.substr(idx, 1).charCodeAt();

    // 如果是汉字，Unicode 字符>255
    if(aCode>255 && bCode>255) {
        while (idx < length) {
            if (a.substr(idx, 1).charCodeAt() !== b.substr(idx, 1).charCodeAt()) {
                flag = false;
                return a.substr(idx, 1).localeCompare(b.substr(idx, 1), 'zh');
            }
            idx++;
        }
    } else if(aCode>255 || bCode>255) {
        if(aCode>255 && bCode<=255) {
            return 1;
        } else {
            return -1;
        }
    } else {
        while (idx < length) {
            if (a.substr(idx, 1).charCodeAt() !== b.substr(idx, 1).charCodeAt()) {
                flag = false;
                return a.substr(idx, 1).charCodeAt() - b.substr(idx, 1).charCodeAt();
            }
            idx++;
        }
    };

    if (flag) {
        if (a.length !== b.length) {
            return a.length - b.length;
        } else {
            return true
        }
    }
}

/** 
* https://www.cnblogs.com/wteng/p/5658972.html
* js中文汉字按拼音排序
*/
function sorter(arr,empty) {
    if(!String.prototype.localeCompare)
        return null;
     
    var letters = "*abcdefghjklmnopqrstwxyz".split('');
    var zh = "阿八嚓哒妸发旮哈讥咔垃痳拏噢妑七呥扨它穵夕丫帀".split('');
     
    var segs = [];
    var curr;
    letters.forEach(function(me, i){
        curr = {letter: me, data:[]};
        arr.forEach(function(the) {
            if((!zh[i-1] || zh[i-1].localeCompare(the,"zh") <= 0) && the.localeCompare(zh[i],"zh") == -1) {
                curr.data.push(the);
            }
        });
        if(empty || curr.data.length) {
            segs.push(curr);
            curr.data.sort(function(a,b){
                return a.localeCompare(b,"zh");
            });
        }
    });
    return segs;
}