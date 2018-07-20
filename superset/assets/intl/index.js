import load from "load-script";
import invariant from "invariant";
import * as constants from "./constants";
import isElectron from 'is-electron';

import intlUniversal from "react-intl-universal";
const isBrowser = !isElectron() && typeof window !== "undefined";
const prefix = '/static/assets/intl/locale-data/';
const version = '1.0.0/';
const COMMON_LOCALE_DATA_URLS = {
    en: `${prefix}${version}en.js`,
    zh: `${prefix}${version}zh.js`,
    fr: `${prefix}${version}fr.js`,
    ja: `${prefix}${version}ja.js`,
    de: `${prefix}${version}de.js`,
    es: `${prefix}${version}es.js`,
    ko: `${prefix}${version}ko.js`,
    pt: `${prefix}${version}pt.js`,
    it: `${prefix}${version}it.js`,
    ru: `${prefix}${version}ru.js`,
    pl: `${prefix}${version}pl.js`,
};

/**
* new Intl class to inherite the react-intl-universal class
*/
class Intl {
}

Intl.prototype = intlUniversal;
/**
 * Initialize properties and load CLDR locale data according to currentLocale
 * Overrite the origin init function on prototype
 * @param {Object} options
 * @param {string} options.currentLocale Current locale such as 'en-US'
 * @param {string} options.locales App locale data like {"en-US":{"key1":"value1"},"zh-CN":{"key1":"值1"}}
 * @returns {Promise}
 */
Intl.prototype.init = function init(options = {}) {

invariant(options.currentLocale, "_options.currentLocale is required");
invariant(options.locales, "_options.locales is required");

Object.assign(this.options, options);

this.options.formats = Object.assign({},
    this.options.formats,
    constants.defaultFormats
);

return new Promise((resolve, reject) => {

    const lang = this.options.currentLocale.split('-')[0].split('_')[0];
    const langUrl = COMMON_LOCALE_DATA_URLS[lang];
    if (isBrowser) {
        if (langUrl) {
            load(langUrl, (err, script) => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        } else {
            this.options.warningHandler(`lang "${lang}" is not supported.`);
            resolve();
        }
    } else {
        // For Node.js, common locales are added in the application
        resolve();
    }
});

}

const intl = new Intl();
export default intl;