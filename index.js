'use strict';

var normalizeOptions = require('es5-ext/object/normalize-options')
  , setPrototypeOf   = require('es5-ext/object/set-prototype-of')
  , ensureObject     = require('es5-ext/object/valid-object')
  , ensureString     = require('es5-ext/object/validate-stringifiable-value')
  , d                = require('d')
  , deferred         = require('deferred')
  , resolve          = require('path').resolve
  , readdir          = require('fs2/readdir')
  , Driver           = require('dbjs-persistence/driver')
  , Storage          = require('./storage')

  , isIdent = RegExp.prototype.test.bind(/^[a-z][a-z0-9A-Z]*$/);

var LevelDriver = module.exports = Object.defineProperties(function (data) {
	if (!(this instanceof LevelDriver)) return new LevelDriver(data);

	this._dbOptions = normalizeOptions(ensureObject(data));
	// Below is workaround for https://github.com/Raynos/xtend/pull/28
	this._dbOptions.hasOwnProperty = Object.prototype.hasOwnProperty;

	this.dbPath = resolve(ensureString(this._dbOptions.path));
	delete this._dbOptions.path;
	Driver.call(this, data);
}, { storageClass: d(Storage) });
setPrototypeOf(LevelDriver, Driver);

LevelDriver.prototype = Object.create(Driver.prototype, {
	constructor: d(LevelDriver),

	__resolveAllStorages: d(function () {
		return readdir(this.dbPath, { type: { directory: true } }).map(function (name) {
			if (!isIdent(name)) return;
			this.getStorage(name);
		}.bind(this))(Function.prototype);
	}),
	__close: d(function () { return deferred(undefined); })
});
