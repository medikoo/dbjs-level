'use strict';

var normalizeOptions  = require('es5-ext/object/normalize-options')
  , setPrototypeOf    = require('es5-ext/object/set-prototype-of')
  , ensureString      = require('es5-ext/object/validate-stringifiable-value')
  , ensureObject      = require('es5-ext/object/valid-object')
  , resolveKeyPath    = require('dbjs/_setup/utils/resolve-key-path')
  , rmdir             = require('fs2/rmdir')
  , d                 = require('d')
  , deferred          = require('deferred')
  , level             = require('levelup')
  , PersistenceDriver = require('dbjs-persistence/abstract')

  , isArray = Array.isArray, create = Object.create, stringify = JSON.stringify
  , parse = JSON.parse, promisify = deferred.promisify
  , getOpts = { fillCache: false };

var LevelDriver = module.exports = function (dbjs, data) {
	if (!(this instanceof LevelDriver)) return new LevelDriver(dbjs, data);
	this._dbOptions = normalizeOptions(ensureObject(data));
	// Below is workaround for https://github.com/Raynos/xtend/pull/28
	this._dbOptions.hasOwnProperty = Object.prototype.hasOwnProperty;
	this._dbOptions.path = ensureString(this._dbOptions.path);
	PersistenceDriver.call(this, dbjs, data);
	this._initialize();
};
setPrototypeOf(LevelDriver, PersistenceDriver);

LevelDriver.prototype = Object.create(PersistenceDriver.prototype, {
	constructor: d(LevelDriver),

	// Any data
	__getRaw: d(function (cat, ns, path) {
		if (cat === 'reduced') return this._getReduced(ns + (path ? ('/' + path) : ''));
		if (cat === 'computed') return this._getComputedValue(path, ns);
		return this.levelDb.getPromised(ns + (path ? ('/' + path) : ''), getOpts)(function (value) {
			var index = value.indexOf('.');
			return { stamp: Number(value.slice(0, index)), value: value.slice(index + 1) };
		}.bind(this), function (err) {
			if (err.notFound) return null;
			throw err;
		});
	}),
	__getDirectObject: d(function (objId, keyPaths) {
		return this._loadDirect({ gte: objId, lte: objId + '/\uffff' },
			keyPaths && function (ownerId, path) { return keyPaths.has(resolveKeyPath(path)); });
	}),
	__storeRaw: d(function (cat, ns, path, data) {
		if (cat === 'reduced') return this._storeReduced(ns + (path ? ('/' + path) : ''), data);
		if (cat === 'computed') return this._storeIndexedValue(path, ns, data);
		return this.levelDb.putPromised(ns + (path ? ('/' + path) : ''), data.stamp + '.' + data.value);
	}),

	// Database data
	__getDirectAll: d(function () { return this._loadDirect(); }),

	// Size tracking
	__searchDirect: d(function (callback) {
		var def = deferred();
		this.levelDb.createReadStream().on('data', function (data) {
			var index;
			if (data.key[0] === '=') return; // computed record
			if (data.key[0] === '_') return; // reduced record
			index = data.value.indexOf('.');
			callback(data.key, {
				stamp: Number(data.value.slice(0, index)),
				value: data.value.slice(index + 1)
			});
		}.bind(this)).on('error', def.reject).on('end', def.resolve);
		return def.promise;
	}),
	__searchComputed: d(function (keyPath, callback) {
		var def = deferred();
		this.levelDb.createReadStream({ gte: '=' + keyPath + ':', lte: '=' + keyPath + ':\uffff' })
			.on('data', function (data) {
				var index, value, ownerId = data.key.slice(data.key.lastIndexOf(':') + 1);
				index = data.value.indexOf('.');
				value = data.value.slice(index + 1);
				if (value[0] === '[') value = parse(value);
				callback(ownerId, { value: value, stamp: Number(data.value.slice(0, index)) });
			}.bind(this)).on('error', def.reject).on('end', def.resolve);
		return def.promise;
	}),

	// Reduced data
	__getReducedNs: d(function (ns, keyPaths) {
		var def, result;
		def = deferred();
		result = create(null);
		this.levelDb.createReadStream({ gte: '_' + ns, lte: '_' + ns + '/\uffff' })
			.on('data', function (data) {
				var index, path;
				if (keyPaths) {
					index = data.key.indexOf('/');
					path = (index !== -1) ? data.key.slice(index + 1) : null;
					if (!keyPaths.has(path)) return; // filtered
				}
				index = data.value.indexOf('.');
				result[data.key.slice(1)] = {
					stamp: Number(data.value.slice(0, index)),
					value: data.value.slice(index + 1)
				};
			}.bind(this)).on('error', def.reject).on('end', function () { def.resolve(result); });
		return def.promise;
	}),

	// Storage import/export
	__exportAll: d(function (destDriver) {
		var def, promises = [], count = 0;
		def = deferred();
		this.levelDb.createReadStream().on('data', function (record) {
			var index, id, cat, ns, path, data;
			if (!(++count % 1000)) def.promise.emit('progress');
			index = record.value.indexOf('.');
			data = {
				value: record.value.slice(index + 1),
				stamp: Number(record.value.slice(0, index))
			};
			if (record.key[0] === '=') {
				cat = 'computed';
				id = record.key.slice(1);
				index = id.lastIndexOf(':');
				ns = id.slice(0, index);
				path = id.slice(index + 1);
			} else {
				if (record.key[0] === '_') {
					cat = 'reduced';
					id = record.key.slice(1);
				} else {
					id = record.key;
					cat = 'direct';
				}
				index = id.indexOf('/');
				ns = (index === -1) ? id : id.slice(0, index);
				path = (index === -1) ? null : id.slice(index + 1);
			}
			promises.push(destDriver.__storeRaw(cat, ns, path, data));
		}.bind(this)).on('error', function (err) { def.reject(err); }).on('end', function () {
			def.resolve(deferred.map(promises));
		});
		return def.promise;
	}),
	__clear: d(function () {
		return this.__close()(function () {
			return rmdir(this._dbOptions.path, { recursive: true, force: true })(function () {
				this._initialize();
			}.bind(this));
		}.bind(this));
	}),

	// Connection related
	__close: d(function () { return this.levelDb.closePromised(); }),

	// Driver specific
	_getComputedValue: d(function (objId, keyPath) {
		return this.levelDb.getPromised('=' + keyPath + ':' + objId, getOpts)(function (data) {
			var index = data.indexOf('.'), value = data.slice(index + 1);
			if (value[0] === '[') value = parse(value);
			return { value: value, stamp: Number(data.slice(0, index)) };
		}.bind(this), function (err) {
			if (err.notFound) return null;
			throw err;
		});
	}),
	_storeIndexedValue: d(function (objId, keyPath, data) {
		return this.levelDb.putPromised('=' + keyPath + ':' + objId,
			data.stamp + '.' + (isArray(data.value) ? stringify(data.value) : data.value));
	}),
	_getReduced: d(function (key) {
		return this.levelDb.getPromised('_' + key, getOpts)(function (value) {
			var index = value.indexOf('.');
			return { stamp: Number(value.slice(0, index)), value: value.slice(index + 1) };
		}, function (err) {
			if (err.notFound) return;
			throw err;
		});
	}),
	_storeReduced: d(function (key, data) {
		return this.levelDb.putPromised('_' + key, data.stamp + '.' + data.value);
	}),
	_loadDirect: d(function (data, filter) {
		var def, result;
		def = deferred();
		result = create(null);
		this.levelDb.createReadStream(data).on('data', function (data) {
			var index, ownerId, path;
			if (data.key[0] === '=') return; // computed record
			if (data.key[0] === '_') return; // reduced record
			if (filter) {
				index = data.key.indexOf('/');
				ownerId = (index !== -1) ? data.key.slice(0, index) : data.key;
				path = (index !== -1) ? data.key.slice(index + 1) : null;
				if (!filter(ownerId, path)) return; // filtered
			}
			index = data.value.indexOf('.');
			result[data.key] = {
				stamp: Number(data.value.slice(0, index)),
				value: data.value.slice(index + 1)
			};
		}.bind(this)).on('error', def.reject).on('end', function () { def.resolve(result); });
		return def.promise;
	}),
	_initialize: d(function () {
		var db = this.levelDb = level(this._dbOptions.path, this._dbOptions);
		db.getPromised = promisify(db.get);
		db.putPromised = promisify(db.put);
		db.delPromised = promisify(db.del);
		db.batchPromised = promisify(db.batch);
		db.closePromised = promisify(db.close);
	})
});
