'use strict';

var assign              = require('es5-ext/object/assign')
  , setPrototypeOf      = require('es5-ext/object/set-prototype-of')
  , camelToHyphen       = require('es5-ext/string/#/camel-to-hyphen')
  , d                   = require('d')
  , lazy                = require('d/lazy')
  , deferred            = require('deferred')
  , resolveKeyPath      = require('dbjs/_setup/utils/resolve-key-path')
  , resolve             = require('path').resolve
  , mkdir               = require('fs2/mkdir')
  , rmdir               = require('fs2/rmdir')
  , Storage             = require('dbjs-persistence/storage')
  , resolveValue        = require('dbjs-persistence/lib/resolve-direct-value')
  , filterComputedValue = require('dbjs-persistence/lib/filter-computed-value')

  , isArray = Array.isArray, create = Object.create, stringify = JSON.stringify, parse = JSON.parse
  , getOpts = { fillCache: false };

var LevelStorage = module.exports = function (driver, name/*, options*/) {
	if (!(this instanceof LevelStorage)) return new LevelStorage(driver, name, arguments[2]);
	Storage.call(this, driver, name, arguments[2]);
	this.dbPath = resolve(driver.dbPath, camelToHyphen.call(name));
};
setPrototypeOf(LevelStorage, Storage);

LevelStorage.prototype = Object.create(Storage.prototype, assign({
	constructor: d(LevelStorage),

	// Any data
	__getRaw: d(function (cat, ns, path) {
		if (cat === 'reduced') return this._getReduced_(ns + (path ? ('/' + path) : ''));
		if (cat === 'computed') return this._getComputed_(path, ns);
		return this._getDirect_(ns, path);
	}),
	__storeRaw: d(function (cat, ns, path, data) {
		if (cat === 'reduced') return this._storeReduced_(ns + (path ? ('/' + path) : ''), data);
		if (cat === 'computed') return this._storeComputed_(path, ns, data);
		return this._storeDirect_(ns, path, data);
	}),

	// Direct data
	__getObject: d(function (ownerId, objectPath, keyPaths) {
		var query = {
			gte: ownerId + (objectPath ? '/' + objectPath : ''),
			lte: ownerId + (objectPath ? '/' + objectPath : '') + '/\uffff'
		};
		return this._loadDirect_(query, keyPaths && function (ownerId, path) {
			if (!path) return true;
			return keyPaths.has(resolveKeyPath(ownerId + '/' + path));
		});
	}),
	__getAllObjectIds: d(function () {
		return this.directDb(function (db) {
			var def = deferred(), data = create(null);
			db.createReadStream().on('data', function (record) {
				var index = record.key.indexOf('/');
				if (index === -1) {
					index = record.value.indexOf('.');
					data[record.key] = {
						value: record.value.slice(index + 1),
						stamp: Number(record.value.slice(0, index))
					};
				}
			}).on('error', def.reject).on('close', function () { def.resolve(data); });
			return def.promise;
		});
	}),
	__getAll: d(function () { return this._loadDirect_(); }),

	// Reduced data
	__getReducedObject: d(function (ns, keyPaths) {
		return this.reducedDb(function (db) {
			var def, result;
			def = deferred();
			result = create(null);
			db.createReadStream({ gte: ns, lte: ns + '/\uffff' })
				.on('data', function (data) {
					var index, path;
					if (keyPaths) {
						index = data.key.indexOf('/');
						path = (index !== -1) ? data.key.slice(index + 1) : null;
						if (!keyPaths.has(path)) return; // filtered
					}
					index = data.value.indexOf('.');
					result[data.key] = {
						stamp: Number(data.value.slice(0, index)),
						value: data.value.slice(index + 1)
					};
				}).on('error', def.reject).on('close', function () { def.resolve(result); });
			return def.promise;
		});
	}),

	__search: d(function (keyPath, value, callback) {
		return this.directDb(function (db) {
			var def = deferred(), stream = db.createReadStream();
			stream.on('data', function (data) {
				var index, result, recordKeyPath = resolveKeyPath(data.key), recordValue
				  , resolvedValue, ownerId, path;
				if (keyPath !== undefined) {
					if (!keyPath) {
						if (recordKeyPath) return;
					} else if (keyPath !== recordKeyPath) {
						return;
					}
				}
				index = data.value.indexOf('.');
				recordValue = data.value.slice(index + 1);
				if (value != null) {
					ownerId = data.key.split('/', 1)[0];
					path = data.key.slice(ownerId.length + 1) || null;
					resolvedValue = resolveValue(ownerId, path, recordValue);
					if (value !== resolvedValue) return;
				}
				result = callback(data.key, {
					stamp: Number(data.value.slice(0, index)),
					value: recordValue
				});
				if (result) stream.destroy();
			}).on('error', def.reject).on('close', def.resolve);
			return def.promise;
		});
	}),
	__searchComputed: d(function (keyPath, value, callback) {
		return this.computedDb(function (db) {
			var def = deferred(), stream, query;
			if (keyPath) query = { gte: keyPath + ':', lte: keyPath + ':\uffff' };
			stream = db.createReadStream(query);

			stream.on('data', function (data) {
				var index, recordValue, ownerId = data.key.slice(data.key.lastIndexOf(':') + 1);
				index = data.value.indexOf('.');
				recordValue = data.value.slice(index + 1);
				if (recordValue[0] === '[') recordValue = parse(recordValue);
				if ((value != null) && !filterComputedValue(value, recordValue)) return;
				if (callback(ownerId, { value: recordValue, stamp: Number(data.value.slice(0, index)) })) {
					stream.destroy();
				}
			}).on('error', def.reject).on('close', def.resolve);
			return def.promise;
		});
	}),

	// Storage import/export
	__exportAll: d(function (destStorage) {
		var count = 0;
		var promise = deferred(
			this.directDb(function (db) {
				var def, promises = [];
				def = deferred();
				db.createReadStream().on('data', function (record) {
					var index, ns, path, data;
					if (!(++count % 1000)) promise.emit('progress');
					index = record.value.indexOf('.');
					data = {
						value: record.value.slice(index + 1),
						stamp: Number(record.value.slice(0, index))
					};
					index = record.key.indexOf('/');
					ns = (index === -1) ? record.key : record.key.slice(0, index);
					path = (index === -1) ? null : record.key.slice(index + 1);
					promises.push(destStorage.__storeRaw('direct', ns, path, data));
				}.bind(this)).on('error', function (err) { def.reject(err); }).on('close', function () {
					def.resolve(deferred.map(promises));
				});
				return def.promise;
			}),
			this.computedDb(function (db) {
				var def, promises = [];
				def = deferred();
				db.createReadStream().on('data', function (record) {
					var index, ns, path, data;
					if (!(++count % 1000)) promise.emit('progress');
					index = record.value.indexOf('.');
					data = {
						value: record.value.slice(index + 1),
						stamp: Number(record.value.slice(0, index))
					};
					index = record.key.lastIndexOf(':');
					ns = record.key.slice(0, index);
					path = record.key.slice(index + 1);
					promises.push(destStorage.__storeRaw('computed', ns, path, data));
				}.bind(this)).on('error', function (err) { def.reject(err); }).on('close', function () {
					def.resolve(deferred.map(promises));
				});
				return def.promise;
			}),
			this.reducedDb(function (db) {
				var def, promises = [];
				def = deferred();
				db.createReadStream().on('data', function (record) {
					var index, ns, path, data;
					if (!(++count % 1000)) promise.emit('progress');
					index = record.value.indexOf('.');
					data = {
						value: record.value.slice(index + 1),
						stamp: Number(record.value.slice(0, index))
					};
					index = record.key.indexOf('/');
					ns = (index === -1) ? record.key : record.key.slice(0, index);
					path = (index === -1) ? null : record.key.slice(index + 1);
					promises.push(destStorage.__storeRaw('reduced', ns, path, data));
				}.bind(this)).on('error', function (err) { def.reject(err); }).on('close', function () {
					def.resolve(deferred.map(promises));
				});
				return def.promise;
			})
		)(Function.prototype);
		return promise;
	}),
	__clear: d(function () {
		return this.__close()(function () {
			return rmdir(this.dbPath, { recursive: true, force: true, loose: true })(function () {
				delete this.directDb;
				delete this.computedDb;
				delete this.reducedDb;
			}.bind(this));
		}.bind(this));
	}),
	__drop: d(function () { return this.__clear(); }),

	// Connection related
	__close: d(function () {
		return deferred(
			this.hasOwnProperty('directDb') && this.directDb.invokeAsync('close'),
			this.hasOwnProperty('computedDb') && this.computedDb.invokeAsync('close'),
			this.hasOwnProperty('reducedDb') && this.reducedDb.invokeAsync('close')
		);
	}),

	// Driver specific
	_getDirect_: d(function (ownerId, path) {
		var id = ownerId + (path ? ('/' + path) : '');
		return this.directDb.invokeAsync('get', id, getOpts)(function (value) {
			var index = value.indexOf('.');
			return { stamp: Number(value.slice(0, index)), value: value.slice(index + 1) };
		}.bind(this), function (err) {
			if (err.notFound) return null;
			throw err;
		});
	}),
	_storeDirect_: d(function (ownerId, path, data) {
		return this.directDb.invokeAsync('put', ownerId + (path ? ('/' + path) : ''),
			data.stamp + '.' + data.value);
	}),
	_getComputed_: d(function (ownerId, keyPath) {
		return this.computedDb.invokeAsync('get', keyPath + ':' + ownerId, getOpts)(function (data) {
			var index = data.indexOf('.'), value = data.slice(index + 1);
			if (value[0] === '[') value = parse(value);
			return { value: value, stamp: Number(data.slice(0, index)) };
		}.bind(this), function (err) {
			if (err.notFound) return null;
			throw err;
		});
	}),
	_storeComputed_: d(function (ownerId, keyPath, data) {
		return this.computedDb.invokeAsync('put', keyPath + ':' + ownerId,
			data.stamp + '.' + (isArray(data.value) ? stringify(data.value) : data.value));
	}),
	_getReduced_: d(function (key) {
		return this.reducedDb.invokeAsync('get', key, getOpts)(function (value) {
			var index = value.indexOf('.');
			return { stamp: Number(value.slice(0, index)), value: value.slice(index + 1) };
		}, function (err) {
			if (err.notFound) return;
			throw err;
		});
	}),
	_storeReduced_: d(function (key, data) {
		return this.reducedDb.invokeAsync('put', key, data.stamp + '.' + data.value);
	}),
	_loadDirect_: d(function (data, filter) {
		return this.directDb(function (db) {
			var def, result;
			def = deferred();
			result = create(null);
			db.createReadStream(data).on('data', function (data) {
				var index, ownerId, path;
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
			}.bind(this)).on('error', def.reject).on('close', function () { def.resolve(result); });
			return def.promise;
		});
	}),
	_makeDb_: d(function (path) {
		return mkdir(path, { intermediate: true })(function () {
			return this.driver.levelConstructor(path, this.driver._dbOptions);
		}.bind(this));
	})
}, lazy({
	directDb: d(function () { return this._makeDb_(resolve(this.dbPath, 'direct')); }),
	computedDb: d(function () { return this._makeDb_(resolve(this.dbPath, 'computed')); }),
	reducedDb: d(function () { return this._makeDb_(resolve(this.dbPath, 'reduced')); })
})));
