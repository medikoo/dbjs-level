'use strict';

var assign            = require('es5-ext/object/assign')
  , normalizeOptions  = require('es5-ext/object/normalize-options')
  , setPrototypeOf    = require('es5-ext/object/set-prototype-of')
  , toArray           = require('es5-ext/object/to-array')
  , ensureString      = require('es5-ext/object/validate-stringifiable-value')
  , ensureObject      = require('es5-ext/object/valid-object')
  , d                 = require('d')
  , lazy              = require('d/lazy')
  , deferred          = require('deferred')
  , resolveKeyPath    = require('dbjs/_setup/utils/resolve-key-path')
  , resolve           = require('path').resolve
  , mkdir             = require('fs2/mkdir')
  , rmdir             = require('fs2/rmdir')
  , level             = require('levelup')
  , PersistenceDriver = require('dbjs-persistence/abstract')

  , isArray = Array.isArray, create = Object.create, stringify = JSON.stringify, parse = JSON.parse
  , getOpts = { fillCache: false }
  , byStamp = function (a, b) { return this[a] - this[b]; };

var makeDb = function (path, options) {
	return mkdir(path, { intermediate: true })(function () { return level(path, options); });
};

var LevelDriver = module.exports = function (dbjs, data) {
	if (!(this instanceof LevelDriver)) return new LevelDriver(dbjs, data);
	this._dbOptions = normalizeOptions(ensureObject(data));
	// Below is workaround for https://github.com/Raynos/xtend/pull/28
	this._dbOptions.hasOwnProperty = Object.prototype.hasOwnProperty;
	this._dbOptions.path = ensureString(this._dbOptions.path);
	PersistenceDriver.call(this, dbjs, data);
};
setPrototypeOf(LevelDriver, PersistenceDriver);

LevelDriver.prototype = Object.create(PersistenceDriver.prototype, assign({
	constructor: d(LevelDriver),

	// Any data
	__getRaw: d(function (cat, ns, path) {
		if (cat === 'reduced') return this._getReduced(ns + (path ? ('/' + path) : ''));
		if (cat === 'computed') return this._getComputed(path, ns);
		return this._getDirect(ns, path);
	}),
	__storeRaw: d(function (cat, ns, path, data) {
		if (cat === 'reduced') return this._storeReduced(ns + (path ? ('/' + path) : ''), data);
		if (cat === 'computed') return this._storeComputed(path, ns, data);
		return this._storeDirect(ns, path, data);
	}),

	// Direct data
	__getDirectObject: d(function (ownerId, keyPaths) {
		return this._loadDirect({ gte: ownerId, lte: ownerId + '/\uffff' },
			keyPaths && function (ownerId, path) {
				if (!path) return true;
				return keyPaths.has(resolveKeyPath(ownerId + '/' + path));
			});
	}),
	__getDirectAllObjectIds: d(function () {
		return this.directDb(function (db) {
			var def = deferred(), data = create(null);
			db.createReadStream().on('data', function (data) {
				var index = data.key.indexOf('/'), ownerId;
				if (index === -1) {
					index = data.value.indexOf('.');
					data[data.key] = Number(data.value.slice(0, index));
					return;
				}
				ownerId = data.key.slice(0, index);
				if (!data[ownerId]) data[ownerId] = 0;
				return;
			}).on('error', def.reject).on('end', function () {
				return toArray(data, function (el, id) { return id; }, this, byStamp);
			});
			return def.promise;
		});
	}),
	__getDirectAll: d(function () { return this._loadDirect(); }),

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
				}).on('error', def.reject).on('end', function () { def.resolve(result); });
			return def.promise;
		});
	}),

	// Size tracking
	__searchDirect: d(function (keyPath, callback) {
		return this.directDb(function (db) {
			var def = deferred(), stream = db.createReadStream();
			stream.on('data', function (data) {
				var index, result, recordKeyPath = resolveKeyPath(data.key);
				if (!keyPath) {
					if (recordKeyPath) return;
				} else if (keyPath !== recordKeyPath) {
					return;
				}
				index = data.value.indexOf('.');
				result = callback(data.key, {
					stamp: Number(data.value.slice(0, index)),
					value: data.value.slice(index + 1)
				});
				if (result) stream.destroy();
			}).on('error', def.reject).on('end', def.resolve);
			return def.promise;
		});
	}),
	__searchComputed: d(function (keyPath, callback) {
		return this.computedDb(function (db) {
			var def = deferred()
			  , stream = db.createReadStream({ gte: keyPath + ':', lte: keyPath + ':\uffff' });

			stream.on('data', function (data) {
				var index, value, ownerId = data.key.slice(data.key.lastIndexOf(':') + 1);
				index = data.value.indexOf('.');
				value = data.value.slice(index + 1);
				if (value[0] === '[') value = parse(value);
				if (callback(ownerId, { value: value, stamp: Number(data.value.slice(0, index)) })) {
					stream.destroy();
				}
			}).on('error', def.reject).on('end', def.resolve);
			return def.promise;
		});
	}),

	// Storage import/export
	__exportAll: d(function (destDriver) {
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
					promises.push(destDriver.__storeRaw('direct', ns, path, data));
				}.bind(this)).on('error', function (err) { def.reject(err); }).on('end', function () {
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
					promises.push(destDriver.__storeRaw('computed', ns, path, data));
				}.bind(this)).on('error', function (err) { def.reject(err); }).on('end', function () {
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
					promises.push(destDriver.__storeRaw('reduced', ns, path, data));
				}.bind(this)).on('error', function (err) { def.reject(err); }).on('end', function () {
					def.resolve(deferred.map(promises));
				});
				return def.promise;
			})
		)(Function.prototype);
		return promise;
	}),
	__clear: d(function () {
		return this.__close()(function () {
			return rmdir(this._dbOptions.path, { recursive: true, force: true })(function () {
				delete this.directDb;
				delete this.computedDb;
				delete this.reducedDb;
			}.bind(this));
		}.bind(this));
	}),

	// Connection related
	__close: d(function () {
		return deferred(
			this.hasOwnProperty('directDb') && this.directDb.invokeAsync('close'),
			this.hasOwnProperty('computedDb') && this.computedDb.invokeAsync('close'),
			this.hasOwnProperty('reducedDb') && this.reducedDb.invokeAsync('close')
		);
	}),

	// Driver specific
	_getDirect: d(function (ownerId, path) {
		var id = ownerId + (path ? ('/' + path) : '');
		return this.directDb.invokeAsync('get', id, getOpts)(function (value) {
			var index = value.indexOf('.');
			return { stamp: Number(value.slice(0, index)), value: value.slice(index + 1) };
		}.bind(this), function (err) {
			if (err.notFound) return null;
			throw err;
		});
	}),
	_storeDirect: d(function (ownerId, path, data) {
		return this.directDb.invokeAsync('put', ownerId + (path ? ('/' + path) : ''),
			data.stamp + '.' + data.value);
	}),
	_getComputed: d(function (ownerId, keyPath) {
		return this.computedDb.invokeAsync('get', keyPath + ':' + ownerId, getOpts)(function (data) {
			var index = data.indexOf('.'), value = data.slice(index + 1);
			if (value[0] === '[') value = parse(value);
			return { value: value, stamp: Number(data.slice(0, index)) };
		}.bind(this), function (err) {
			if (err.notFound) return null;
			throw err;
		});
	}),
	_storeComputed: d(function (ownerId, keyPath, data) {
		return this.computedDb.invokeAsync('put', keyPath + ':' + ownerId,
			data.stamp + '.' + (isArray(data.value) ? stringify(data.value) : data.value));
	}),
	_getReduced: d(function (key) {
		return this.reducedDb.invokeAsync('get', key, getOpts)(function (value) {
			var index = value.indexOf('.');
			return { stamp: Number(value.slice(0, index)), value: value.slice(index + 1) };
		}, function (err) {
			if (err.notFound) return;
			throw err;
		});
	}),
	_storeReduced: d(function (key, data) {
		return this.reducedDb.invokeAsync('put', key, data.stamp + '.' + data.value);
	}),
	_loadDirect: d(function (data, filter) {
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
			}.bind(this)).on('error', def.reject).on('end', function () { def.resolve(result); });
			return def.promise;
		});
	})
}, lazy({
	directDb: d(function () {
		return makeDb(resolve(this._dbOptions.path, 'direct'), this._dbOptions);
	}),
	computedDb: d(function () {
		return makeDb(resolve(this._dbOptions.path, 'computed'), this._dbOptions);
	}),
	reducedDb: d(function () {
		return makeDb(resolve(this._dbOptions.path, 'reduced'), this._dbOptions);
	})
})));
