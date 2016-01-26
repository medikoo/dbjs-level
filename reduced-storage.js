'use strict';

var assign         = require('es5-ext/object/assign')
  , setPrototypeOf = require('es5-ext/object/set-prototype-of')
  , d              = require('d')
  , lazy           = require('d/lazy')
  , deferred       = require('deferred')
  , resolve        = require('path').resolve
  , mkdir          = require('fs2/mkdir')
  , rmdir          = require('fs2/rmdir')
  , level          = require('levelup')
  , ReducedStorage = require('dbjs-persistence/reduced-storage')

  , create = Object.create
  , getOpts = { fillCache: false };

var makeDb = function (path, options) {
	return mkdir(path, { intermediate: true })(function () { return level(path, options); });
};

var LevelReducedStorage = module.exports = function (driver) {
	if (!(this instanceof LevelReducedStorage)) return new LevelReducedStorage(driver);
	ReducedStorage.call(this, driver);
	this.dbPath = resolve(driver.dbPath, '_reduced');
};
setPrototypeOf(LevelReducedStorage, ReducedStorage);

LevelReducedStorage.prototype = Object.create(ReducedStorage.prototype, assign({
	constructor: d(LevelReducedStorage),

	// Any data
	__get: d(function (ns, path) {
		return this._get_(ns + (path ? ('/' + path) : ''));
	}),
	__store: d(function (ns, path, data) {
		return this._store_(ns + (path ? ('/' + path) : ''), data);
	}),

	__getObject: d(function (ns, keyPaths) {
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

	// Storage import/export
	__exportAll: d(function (destStorage) {
		var count = 0;
		var promise = deferred(
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
					promises.push(destStorage._storeRaw(ns, path, data));
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
			if (this.hasOwnProperty('reducedDb')) {
				return rmdir(this.dbPath, { recursive: true, force: true });
			}
		}.bind(this))(function () {
			delete this.reducedDb;
		}.bind(this));
	}),
	__drop: d(function () { return this.__clear(); }),

	// Connection related
	__close: d(function () {
		return deferred(this.hasOwnProperty('reducedDb') && this.reducedDb.invokeAsync('close'));
	}),

	// Driver specific
	_get_: d(function (key) {
		return this.reducedDb.invokeAsync('get', key, getOpts)(function (value) {
			var index = value.indexOf('.');
			return { stamp: Number(value.slice(0, index)), value: value.slice(index + 1) };
		}, function (err) {
			if (err.notFound) return;
			throw err;
		});
	}),
	_store_: d(function (key, data) {
		return this.reducedDb.invokeAsync('put', key, data.stamp + '.' + data.value);
	})
}, lazy({
	reducedDb: d(function () {
		return makeDb(this.dbPath, this.driver._dbOptions);
	})
})));
