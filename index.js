'use strict';

var flatten           = require('es5-ext/array/#/flatten')
  , setPrototypeOf    = require('es5-ext/object/set-prototype-of')
  , ensureString      = require('es5-ext/object/validate-stringifiable-value')
  , ensureObject      = require('es5-ext/object/valid-object')
  , d                 = require('d')
  , deferred          = require('deferred')
  , level             = require('levelup')
  , PersistenceDriver = require('dbjs-persistence/abstract')

  , isArray = Array.isArray, stringify = JSON.stringify
  , parse = JSON.parse, promisify = deferred.promisify
  , getOpts = { fillCache: false }
  , byStamp = function (a, b) { return a.data.stamp - b.data.stamp; };

var LevelDriver = module.exports = function (dbjs, data) {
	var db;
	if (!(this instanceof LevelDriver)) return new LevelDriver(dbjs, data);
	ensureObject(data);
	PersistenceDriver.call(this, dbjs, data);
	db = this.levelDb = level(ensureString(data.path), data);
	db.getPromised = promisify(db.get);
	db.putPromised = promisify(db.put);
	db.delPromised = promisify(db.del);
	db.batchPromised = promisify(db.batch);
	db.closePromised = promisify(db.close);
};
setPrototypeOf(LevelDriver, PersistenceDriver);

LevelDriver.prototype = Object.create(PersistenceDriver.prototype, {
	constructor: d(LevelDriver),

	// Any data
	_getRaw: d(function (id) {
		var index;
		if (id[0] === '_') return this._getCustom(id.slice(1));
		if (id[0] === '=') {
			index = id.lastIndexOf(':');
			return this._getIndexedValue(id.slice(index + 1), id.slice(1, index));
		}
		return this.levelDb.getPromised(id, getOpts)(function (value) {
			var index = value.indexOf('.');
			return { stamp: Number(value.slice(0, index)), value: value.slice(index + 1) };
		}.bind(this), function (err) {
			if (err.notFound) return null;
			throw err;
		});
	}),
	_getRawObject: d(function (objId) { return this._load({ gte: objId, lte: objId + '/\uffff' }); }),
	_storeRaw: d(function (id, value) {
		var index;
		if (id[0] === '_') return this._storeCustom(id.slice(1), value);
		if (id[0] === '=') {
			index = id.lastIndexOf(':');
			return this._storeIndexedValue(id.slice(index + 1), id.slice(1, index), value);
		}
		return this.levelDb.putPromised(id, value.stamp + '.' + value.value);
	}),

	// Database data
	_loadAll: d(function () {
		var count = 0;
		var promise = this._load().map(function (data) {
			if (!(++count % 1000)) promise.emit('progress');
			return this._importValue(data.id, data.data.value, data.data.stamp);
		}.bind(this)).invoke(flatten);
		return promise;
	}),
	_storeEvent: d(function (ownerId, targetPath, data) {
		var id = ownerId + (targetPath ? ('/' + targetPath) : '');
		return this.levelDb.putPromised(id, data.stamp + '.' + data.value);
	}),

	// Indexed database data
	_getIndexedValue: d(function (objId, keyPath) {
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

	// Size tracking
	_searchDirect: d(function (callback) {
		return this._load().map(function (data) {
			if (data.id.indexOf('/') === -1) return;
			callback(data.id, data.data);
		});
	}),
	_searchIndex: d(function (keyPath, callback) {
		var def = deferred();
		this.levelDb.createReadStream({ gte: '=' + keyPath + ':', lte: '=' + keyPath + ':\uffff' })
			.on('data', function (data) {
				var index, value, objId = data.key.slice(data.key.lastIndexOf(':') + 1);
				index = data.value.indexOf('.');
				value = data.value.slice(index + 1);
				if (value[0] === '[') value = parse(value);
				callback(objId, { value: value, stamp: Number(data.value.slice(0, index)) });
			}.bind(this)).on('error', function (err) { def.reject(err); }).on('end', function () {
				def.resolve();
			});
		return def.promise;
	}),

	// Custom data
	_getCustom: d(function (key) {
		return this.levelDb.getPromised('_' + key, getOpts)(function (value) {
			var index = value.indexOf('.');
			return { stamp: Number(value.slice(0, index)), value: value.slice(index + 1) };
		}, function (err) {
			if (err.notFound) return;
			throw err;
		});
	}),
	_storeCustom: d(function (key, data) {
		return this.levelDb.putPromised('_' + key, data.stamp + '.' + data.value);
	}),

	// Storage import/export
	_exportAll: d(function (destDriver) {
		var def, promises = [], count = 0;
		def = deferred();
		this.levelDb.createReadStream().on('data', function (data) {
			var index;
			if (!(++count % 1000)) def.promise.emit('progress');
			if (data.key[0] === '_') {
				promises.push(destDriver._storeRaw(data.key, data.value));
				return;
			}
			index = data.value.indexOf('.');
			promises.push(destDriver._storeRaw(data.key, {
				value: data.value.slice(index + 1),
				stamp: Number(data.value.slice(0, index))
			}));
		}.bind(this)).on('error', function (err) { def.reject(err); }).on('end', function () {
			def.resolve(deferred.map(promises));
		});
		return def.promise;
	}),

	// Connection related
	_close: d(function () { return this.levelDb.closePromised(); }),

	// Driver specific
	_load: d(function (data) {
		var def, result;
		def = deferred();
		result = [];
		this.levelDb.createReadStream(data).on('data', function (data) {
			var index;
			if (data.key[0] === '=') return; // computed record
			if (data.key[0] === '_') return; // custom record
			index = data.value.indexOf('.');
			result.push({
				id: data.key,
				data: { stamp: Number(data.value.slice(0, index)), value: data.value.slice(index + 1) }
			});
		}.bind(this)).on('error', function (err) { def.reject(err); }).on('end', function () {
			def.resolve(result.sort(byStamp));
		});
		return def.promise;
	})
});
