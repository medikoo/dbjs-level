'use strict';

var setPrototypeOf    = require('es5-ext/object/set-prototype-of')
  , ensureString      = require('es5-ext/object/validate-stringifiable-value')
  , ensureObject      = require('es5-ext/object/valid-object')
  , d                 = require('d')
  , deferred          = require('deferred')
  , serialize         = require('dbjs/_setup/serialize/value')
  , level             = require('levelup')
  , PersistenceDriver = require('dbjs-persistence/abstract')

  , isArray = Array.isArray, stringify = JSON.stringify
  , create = Object.create, parse = JSON.parse, promisify = deferred.promisify
  , getOpts = { fillCache: false };

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
	_load: d(function (data) {
		var def, result;
		def = deferred();
		result = [];
		this.levelDb.createReadStream(data).on('data', function (data) {
			var index, event;
			if (data.key[0] === '=') return; // computed record
			if (data.key[0] === '_') return; // custom record
			index = data.value.indexOf('.');
			event = this._importValue(data.key, data.value.slice(index + 1),
				Number(data.value.slice(0, index)));
			if (event) result.push(event);
		}.bind(this)).on('error', function (err) { def.reject(err); }).on('end', function () {
			def.resolve(result);
		});
		return def.promise;
	}),
	_getCustom: d(function (key) {
		return this.levelDb.getPromised(key, getOpts)(function (value) { return value; },
			function (err) {
				if (err.notFound) return;
				throw err;
			});
	}),
	_loadValue: d(function (id) {
		return this.levelDb.getPromised(id, getOpts)(function (value) {
			var index = value.indexOf('.');
			return this._importValue(id, value.slice(index + 1), Number(value.slice(0, index)));
		}.bind(this), function (err) {
			if (err.notFound) return null;
			throw err;
		});
	}),
	_loadObject: d(function (id) { return this._load({ gte: id, lte: id + '/\uffff' }); }),
	_loadAll: d(function () { return this._load(); }),
	_storeCustom: d(function (key, value) {
		if (value === undefined) return this.levelDb.delPromised(key);
		return this.levelDb.putPromised(key, value);
	}),
	_storeEvent: d(function (event) {
		return this.levelDb.putPromised(event.object.__valueId__,
			event.stamp + '.' + serialize(event.value));
	}),
	_storeEvents: d(function (events) {
		return this.levelDb.batchPromised(events.map(function (event) {
			return { type: 'put', key: event.object.__valueId__,
				value: event.stamp + '.' + serialize(event.value) };
		}));
	}),
	_getComputed: d(function (id) {
		return this.levelDb.getPromised('=' + id, getOpts)(function (data) {
			var index = data.indexOf('.'), value = data.slice(index + 1);
			if (value[0] === '[') value = parse(value);
			return { value: value, stamp: Number(data.slice(0, index)) };
		}.bind(this), function (err) {
			if (err.notFound) return null;
			throw err;
		});
	}),
	_getAllComputed: d(function (keyPath) {
		var def, map = create(null);
		def = deferred();
		this.levelDb.createReadStream({ gte: '=', lte: '=\uffff' }).on('data', function (data) {
			var index, id = data.key.slice(1), value
			  , objId = id.split('/', 1)[0], localKeyPath = id.slice(objId.length + 1);
			if (localKeyPath !== keyPath) return;
			index = data.value.indexOf('.');
			value = data.value.slice(index + 1);
			if (isArray(value)) value = parse(value);
			map[id] = { value: value, stamp: Number(data.value.slice(0, index)) };
		}.bind(this)).on('error', function (err) { def.reject(err); }).on('end', function () {
			def.resolve(map);
		});
		return def.promise;
	}),
	_storeComputed: d(function (id, value, stamp) {
		return this.levelDb.putPromised('=' + id,
			stamp + '.' + (isArray(value) ? stringify(value) : value));
	}),
	_close: d(function () { return this.levelDb.closePromised(); })
});
