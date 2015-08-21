'use strict';

var clear          = require('es5-ext/array/#/clear')
  , assign         = require('es5-ext/object/assign')
  , ensureCallable = require('es5-ext/object/valid-callable')
  , ensureString   = require('es5-ext/object/validate-stringifiable-value')
  , ensureObject   = require('es5-ext/object/valid-object')
  , d              = require('d')
  , lazy           = require('d/lazy')
  , deferred       = require('deferred')
  , ensureDatabase = require('dbjs/valid-dbjs')
  , Event          = require('dbjs/_setup/event')
  , serialize      = require('dbjs/_setup/serialize/value')
  , unserialize    = require('dbjs/_setup/unserialize/value')
  , once           = require('timers-ext/once')
  , level          = require('levelup')

  , isModelId = RegExp.prototype.test.bind(/^[A-Z]/)
  , stringify = JSON.stringify, promisify = deferred.promisify
  , getOpts = { fillCache: false };

var LevelDriver = module.exports = Object.defineProperties(function (dbjs, data) {
	var db, autoSaveFilter;
	if (!(this instanceof LevelDriver)) return new LevelDriver(dbjs, data);
	this.db = ensureDatabase(dbjs);
	ensureObject(data);
	db = this.levelDb = level(ensureString(data.path), data);
	db.getPromised = promisify(db.get);
	db.putPromised = promisify(db.put);
	db.batchPromised = promisify(db.batch);
	db.closePromised = promisify(db.close);
	autoSaveFilter = (data.autoSaveFilter != null)
		? ensureCallable(data.autoSaveFilter) : this.constructor.defaultAutoSaveFilter;
	dbjs.objects.on('update', function (event) {
		if (event.sourceId === 'persistentLayer') return;
		if (!autoSaveFilter(event)) return;
		this._eventsToStore.push(event);
		this._storeEvents();
	}.bind(this));
}, {
	defaultAutoSaveFilter: d(function (event) { return !isModelId(event.object.master.__id__); })
});
Object.defineProperties(LevelDriver.prototype, assign({
	_loadValue: d(function (key, value) {
		var index = value.indexOf('.'), stamp = Number(value.slice(0, index)), proto;
		value = unserialize(value.slice(index + 1), this.db.objects);
		if (value && value.__id__ && (value.constructor.prototype === value)) proto = value.constructor;
		return new Event(this.db.objects.unserialize(key, proto), value, stamp, 'persistentLayer');
	}),
	_load: d(function (data) {
		var def, result;
		def = deferred();
		result = [];
		this.levelDb.createReadStream(data).on('data', function (data) {
			if (data.key[0] === '_') return; // custom record
			result.push(this._loadValue(data.key, data.value));
		}.bind(this)).on('error', function (err) { def.reject(err); }).on('end', function () {
			def.resolve(result);
		});
		return def.promise;
	}),
	getCustom: d(function (key) {
		key = ensureString(key);
		if (key[0] !== '_') {
			throw new Error("Provided key " + stringify(key) + " is not a valid custom key");
		}
		return this.levelDb.getPromised(key, getOpts)(function (value) { return value; },
			function (err) {
				if (err.notFound) return null;
				throw err;
			});
	}),
	loadValue: d(function (id) {
		return this.levelDb.getPromised(id, getOpts)(function (value) {
			return this._loadValue(id, value);
		}.bind(this), function (err) {
			if (err.notFound) return null;
			throw err;
		});
	}),
	loadObject: d(function (id) {
		id = ensureString(id);
		return this._load({ gte: id, lte: id + '/\uffff' });
	}),
	loadAll: d(function () { return this._load(); }),
	storeCustom: d(function (key, value) {
		key = ensureString(key);
		if (key[0] !== '_') {
			throw new Error("Provided key " + stringify(key) + " is not a valid custom key");
		}
		return this.levelDb.putPromised(key, value);
	}),
	storeValue: d(function (event) {
		return this.levelDb.putPromised(event.object.__valueId__,
			event.stamp + '.' + serialize(event.value));
	}),
	storeValues: d(function (events) {
		return this.levelDb.batchPromised(events.map(function (event) {
			return { type: 'put', key: event.object.__valueId__,
				value: event.stamp + '.' + serialize(event.value) };
		}));
	}),
	close: d(function () { return this.levelDb.closePromised(); })
}, lazy({
	_eventsToStore: d(function () { return []; }),
	_storeEvents: d(function () {
		return once(function () {
			this.storeValues(this._eventsToStore);
			clear.call(this._eventsToStore);
		}.bind(this));
	})
})));
