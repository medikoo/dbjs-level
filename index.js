'use strict';

var ensureCallable = require('es5-ext/object/valid-callable')
  , ensureString   = require('es5-ext/object/validate-stringifiable-value')
  , ensureObject   = require('es5-ext/object/valid-object')
  , deferred       = require('deferred')
  , ensureDatabase = require('dbjs/valid-dbjs')
  , Event          = require('dbjs/_setup/event')
  , serialize      = require('dbjs/_setup/serialize/value')
  , unserialize    = require('dbjs/_setup/unserialize/value')
  , level          = require('levelup')

  , isModelId = RegExp.prototype.test.bind(/^[A-Z]/)
  , stringify = JSON.stringify, promisify = deferred.promisify
  , getOpts = { fillCache: false };

var loadValue = function (dbjs, key, value) {
	var index = value.indexOf('.'), stamp = Number(value.slice(0, index)), proto;
	value = unserialize(value.slice(index + 1), dbjs.objects);
	if (value && value.__id__ && (value.constructor.prototype === value)) proto = value.constructor;
	return new Event(dbjs.objects.unserialize(key, proto), value, stamp, 'persistentLayer');
};

var defaultSaveFilter = function (event) { return !isModelId(event.object.master.__id__); };

module.exports = function (dbjs, conf/*, options*/) {
	var db, load, saveFilter, storeValue;
	ensureDatabase(dbjs);
	ensureObject(conf);
	db = level(ensureString(conf.path), arguments[1]);
	saveFilter = (conf.saveFilter != null) ? ensureCallable(conf.saveFilter) : defaultSaveFilter;
	db.getPromised = promisify(db.get);
	db.putPromised = promisify(db.put);
	db.batchPromised = promisify(db.batch);
	db.closePromised = promisify(db.close);
	load = function (conf) {
		var def, result;
		def = deferred();
		result = [];
		db.createReadStream(conf).on('data', function (data) {
			if (data.key[0] === '_') return; // custom record
			result.push(loadValue(dbjs, data.key, data.value));
		}).on('error', function (err) { def.reject(err); }).on('end', function () {
			def.resolve(result);
		});
		return def.promise;
	};
	dbjs.objects.on('update', function (event) {
		if (event.sourceId === 'persistentLayer') return;
		if (!saveFilter(event)) return;
		storeValue(event);
	});
	return {
		getCustom: function (key) {
			key = ensureString(key);
			if (key[0] !== '_') {
				throw new Error("Provided key " + stringify(key) + " is not a valid custom key");
			}
			return db.getPromised(key, getOpts)(function (value) { return value; }, function (err) {
				if (err.notFound) return null;
				throw err;
			});
		},
		loadValue: function (id) {
			return db.getPromised(id, getOpts)(function (value) {
				return loadValue(dbjs, id, value);
			}, function (err) {
				if (err.notFound) return null;
				throw err;
			});
		},
		loadObject: function (id) {
			id = ensureString(id);
			return load({ gte: id, lte: id + '/\uffff' });
		},
		loadAll: function () { return load(); },
		storeCustom: function (key, value) {
			key = ensureString(key);
			if (key[0] !== '_') {
				throw new Error("Provided key " + stringify(key) + " is not a valid custom key");
			}
			return db.putPromised(key, value);
		},
		storeValue: storeValue = function (event) {
			return db.putPromised(event.object.__valueId__, event.stamp + '.' + serialize(event.value));
		},
		storeValues: function (events) {
			return db.batchPromised(events.map(function (event) {
				return { type: 'put', key: event.object.__valueId__,
					value: event.stamp + '.' + serialize(event.value) };
			}));
		},
		close: function () { return db.closePromised(); }
	};
};
