'use strict';

var deferred = require('deferred')
  , resolve  = require('path').resolve
  , rmdir    = require('fs2/rmdir')
  , Database = require('dbjs')
  , Event    = require('dbjs/_setup/event')

  , dbPath = resolve(__dirname, 'test-db');

require('leveldown');

module.exports = function (t, a, d) {
	var db = new Database()
	  , driver = t(db, { path: dbPath })
	  , aaa = db.Object.newNamed('aaa')
	  , bar = db.Object.newNamed('bar')
	  , foo = db.Object.newNamed('foo')
	  , zzz = db.Object.newNamed('zzz');

	deferred(
		driver.storeEvent(zzz._lastOwnEvent_),
		driver.storeEvent(bar._lastOwnEvent_),
		driver.storeEvent(foo._lastOwnEvent_),
		driver.storeEvent(aaa._lastOwnEvent_)
	)(function () {
		return driver.storeEvents([
			new Event(aaa.getOwnDescriptor('sdfds'), 'sdfs'),
			new Event(zzz.getOwnDescriptor('sdfffds'), 'sdfs'),
			new Event(foo.getOwnDescriptor('raz'), 'marko'),
			new Event(bar.getOwnDescriptor('miszka'), 343),
			new Event(foo.getOwnDescriptor('bal'), false),
			new Event(foo.getOwnDescriptor('ole'), 767),
			new Event(bar.getOwnDescriptor('ssss'), 343)
		])(function () {
			return driver.close();
		})(function () {
			var db = new Database()
			  , driver = t(db, { path: dbPath });
			return driver.loadObject('foo')(function () {
				a(db.foo.constructor, db.Object);
				a(db.aaa, undefined);
				a(db.bar, undefined);
				a(db.zzz, undefined);
				a(db.foo.raz, 'marko');
				a(db.foo.bal, false);
				a(db.foo.ole, 767);
				return driver.loadValue('bar')(function (event) {
					a(event.object, db.bar);
					a(event.value, db.Object.prototype);
					a(db.bar.constructor, db.Object);
					a(db.bar.miszka, undefined);
				});
			})(function () {
				return driver.loadValue('bar/miszka')(function (event) {
					a(db.bar.miszka, 343);
				});
			})(function () {
				return driver.close();
			});
		})(function () {
			var db = new Database()
			  , driver = t(db, { path: dbPath });
			return driver.loadAll()(function () {
				a(db.foo.constructor, db.Object);
				a(db.foo.raz, 'marko');
				a(db.foo.bal, false);
				a(db.foo.ole, 767);
				a(db.aaa.constructor, db.Object);
				a(db.zzz.constructor, db.Object);
				a(db.bar.miszka, 343);
			})(function () {
				return driver.close();
			});
		})(function () {
			return rmdir(dbPath, { recursive: true, force: true });
		});
	}).done(function () { d(); }, d);
};
