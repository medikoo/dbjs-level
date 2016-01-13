'use strict';

var resolve           = require('path').resolve
  , rmdir             = require('fs2/rmdir')
  , getTests          = require('dbjs-persistence/test/_common')
  , storageSplitTests = require('dbjs-persistence/test/_storage-split')

  , dbPath = resolve(__dirname, 'test-db')
  , dbSplitPath = resolve(__dirname, 'test-db-split')
  , dbCopyPath = resolve(__dirname, 'test-db-copy')
  , tests = getTests({ path: dbPath }, { path: dbCopyPath });

module.exports = function (t, a, d) {
	return tests.apply(null, arguments)(function () {
		return rmdir(dbCopyPath, { recursive: true, force: true });
	})(function () {
		return storageSplitTests(t, { path: dbSplitPath }, a)(function () {
			return rmdir(dbSplitPath, { recursive: true, force: true });
		});
	}).done(function () { d(); }, d);
};
