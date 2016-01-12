'use strict';

var resolve  = require('path').resolve
  , rmdir    = require('fs2/rmdir')
  , getTests = require('dbjs-persistence/test/_common')

  , dbPath = resolve(__dirname, 'test-db')
  , dbCopyPath = resolve(__dirname, 'test-db-copy')
  , tests = getTests({ path: dbPath }, { path: dbCopyPath });

module.exports = function (t, a, d) {
	return tests.apply(null, arguments)(function () {
		return rmdir(dbCopyPath, { recursive: true, force: true });
	}).done(function () { d(); }, d);
};
