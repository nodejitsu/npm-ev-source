#!/usr/bin/env node

var EventSource = require('../event-source');

var yargs = require('yargs')
  .usage('$0 [options]')
  .options('c', {
    description: 'Config file to encapsulate all the things if you roll like that',
    alias: 'config',
    string: true,
  })
  .options('r', {
    description: 'Registry where you specifically fetch TARBALLs from',
    alias: 'registry',
    string: true
  })
  .options('e', {
    description: 'Event source couchdb database to insert docs into',
    alias: 'event-source',
    string: true
  })
  .options('s', {
    description: 'Skimdb to listen on changes for to run this process',
    alias: 'skim',
    string: true
  })
  .options('t', {
    description: 'Temp directory to store the tarballs that are fetched',
    alias: 'tmp',
    string: true
  })
  .options('u', {
    description: 'user-agent if you want to specify your own',
    alias: 'user-agent',
    string: true
  })
  .options('f', {
    description: 'The seq-file to be use',
    alias: 'seq-file'
  })
  .options('h', {
    description: 'Displays this message',
    alias: 'h'
  })

var argv = yargs.argv;

if (argv.h) {
  return yargs.showHelp();
}

var config = argv.c
  ? require(path.resolve(argv.c))
  : { registry: argv.r, eventSource: argv.e, skim: argv.s, ua: argv.u, tmp: argv.t }

try {
  var es = new EventSource(config)
}
catch (ex) {
  console.error(ex);
  yargs.showHelp();
  process.exit(1);
}

es.on('start', function () {
  console.log('AND WE BEGIN GOOD SIR/MADAM %s pid=%d', es.ua, process.pid);
}).on('change', function (change) {
  console.log('CHANGE %d: %s', change.seq, change.id);
}).on('restart', function (message) {
  console.log('RESTART on %s', message)
}).on('put', function (doc) {
  console.log('PUT <- doc %s', doc._id);
}).on('skip', function (message) {
  console.log('SKIP %s', message);
}).on('delete', function (doc) {
  console.log('DELETE doc %s', doc._id);
}).on('error', function (err) {
  console.error('ERROR', err);
  throw err;
}).on('download', function(name) {
  console.log('DOWNLOAD -> doc %s', name);
})
