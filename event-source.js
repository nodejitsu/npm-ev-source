var EE = require('events').EventEmitter;
var crypto = require('crypto');
var url = require('url');
var util = require('util');
var path = require('path');
var Counter = require('stream-counter');
var mkdirp = require('mkdirp');
var rimraf = require('rimraf');
var follow = require('follow');
var SeqFile = require('seq-file');
var http = require('http-https');
var parse = require('parse-json-response');
var readmeTrim = require('npm-registry-readme-trim');

var version = require('../package.json').version;
var ua = 'npm-registry-transform/' + version + ' node/' + process.version;

module.exports = EventSource;

util.inherits(EventSource, EE);

function EventSource(options) {
  if (!(this instanceof EventSource)) { return new EventSource(options) }

  //
  // URLs and such
  //
  this.skim = options.skim || 'https://skimdb.npmjs.com/registry';

  if (!options.eventSource)) { throw new Error('Need eventSource DB to populate') }
  //
  // Setup both public and private urls (public needed for attachment creation)
  //
  var es = url.parse(options.eventSource);
  this.eventSource = es.href.replace(/\/+$/, '');
  delete es.auth;
  this.pubEventSource = url.format(es).replace(/\/+$/, '');

  this.seqFile = new SeqFile(options.seqFile || 'seqeunce.seq');

  //
  // Follow Opts
  //
  this.inactivity_ms = options.inactivity_ms || 60 * 60 * 1000;
  this.since = 0;
  this.follow = undefined;

  //
  // Count obj for things being iterated on each doc
  //
  this.count = {};

  this.tmp = options.tmp;

  //
  // Randomish dir so we donr reuse one potentially when debugging
  //
  if(!this.tmp) {
    var c = crypto.randomBytes(6).toString('hex');
    this.tmp = 'npm-registry-transform-' + process.pid + '-' + c;
  }

  this.seqFile.read(this.onSeq.bind(this));

};

//
// Callback of SeqFile function that reads from the specified file
//
EventSource.prototype.onSeq = function (err, data) {
  //
  // Errors don't matter, we just default to `this.since`
  //
  this.since = +data || this.since;
  this.start();
};

//
// Start following the skimdb database and get the changes!
//
EventSource.prototype.start = function () {
  this.emit('start');

  this.follow = follow({
    db: this.skim,
    since: this.since,
    inactivity_ms: this.inactivity_ms
  }, this.onChange.bind(this));
};

EventSource.prototype.onChange(err, change) {
  if (err) {
    return this.emit('error', err);
  }

  if(!change.id) {
    return;
  }

  this.pause();
  this.since = change.seq;

  return change.deleted
    ? this.delete(change)
    : this.getDoc(change);

};

EventSource.prototype.delete = function (change) {
  //
  // Not implemented yet
  //
};

EventSource.prototype.getDoc = function (change) {
  var q = '?revs=true&att_encoding_info=true';
  var opts = url.parse(this.skim + '/' + change.id + q);

  opts.method = 'GET';
  opts.headers = {
    'content-type': 'application/json',
    'connection': 'close'
  };

  var req = http.get(opts);
  //
  // TODO: Implement generic retries for ALL errors, this is especially
  // important when it comes to attachments because we must ENSURE we fetch
  // them
  //
  req.on('error', this.emit.bind(this, 'error'));
  req.on('response', parse(this.onGet.bind(this, change)));

};

EventSource.prototype.onGet = function (err, doc, res) {
  if (err || res.statusCode != 200) {
    return this.emit('error', err || new Error('Fetch from skim failed with ' + res.statusCode));
  }
  change.doc = doc;
  this.split(change);
};

//
// Iterate through document versions and split it into separate documents to
// insert into the database
//
EventSource.prototype.split = function (change) {
  //
  // TODO: handle diffing which versions we need to fetch instead of assuming
  // all
  //
  var doc = change.doc;
  var versions = Object.keys(doc.versions);
  var tmp = path.resolve(this.tmp, change.id + '-' + change.seq);
  //
  // TODO: Handle stars
  //
  this.count[change.id] = versions.length;
  //
  // TODO: check if it exists in database
  // First iteration we are just going to populate this
  // so we have data to write some views with
  //
  mkdirp(tmp, function (err) {
    if (err) {
      return this.emit('error', err);
    }
    versions.forEach(this.fetchAtt.bind(this, change))
  }.bind(this));

};

//
//
//
EventSource.prototype.fetchAtt = function (change, v) {
  var doc = change.doc;
  var vDoc = doc.versions[v];
  var reg = url.parse(vDoc.dist.tarball);
  //
  // May need to specify registry depending on the situation
  //
  if (this.registry) {
    var p = '/' + change.id + '/-/' + path.basename(reg.pathname)
    reg = url.parse(this.registry + p)
  }

  reg.method = 'GET';
  reg.headers = {
    'user-agent': ua,
    'connection': 'close'
  };

  var req = http.request(reg);
  req.on('error', this.emit.bind(this, 'error'));
  req.on('response', this.onAttRes.bind(this, change, v));
  req.end();

};

//
// Check the files and make sure they match what the document says while
// streaming them to the file system
//
EventSource.prototype.onAttRes = function (change, v, res) {
  var doc = change.doc;
  var vDoc = doc.versions[v];
  var att = vDoc.dist.tarball;
  var sum = vDoc.dist.shasum;
  var filename = doc.name + '-' + v + '.tgz';
  var file = path.join(this.tmp, change.id + '-' + change.seq, filename);

  if (res.statusCode != 200) {
    return; // retry here
  }

  //
  // Make them streams for some crazy pipe action
  //
  var fileStream = fs.createWriteStream(file);
  var sha = crypto.createHash('sha1');
  var goodSha = false;

  sha.on('data', function (data) {
    data = data.toString('hex');
    if (data === sum) {
      goodSha = true;
    }
  });
  if (!res.headers['content-length']) {
    var counter = new Counter();
    res.pipe(counter);
  }
  res.pipe(sha);
  res.pipe(fileStream);

  fstr.on('error', function(er) {
    err.change = change
    err.version = v
    err.path = file
    err.url = att
    this.emit('error', errState = errState || er)
  }.bind(this));
};

//
// Head request the event source database to see if we have a doc
// (may not be needed actually)
//
EventSource.prototype.headEs = function (fetch, doc, key) {
  var id = doc.versions[key]._id;
  var opts = url.parse(this.eventSource + '/' + id);
  opts.method = 'HEAD';
  opts.headers = {
    'content-type': 'application/json',
    'connection': 'close'
  };

  var req = http.request(opts);
  req.on('error', this.emit.bind(this, 'error'));
  req.on('response', this.onHeadRes.bind(this))
  req.end();
};

EventSource.prototype.onHeadRes = function () {

};

EventSource.prototype.pause = function () {
  if (this.follow) {
    this.follow.pause();
  }
};

EventSource.prototype.resume = function () {
  if (this.follow) {
    this.follow.resume();
  }
};
