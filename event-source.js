var EE = require('events').EventEmitter;
var crypto = require('crypto');
var url = require('url');
var util = require('util');
var path = require('path');
var fs = require('fs');
var Counter = require('stream-counter');
var mkdirp = require('mkdirp');
var rimraf = require('rimraf');
var follow = require('follow');
var SeqFile = require('seq-file');
var http = require('http-https');
var parse = require('parse-json-response');

var version = require('./package.json').version;
var ua = 'npm-ev-source/' + version + ' node/' + process.version;

var noop = function () {}

module.exports = EventSource;

util.inherits(EventSource, EE);

function EventSource(options) {
  if (!(this instanceof EventSource)) { return new EventSource(options) }

  //
  // URLs and such
  //
  this.skim = options.skim || 'https://skimdb.npmjs.com/registry';

  if (!options.eventSource) { throw new Error('Need eventSource DB to populate') }
  //
  // Setup both public and private urls (public needed for attachment creation)
  //
  var es = url.parse(options.eventSource);
  this.eventSource = es.href.replace(/\/+$/, '');
  delete es.auth;
  this.pubEventSource = url.format(es).replace(/\/+$/, '');

  this.seqFile = new SeqFile(options.seqFile || 'sequence.seq');
  this.missingLog = options.missingLog || false;

  //
  // Follow Opts
  //
  this.inactivity_ms = options.inactivity_ms || 60 * 60 * 1000;
  this.since = 0;
  this.follow = undefined;

  //
  // optional things
  //
  this.ua = options.ua || ua;
  this.tmp = options.tmp;
  this.registry = options.registry || undefined;

  //
  // Prefix for _ keys that are currently in the nested level documents.
  // This is bad practice with couchdb and it rejects them in most cases
  //
  this._prefix = 'npm:';
  //
  // Default boundary for multipart/related requests
  //
  this.boundary = 'npm-ev-source-' + crypto.randomBytes(6).toString('base64');
  //
  // Randomish dir so we donr reuse one potentially when debugging
  //
  if(!this.tmp) {
    var c = crypto.randomBytes(6).toString('hex');
    this.tmp = 'npm-ev-source-tmp-' + process.pid + '-' + c;
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

EventSource.prototype.onChange = function (err, change) {
  if (err) { return this.maybeRestart(err) }

  if(!change.id) {
    return;
  }

  this.pause();
  this.since = change.seq;

  //
  // We don't REALLY care about design but in the future we
  // do need the rewrites element. Might as well just store this separately
  // though.
  //
  if (/^_design/.test(change.id)) {
    return this.resume();
  }

  return change.deleted
    ? this.delete(change)
    : this.getDoc(change);

};

EventSource.prototype.delete = function (change) {
  //
  // Not implemented yet
  //
  this.resume();
};

EventSource.prototype.getDoc = function (change) {
  var q = '?revs=true&att_encoding_info=true';
  var opts = url.parse(this.skim + '/' + change.id + q);

  opts.method = 'GET';
  opts.agent = false;
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

EventSource.prototype.onGet = function (change, err, doc, res) {
  if (err) {
    return this.emit('error', err);
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
  // Setup count for how many iterations to expect
  //
  change.count = versions.length;
  //
  // TODO: Handle stars
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
// Fetch the attachment for the specified version that we are iterating through
//
EventSource.prototype.fetchAtt = function (change, v) {
  var doc = change.doc;
  var vDoc = doc.versions[v];
  var reg = url.parse(vDoc.dist.tarball);
  //
  // May need to specify registry depending on the situation which is the
  // direct url to whatever tarball service
  //
  if (this.registry) {
    var p = '/' + change.id + '/-/' + path.basename(reg.pathname)
    reg = url.parse(this.registry + p)
  }

  reg.method = 'GET';
  reg.agent = false;
  reg.headers = {
    'user-agent': this.ua,
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
  var filename = change.id + '-' + v + '.tgz';
  var file = path.join(this.tmp, change.id + '-' + change.seq, filename);

  if (res.statusCode != 200) {
    //
    // TODO: Retry
    //
    return this.missingLog
      ? fs.appendFile(this.missingLog, att + '\n', this.isFinished.bind(this, change))
      : this.emit('error', new Error('Unable to properly fetch attachment ' + filename));
  }

  //
  // Make them streams for some crazy pipe action
  //
  var fileStream = fs.createWriteStream(file);
  var sha = crypto.createHash('sha1');
  var shaOk = false;
  var errState = null;

  sha.on('data', function (data) {
    data = data.toString('hex');
    if (data === sum) {
      shaOk = true;
    }
  });
  if (!res.headers['content-length']) {
    var counter = new Counter();
    res.pipe(counter);
  }

  res.pipe(sha);
  res.pipe(fileStream);

  //
  // Add some extra info to the error obj if this happens and emit
  // TODO: This shouldn't really happen but we should refetch if this is the
  // case
  //
  fileStream.on('error', function (err) {
    err.change = change;
    err.version = v;
    err.path = file;
    err.url = att;
    errState = err;
    this.emit('error', err);
  }.bind(this));

  //
  // See if we to stop here because the stream errored
  //
  fileStream.on('close', function () {
    if (errState || !shaOk) {
      //
      // Hmm so this will definitely be hit maybe do the refetch here
      // for both cases if the sha is not the same AND if we failed to write to
      // the filesystem
      //
      // just gonna throw an error for now cause shit is fucked
      //
      return this.emit('error', new Error('Sha did not match or we are in a bad state ' + errState));
    }

    //
    // Remark: I guess keep the name of the attachment the same as before?
    // **note** at least we know that we can form this name from the name
    // property and the version property
    //
    var name = path.basename(filename);
    this.emit('download', name);

    //
    // Hmm should these be the same urls as before? I think so
    //
    var newAtt = this.pubEventSource + '/' + change.id +
                  '/' + change.id + '-' + v + '.tgz';

    //
    // Remark: Setup the new document that we are forming with any necessary
    // attributes from the old behemoth doc.
    //
    vDoc.dist.tarball = newAtt
    vDoc.time = doc.time && doc.time[v] || null;
    //
    // Attach the default readme to the version document so we ensure
    // each version has one at least
    //
    vDoc.readmeFilename = vDoc.readmeFilename || doc.readmeFilename;
    vDoc.readme = vDoc.readme || doc.readme;
    //
    // Remark: So these tricksy little underscore based keys can cause problems
    // for couchDB as it uses them internally. In reality we shouldnt use these
    // AT ALL but lets see with what we can get away with prefixing.
    //
    vDoc = Object.keys(vDoc).reduce(function (doc, key) {
      if (key !== '_id' && /^_/.test(key)) {
        var k = this._prefix + key;
        doc[k] = vDoc[key];
        return doc;
      }
      doc[key] = vDoc[key];
      return doc;
    }.bind(this), {});

    //
    // Ensure we get content length to append to the data structure we use to
    // send the actual request.
    //
    var cl = res.headers['content-length']
      ? +res.headers['content-length']
      : counter.bytes;

    //
    // Create the attachment here and form the final doc when we do a PUT
    // as we need the attachment values to do all the crazy boundary shit
    //
    var attachment = {
      length: cl,
      follows: true,
      // We make an assumption here but this should be safe (in theory);
      content_type: res.headers['content-type'] || 'application/octet-stream'
    };
    this.putDoc(change, vDoc, name, attachment);
  }.bind(this));
};

//
// Insert the single document with its one attachment as a single
// multipart/related request so it is much more efficient
//
EventSource.prototype.putDoc = function (change, vDoc, name, att)  {
  var id = encodeURIComponent(vDoc._id);
  var file = path.join(this.tmp, change.id + '-' + change.seq, name);

  //
  // Oh yea add attachment shit to the actual vDoc
  //
  vDoc._attachments = {};
  vDoc._attachments[name] = att;
  //
  // Start setting up request related things;
  //
  var opts = url.parse(this.eventSource + '/' + id);
  opts.method = 'PUT';
  opts.agent = false;
  opts.headers = {
    'user-agent': this.ua,
    'content-type': 'multipart/related;boundary="' + this.boundary + '"',
    'connection': 'close'
  };

  //
  // Make a doc buffer to write to the stream
  //
  var doc = new Buffer(JSON.stringify(vDoc), 'utf8');

  //
  // Ridiculous boundary strings that we need to separate the pieces of our request with.
  //
  var docBoundary = '--' + this.boundary + '\r\n' +
    'content-type: application/json\r\n' +
    'content-length: ' + doc.length + '\r\n\r\n';

  var attBoundary = '\r\n--' + this.boundary + '\r\n' +
    'content-length: ' + att.length + '\r\n' +
    'content-disposition: attachment; filename=' +
    JSON.stringify(name) + '\r\n' +
    'content-type: ' + att.content_type + '\r\n\r\n';

  var finalBoundary = '\r\n--' + this.boundary + '--';

  //
  // Add all the length values so we know what we are dealing with here
  //
  opts.headers['content-length'] = doc.length + att.length
    + docBoundary.length + attBoundary.length + finalBoundary.length;

  //
  // Create that request for the big PUT!
  //
  var req = http.request(opts);
  req.on('error', this.emit.bind(this, 'error'));
  req.on('response', parse(this.onPutRes.bind(this, change, vDoc)));

  //
  // Start the boundary -> data chain. Pipe some shit, then write the last
  // boundary
  //
  req.write(docBoundary, 'ascii');
  req.write(doc);
  req.write(attBoundary, 'ascii');

  //
  // Setup the readStream and finish it off with a pipe!
  //
  var rs = fs.createReadStream(file);
  rs.on('error', this.emit.bind(this, 'error'));

  //
  // Since we technically need to write a last boundary after the stream,
  // we listen for end and do that here
  //
  rs.on('end', function () {
    req.write(finalBoundary, 'ascii');
    req.end();
  }.bind(this));
  //
  // Don't auto end so that we can write the final boundary
  //
  rs.pipe(req, { end: false });

};

//
// Check for errors and such after we attempt our crazy multipart/related PUT
//
EventSource.prototype.onPutRes = function (change, doc, err, data, res) {
  if (err && err.statusCode != 409) {
    return this.emit('error', err);
  }
  //
  // Jut ignore because it means it was inserted and we crashed for whatever
  // reason
  // TODO: Don't let this case exist and use retries and record skip failures
  //
  if (res.statusCode == 409) {
    this.emit('skip', err.message);
  }
  else {
    this.emit('put', doc, data);
  }
  this.isFinished(change)

};

//
// Check if we are finished with our iteration since we are doing `n` PUTs
// based on the number of versions each document has
//
EventSource.prototype.isFinished = function (change) {
  if (--change.count === 0) {
    rimraf(this.tmp + '/' + change.id + '-' + change.seq, noop);
    return this.resume();
  }
};

//
// If follow errors, there is a good chance we don't need to crash
// as we have already completely disposed of the underlying request object.
// Only do this when there are no changes
//
EventSource.prototype.maybeRestart = function (err) {
  if (/made no changes/.test(err.message)) {
    this.emit('restart', err.message);
    this.follow.dead = false;
    return follow.start();
  }
  this.emit('error', err);
};

//
// Follow pause helper, just cause its nice
//
EventSource.prototype.pause = function () {
  this.follow.pause();
};
//
// Since we assume a concurrency of 1, on each resume save the seq that we just
// inserted into the database so we know where to start from if something
// terrible happens
//
EventSource.prototype.resume = function () {
  this.seqFile.save(this.since);
  this.follow.resume();
};
