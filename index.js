var util = require('util');

var _ = require('lodash');
var _s = require('underscore.string');
var async = require('async');
var request = require('request');

var BmDriverBase = require('benangmerah-driver-base');

var RDF_NS = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
var RDFS_NS = 'http://www.w3.org/2000/01/rdf-schema#';
var OWL_NS = 'http://www.w3.org/2002/07/owl#';
var XSD_NS = 'http://www.w3.org/2001/XMLSchema#';
var BM_NS = 'http://benangmerah.net/ontology/';
var PLACE_NS = 'http://benangmerah.net/place/idn/';
var BPS_NS = 'http://benangmerah.net/place/idn/bps/';
var GEO_NS = 'http://www.w3.org/2003/01/geo/wgs84_pos#';
var QB_NS = 'http://purl.org/linked-data/cube#';
var ORG_NS = 'http://www.w3.org/ns/org#';
var DCT_NS = 'http://purl.org/dc/terms/';
var SKOS_NS = 'http://www.w3.org/2004/02/skos/core#';

var MP3EI_API = 'http://mp3ei.big.go.id/webmp3ei/';
var PROJECT_CLASS = BM_NS + 'mp3ei/Project';

var projectContext = {
  '@context': {
    '@vocab': BM_NS + 'mp3ei/',
    'nama_project': RDFS_NS + 'label',
    'deskripsi': RDFS_NS + 'comment',
    'lat': GEO_NS + 'latitude',
    'lon': GEO_NS + 'longitude'
  }
};
var klUris = {
  'Kementerian Pekerjaan Umum': 'http://www.pu.go.id/',
  'Kementerian ESDM': 'http://www.esdm.go.id/',
  'Kementerian Perhubungan': 'http://www.dephub.go.id/'
};

function Mp3eiDriver() {}

util.inherits(Mp3eiDriver, BmDriverBase);

module.exports = Mp3eiDriver;

Mp3eiDriver.prototype.setOptions = function(options) {

};

Mp3eiDriver.prototype.fetch = function() {
  var self = this;

  async.series([
    self.fetchKoridor.bind(self),
    self.fetchProvinsi.bind(self)
  ], function(err) {
    if (err) {
      return self.error(err);
    }

    self.finish();
  });
};

Mp3eiDriver.prototype.fetchKoridor = function(callback) {
  var self = this;

  self.info('Fetching corridors and provinces...');

  self.provinsiPaths = [];

  request.get({ url: MP3EI_API + 'init/koridor', json: true },
    function(err, req, data) {
      if (err) {
        return callback(err);
      }

      var koridors = data.result.koridor;
      self.koridorObject = koridors;

      _.forEach(koridors, function(koridor) {
        _.forEach(koridor.provinsi, function(provinsi) {
          self.provinsiPaths.push(
            'koridor/' + koridor.url_segment +
            '/provinsi/' + provinsi.url_segment
          );
        });
      });

      callback();
    });
};

Mp3eiDriver.prototype.fetchProvinsi = function(callback) {
  var self = this;

  self.info('Fetching projects...');

  async.each(self.provinsiPaths, self.fetchProjects.bind(self), callback);
};

Mp3eiDriver.prototype.fetchProjects = function(provinsiPath, callback) {
  var self = this;

  request.get({ url: MP3EI_API + provinsiPath, json: true },
    function(err, req, data) {
      if (err) {
        return callback(err);
      }

      async.each(data.proyek, self.addProject.bind(self), callback);
    });
};

Mp3eiDriver.prototype.addProject = function(project, callback) {
  var self = this;
  delete project.reviews;

  var context = projectContext['@context'];

  var projectUri = MP3EI_API +
                   '#k/' + project.koridor_url_segment +
                   '/p/' + project.provinsi_url_segment +
                   '/j/' + project.url_segment;

  self.addTriple(projectUri, RDF_NS + 'type', PROJECT_CLASS);
  self.addTriple(
    projectUri, BM_NS + 'hasLocation', BPS_NS + project.id_provinsi);
  self.addTriple(
    projectUri, BM_NS + 'hasOwner', klUris[project.kementerian_lembaga]);

  _.forEach(project, function(value, key) {
    if (_s.endsWith(key, 'segment') || _s.startsWith(key, 'id')) {
      return;
    }

    var predicate = context['@vocab'] + key;
    if (context[key]) {
      predicate = context[key];
    }

    self.addTriple(projectUri, predicate, '"' + value + '"');
  });

  callback();
};

BmDriverBase.handleCLI();