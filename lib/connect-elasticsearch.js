var elasticsearch = require('elasticsearch'),
    util = require('util'),
    debug = require('debug')('connect:store:elasticsearch'),
    _ttl = '1h';


module.exports = function(connect) {
  var Store = connect.session.Store;

  function ElasticsearchStore(options) {
    var self = this;

    this.options = options || {};
    Store.call(this, options);

    // override es default index/type
    this.es = {
      index: this.options.index  || 'connect',
      type: this.options.type || 'session'
    }

    // override ttl
    if( this.options.ttl ) {
      _ttl = this.options.ttl;
    }

    this.client = new elasticsearch.Client({
       host: this.options.hosts
    });

    this.client.indices.create({
      index: this.es.index,
      type: this.es.type,
      body: {
        "settings": {
          "index": {
            "number_of_shards": this.options.number_of_shards || 1,
            "number_of_replicas": this.options.number_of_replicas || 1
          }
        }
      }
    }, function(){
      debug( 'Index [%s] and type [%s] created.', self.es.index, self.es.type );
      
      self.client.indices.putMapping({
        index: self.es.index,
        type: self.es.type,
        body: {
          "session" : {
            "_ttl" : { "enabled" : true }
          }
        }
      }, function(){ });

    });
  }

  util.inherits(ElasticsearchStore, connect.session.Store);

  ElasticsearchStore.prototype.pSid = function(sid) {
    return ( this.options.prefix || '' ) + sid;
  }

  ElasticsearchStore.prototype.get = function(sid, cb) {
    this.client.get({
      index: this.es.index,
      type: this.es.type,
      id: this.pSid(sid)
    }, function (e, r) {
      if ( typeof r == 'undefined' ) cb();
      else cb(null, r._source);
    })
  }

  ElasticsearchStore.prototype.set = function(sid, sess, cb) {

    var maxAge = sess.cookie.maxAge,
      ttl = 'number' == typeof maxAge ? (maxAge / 1000 | 0) + "s" : _ttl,
      self = this;

    this.client.index({
      index: this.es.index,
      type: this.es.type,
      id: this.pSid(sid),
      ttl: ttl,
      body: sess
    }, function (e, r) {
      debug( 'Recorded session in [%s] and type [%s] with [%s] id using ttl [%s]', self.es.index, self.es.type, self.pSid(sid), ttl );
      cb(e);
    })

  }

  ElasticsearchStore.prototype.destroy = function(sid, cb) {
    this.client.delete({
      index: this.es.index,
      type: this.es.type,
      id: this.pSid(sid)
    }, function (e, r) {
      cb(e)
    });
  }

  return ElasticsearchStore;
}
