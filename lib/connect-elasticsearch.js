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
      index: this.es && this.es.index ? this.es.index : 'connect',
      type: this.es && this.es.session ? this.es.session : 'session'
    }

    // override ttl
    if( this.ttl ) {
      _ttl = this.ttl;
    }

    this.client = new elasticsearch.Client({
       host: this.options.hosts
    });

    this.client.indices.create({
      index: this.es.index,
      type: this.es.type,
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
    return this.options.prefix + sid;
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
      ttl = 'number' == typeof maxAge ? (maxAge / 1000 | 0) + "s" : _ttl;

    debug( "Using ttl [%s].", ttl );
    this.client.index({
      index: this.es.index,
      type: this.es.type,
      id: this.pSid(sid),
      ttl: ttl,
      body: sess
    }, function (e, r) {
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
