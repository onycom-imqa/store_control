var mysql = require('mysql');

var inherits = require('util').inherits;
var EventEmitter = require('events').EventEmitter;
var _ = require('underscore');

module.exports = MySql;

//사용법
// var redis = new store(database.mysql);

//
// Master / Slave Replication 구조이다.
//

function MySql(config) {
    var clusterConfig = {
        removeNodeErrorCount: 1, // Remove the node immediately when connection fails.
        defaultSelector: 'RR',
        canRetry: true
    };

    this.poolCluster = mysql.createPoolCluster(clusterConfig);
    this.dirties = {};
    this.connections = {};
    this.config = config;

    this.connect(config);
};

inherits(MySql, EventEmitter);

MySql.prototype.connect = function(config) {
    var self = this;

    config.forEach(function(connectionGroup) {
        self.poolCluster.add('MASTER', connectionGroup.master);

        for( var i = 0; i < connectionGroup.slaves.length; i++) {
            console.log("SLAVE-Key: " , 'SLAVE'+(i+1) );
            self.poolCluster.add('SLAVE'+(i+1), connectionGroup.slaves[i]);
        }
    });
};


MySql.prototype.getConnection = function(ReplicationKey, callback) {
    var self = this;
    self.poolCluster.getConnection(ReplicationKey, callback);
};


MySql.prototype.query = function(ReplicationKey, statement, args, callback) {
    var self = this;
    if(ReplicationKey == 'MASTER'){
        self.getConnection('MASTER', function(error, connection) {
            connection.query(statement, args, function(err, result) {
                connection.release();
                callback(err, result);
            });
        });
    }else{
        self.getConnection(ReplicationKey, function(error, connection) {
            connection.query(statement, args, function(err, result) {
                connection.release();
                callback(err, result);
            });
        });
    }
};
