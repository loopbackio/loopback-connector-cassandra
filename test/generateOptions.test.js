const rewire = require('rewire');
const cassandraLib = rewire('../lib/cassandra.js');
const generateOptions = cassandraLib.__get__('generateOptions');
var should = require('should');


describe('imported features', function () {

    it('should generate multiple contact points in options for cassandra connection', function (done) {
        const settings = {
            host: 'localhost',
            port: 9042,
            keyspace: 'test',
            contactPoints: ['localhost','localhost'],

        };
        const expectedContactPointCount = 2;
        const abc = generateOptions(settings);
        const actualContactPointCount = abc.contactPoints.length;
        (actualContactPointCount).should.be.equal(expectedContactPointCount);
        done();
    });

    it('should generate single contact point in  options for cassandra connection', function (done) {
        const settings = {
            host: 'localhost',
            port: 9042,
            keyspace: 'test'
        };
        const expectedContactPointCount = 1;
        const abc = generateOptions(settings);
        const actualContactPointCount = abc.contactPoints.length;
        (actualContactPointCount).should.be.equal(expectedContactPointCount);
        done();
    });

});