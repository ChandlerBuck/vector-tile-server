var Hapi = require('hapi'),
server = new Hapi.Server(),
SphericalMercator = require('sphericalmercator'),
sm = new SphericalMercator({
    size: 256
}),
pg = require('pg'),
squel = require('squel').useFlavour('postgres'),
fs = require('fs'),
zlib = require('zlib'),
vtpbf = require('vt-pbf'),
geojsonVt = require('geojson-vt'),
inert = require('inert'); //serves static files

var Caching = require('caching');
var cache = new Caching('redis'); /* use 'memory' or 'redis' */

server.connection({
    host: 'localhost',
    port: 3000,
    routes: {
        cors: true
    },
});

function generateSQL(bbox) {
    var sql= squel.select()
    .field('row_to_json(fc)')
    .from(
        squel.select()
        .field("'FeatureCollection' As type")
        .field("array_to_json(array_agg(f)) As features")
        .from(
            squel.select()
            .field("'Feature' As type")
            .field(`ST_AsGeoJSON(ST_transform(ST_Simplify(lg.wkb_geometry, 400), 4326), 6)::json As geometry`)
            .field(`row_to_json((SELECT l FROM (SELECT layername) As l)) As properties`)
            .from(`guc_all_utilities As lg`) 
            .where(`lg.wkb_geometry && ST_Transform(ST_MakeEnvelope(${bbox.join(',')} , 4326), find_srid('', 'guc_all_utilities', 'wkb_geometry'))`)
              // .limit(50)
              , 'f')
        , 'fc');
      return sql.toString();
  }
// Tile canon
server.route({
    method: 'GET',
    path: '/{table}/{z}/{x}/{y}.pbf',
    handler: function(request, reply) {
        zoom = request.params.z;
        var sql = generateSQL(sm.bbox(request.params.x, request.params.y, request.params.z));              
        //set the redis key equal to the xyz params combined
        cacheId = request.params.x + request.params.y + request.params.z;  
            console.time("load time: ");
            cache(cacheId, 100000 * 60 /*ttl in ms*/, function(passalong) {
              console.log("Looked in cache couldnt find tile going to database");
              // database connection
              pg.connect("postgres://appdev:401@greene@gispub:5432/guc_utilities_unproj_onefile", function(err, client, done) {
                if (err) {
                    passalong('Error fetching client from pool.', null);          
                } else {
                    // extract json
                    client.query(sql, function(err, result) {
                        done(); // call done to release the connection back to the pool
                        if (err) {
                            passalong('SQL Error: ' + err + '\n', null);
                        } else {
                            geoJSON = result.rows[0].row_to_json;
                            // fetchig tile index vt
                            var tileIndex = geojsonVt(geoJSON);
                            var tile = tileIndex.getTile(parseInt(request.params.z, 10), parseInt(request.params.x, 10), parseInt(request.params.y));
                            passalong(null, tile);
                            }
                        });
                    }
                });
            }, function(err, tile) {
                console.log("return vectorjson");
                if(err){
                    console.log("this is the error " + err);
                }
                console.log("table " + request.params.table);
                var buff = vtpbf.fromGeojsonVt({
                    [request.params.table]: tile
                });
                zlib.gzip(buff, function(err, pbf) {
                    reply(pbf)
                    .header('Content-Type', 'application/x-protobuf')
                    .header('Content-Encoding', 'gzip') 
                });
                if (cacheId){
                    console.timeEnd("load time: ");
                };
        });      
    }
});
//Get style.json
server.route({
    method: 'GET',
    path: '/style',
    handler: function(request, reply){
        reply.file('public/gucdata.json');
    }
});

server.route({
    method: 'GET',
    path: '/mapbox/{ttl?}',
    handler: function(request, reply){
        reply.file('public/mapbox.html');
    }
});
// Start Server
server.register(require('inert'), (err) => {
    if (err) {
        throw err;
    }
    server.start((err) => {
        if (err) {
            throw err;
        }
        console.log('Server running at:', server.info.uri);
    });
});