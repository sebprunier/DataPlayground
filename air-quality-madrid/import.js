const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');
const elasticsearch = require('elasticsearch');
const log = require('single-line-log').stdout;

const DATA_PATH = path.join(__dirname, 'data/csvs_per_year');
const INDEX_NAME = 'airquality-madrid';
const BULK_SIZE = 500;

const STATIONS = {
    "28079004" : { "name" : "Pza. de España", "location" : [-3.712247222222224,40.423852777777775]},
    "28079008" : { "name" : "Escuelas Aguirre", "location" : [-3.682319444444445,40.42156388888888]},
    "28079011" : { "name" : "Avda. Ramón y Cajal", "location" : [-3.6773555555555553,40.451475]},
    "28079016" : { "name" : "Arturo Soria", "location" : [-3.6392333333333333,40.44004722222222]},
    "28079017" : { "name" : "Villaverde", "location" : [-3.713322222222221,40.347138888888885]},
    "28079018" : { "name" : "Farolillo", "location" : [-3.7318527777777777,40.39478055555556]},
    "28079024" : { "name" : "Casa de Campo", "location" : [-3.7473472222222224,40.41935555555556]},
    "28079027" : { "name" : "Barajas Pueblo", "location" : [-3.580030555555555,40.47692777777778]},
    "28079035" : { "name" : "Pza. del Carmen", "location" : [-3.7031722222222223,40.41920833333333]},
    "28079036" : { "name" : "Moratalaz", "location" : [-3.6453055555555554,40.40794722222222]},
    "28079038" : { "name" : "Cuatro Caminos", "location" : [-3.7071277777777785,40.44554444444445]},
    "28079039" : { "name" : "Barrio del Pilar", "location" : [-3.7115416666666654,40.47822777777778]},
    "28079040" : { "name" : "Vallecas", "location" : [-3.6515222222222223,40.38815277777777]},
    "28079047" : { "name" : "Mendez Alvaro", "location" : [-3.686825,40.398113888888886]},
    "28079048" : { "name" : "Castellana", "location" : [-3.690366666666667,40.43989722222222]},
    "28079049" : { "name" : "Parque del Retiro", "location" : [-3.682583333333333,40.414444444444435]},
    "28079050" : { "name" : "Plaza Castilla", "location" : [-3.688769444444445,40.46557222222223]},
    "28079054" : { "name" : "Ensanche de Vallecas", "location" : [-3.612116666666666,40.372933333333336]},
    "28079055" : { "name" : "Urb. Embajada", "location" : [-3.5807472222222216,40.46253055555556]},
    "28079056" : { "name" : "Pza. Fernández Ladreda", "location" : [-3.7187277777777785,40.38496388888889]},
    "28079057" : { "name" : "Sanchinarro", "location" : [-3.6605027777777774,40.49420833333333]},
    "28079058" : { "name" : "El Pardo", "location" : [-3.774611111111111,40.51805833333333]},
    "28079059" : { "name" : "Juan Carlos I", "location" : [-3.6090722222222222,40.46525000000001]},
    "28079060" : { "name" : "Tres Olivos", "location" : [-3.6897611111111113,40.50058888888889]},
}

const esClient = new elasticsearch.Client({
    host: 'localhost:9200',
    log: 'error'
  });

let totalNumberOfDocuments = 0;

esClient.indices.delete({
    index: INDEX_NAME
}, (err, res) => {
    if (err) throw err;
    console.log(`✅ Index ${INDEX_NAME} deleted!`);

    esClient.indices.create({
        index: INDEX_NAME,
        body: {
            settings: {
                number_of_shards: 1,
                number_of_replicas: 0
            },
            mappings: {
                _doc: {
                    properties: {
                        "@timestamp": {
                            type : "date",
                            format : "yyyy-MM-dd HH:mm:ss"
                        },
                        location: {
                            type: "geo_point"
                        }
                    }
                }
            }
        }
    }, (err, res) => {
        if (err) throw err;
        console.log(`✅ Index ${INDEX_NAME} created!`)
    });
});

const insertToElastic = (documents) => {
    const bulkBody = [].concat.apply([], documents.map(document => [
        {index: {_index: INDEX_NAME, '_type': '_doc'}},
        document
    ]));
    esClient.bulk({body: bulkBody}, (err, res) => {
        if (err) throw err;
        totalNumberOfDocuments = totalNumberOfDocuments + res.items.length;
        log(`⚙️  ${totalNumberOfDocuments} documents inserted`);
    });
}

fs.readdir(DATA_PATH, function (err, files) {
    if (err) throw err;

    files.forEach(file => {
        const filePath = path.join(DATA_PATH, file);
        let documents = [];
        fs.createReadStream(filePath)
          .pipe(csv({
              separator: ',' 
          }))
          .on('data', data => {
              const stationId = data["station"];
              const station = STATIONS[stationId];
              if (station) {
                const document = {
                    "@timestamp" : data["date"],
                    "station"    : `${station.name} (${stationId})`,
                    "location"   : station.location,
                  };
                  const co = data["CO"];
                  if (co && co !== "") document["CO"] = parseFloat(co);
    
                  documents.push(document);
              }
              
              if  (documents.length >= BULK_SIZE) {
                insertToElastic(documents);
                documents = [];
              }
          })
          .on('end', () => {
              if (documents.length > 0) {
                  insertToElastic(documents);
              }
          });
    });
});
