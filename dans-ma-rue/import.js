const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');
const elasticsearch = require('elasticsearch');
const log = require('single-line-log').stdout;

const DATA_PATH = path.join(__dirname, 'data');
const INDEX_NAME = 'dansmarue';
const BULK_SIZE = 500;

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
              separator: ';' 
          }))
          .on('data', data => {
              /*
               * TYPE                => Objets abandonnés
               * SOUSTYPE            => Objets entrant dans plusieurs catégories
               * ADRESSE             => 45 Rue Damrémont, 75018 PARIS
               * CODE_POSTAL         => 75018
               * VILLE               => Paris 18
               * ARRONDISSEMENT      => 18.0
               * DATEDECL            => 2018-07-22T04:00:00+02:00
               * ANNEE DECLARATION   => 2018
               * MOIS DECLARATION    => 7
               * NUMERO              => 18124.0
               * PREFIXE             => G
               * INTERVENANT         => Ramen en tant que prestataire de DansMaRue
               * CONSEIL DE QUARTIER => GRANDES CARRIERES - CLICHY
               * OBJECTID            => 1247
               * geo_shape           => "{""type"": ""Point"", ""coordinates"": [2.333974603227083, 48.89019799615594]}"
               * geo_point_2d        => 48.8901979962, 2.33397460323
               */

              const document = {
                "@timestamp"          : data["DATEDECL"],
                "year"                : data["ANNEE DECLARATION"],
                "month"               : data["MOIS DECLARATION"],
                "number"              : parseFloat(data["NUMERO"]).toFixed(),
                "type"                : data["TYPE"],
                "subtype"             : data["SOUSTYPE"],
                "address"             : data["ADRESSE"],
                "zipCode"             : data["CODE_POSTAL"],
                "district"            : parseFloat(data["ARRONDISSEMENT"]).toFixed(),
                "city"                : data["VILLE"],
                "prefix"              : data["PREFIXE"],
                "objectId"            : parseInt(data["OBJECTID"], 10),
                "provider"            : data["INTERVENANT"],
                "neighborhoodCouncil" : data["CONSEIL DE QUARTIER"],
                "location"            : data["geo_point_2d"].split(", ").map(s => parseFloat(s))
              }
              documents.push(document);
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
