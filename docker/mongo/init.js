// MongoDB initialisation for UniversalConnector
// Runs once when the container is first created (docker-entrypoint-initdb.d).
//
// Creates the assetsdb database, seed collections, and indexes.
// The connector uses polling mode against these collections.

const dbName = process.env.MONGO_INITDB_DATABASE || 'assetsdb';
const db = db.getSiblingDB(dbName);

// ── Collections + indexes ─────────────────────────────────────────────────────

// assets
db.createCollection('assets');
db.assets.createIndex({ updatedAt: 1 }, { background: true });
db.assets.createIndex({ assetType: 1 }, { background: true });

// locations
db.createCollection('locations');
db.locations.createIndex({ updatedAt: 1 }, { background: true });

// ── Seed data ─────────────────────────────────────────────────────────────────
const now = new Date();

if (db.assets.countDocuments() === 0) {
    db.assets.insertMany([
        {
            _id:       new ObjectId(),
            name:      'Pump-A01',
            assetType: 'pump',
            status:    'operational',
            locationId: null,
            createdAt: now,
            updatedAt: now
        },
        {
            _id:       new ObjectId(),
            name:      'Valve-B02',
            assetType: 'valve',
            status:    'maintenance',
            locationId: null,
            createdAt: now,
            updatedAt: now
        }
    ]);
    print('Seed: inserted 2 assets');
}

if (db.locations.countDocuments() === 0) {
    db.locations.insertMany([
        {
            _id:       new ObjectId(),
            name:      'Plant-1 / Zone-A',
            latitude:  51.5074,
            longitude: -0.1278,
            createdAt: now,
            updatedAt: now
        }
    ]);
    print('Seed: inserted 1 location');
}

print('MongoDB init complete for database: ' + dbName);
