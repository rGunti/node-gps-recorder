/*
 * Copyright 2017 Raphael Guntersweiler
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

const logDebug = require('debug')('GPSRecorder:DEBUG');
const logInfo = require('debug')('GPSRecorder:INFO');
const logWarn = require('debug')('GPSRecorder:WARN');
const logError = require('debug')('GPSRecorder:ERROR');
const logFatal = require('debug')('GPSRecorder:FATAL');

const config = require('config');
const async = require('async');
const redis = require('redis');

const gpsDistance = require('gps-distance');

const TripPoint = require('./db/models/TripPoint');
const Trip = require('./db/models/Trip');

const REDIS_KEYS = {
    gpsAlive: config.get('redis.keys.gpsAlive'),
    obdAlive: config.get('redis.keys.obdAlive'),
    latitude: config.get('redis.keys.latitude'),
    longitude: config.get('redis.keys.longitude'),
    altitude: config.get('redis.keys.altitude'),
    fixMode: config.get('redis.keys.fixMode'),
    gpsTime: config.get('redis.keys.gpsTime'),
    gpsSpeed: config.get('redis.keys.gpsSpeed'),
    gpsSpeedKMH: config.get('redis.keys.gpsSpeedKMH'),
    gpsAccuracyLon: config.get('redis.keys.gpsAccuracyLon'),
    gpsAccuracyLat: config.get('redis.keys.gpsAccuracyLat'),
    gpsAccuracyHeight: config.get('redis.keys.gpsAccuracyHeight'),
    gpsAccuracySpeed: config.get('redis.keys.gpsAccuracySpeed'),
    obdSpeedKMH: config.get('redis.keys.obdSpeedKMH'),
    engineRpm: config.get('redis.keys.engineRpm'),
    intakeTemp: config.get('redis.keys.intakeTemp'),
    intakeMAP: config.get('redis.keys.intakeMAP'),
    recordTripIDA: config.get('redis.keys.recordTripIDA'),
    recordTripIDB: config.get('redis.keys.recordTripIDB')
};
const PERS_REDIS_KEYS = {
    odo: config.get('persistentRedis.keys.odo'),
    tripA: config.get('persistentRedis.keys.tripA'),
    tripB: config.get('persistentRedis.keys.tripB')
};

const getCurrentData = (rClient, callback) => {
    let o = {};
    async.forEachOf(REDIS_KEYS, (value, key, callback) => {
        logDebug(`Getting ${key}`);
        rClient.get(value, (err, reply) => {
            o[key] = reply;
            if (err) logWarn(`Error while getting ${key}`, err);
            callback(err);
        });
    }, (err) => {
        if (err) {
            logError('Failed to get data because of an error:', err);
        }
        callback(err, o);
    });
};

const transformIntoTripPoint = (tripID, o) => {
    let out = {
        tripID:            tripID,
        latitude:          o.latitude,
        longitude:         o.longitude,
        altitude:          o.altitude,
        fixMode:           o.fixMode,
        gpsSpeed:          o.gpsSpeed,
        accuracyLat:       o.gpsAccuracyLat,
        accuracyLon:       o.gpsAccuracyLon,
        accuracyAlt:       o.gpsAccuracyHeight,
        accuracySpd:       o.gpsAccuracySpeed,
        vehicleSpeed:      o.obdSpeedKMH,
        vehicleIntakeTemp: o.intakeTemp,
        vehicleIntakeMAP:  o.intakeMAP,
        vehicleRPM:        o.engineRpm
    };
    return replaceNaN(out);
};

const pointNeedsUpdate = (prev, current) => {
    // (Obvious) Null Checks
    if (prev == null && current == null) return false;
    if (prev == null && current != null &&
        current.latitude != 0 && current.longitude != 0) return true;

    // LAT & LON 0 will be ignored
    if (current.latitude == 0 && current.longitude == 0) return false;

    // When we know that the vehicle is moving and something has changed, force the update
    if (prev.vehicleSpeed !== current.vehicleSpeed) return true;

    // When the position is the same, we don't need to update
    if (prev.latitude === current.latitude &&
        prev.longitude === current.longitude) return false;

    // Check the distance between the two positions. If smaller than configured (default: 10m)
    // don't update
    if (gpsDistance(
            parseFloat(prev.latitude), parseFloat(prev.longitude),
            parseFloat(current.latitude), parseFloat(current.longitude)) * 1000 < config.get('minTravelDistanceM'))
        return false;

    // TODO: Maybe more checks?

    return true;
};

const replaceNaN = (o) => {
    for (let k in o) {
        if (o[k] === 'nan') { o[k] = null; }
    }
    return o;
};

const updateOdoMeters = (rClient, prev, current) => {
    if (!prev) return;
    let kmDistance = gpsDistance(parseFloat(prev.latitude), parseFloat(prev.longitude),
        parseFloat(current.latitude), parseFloat(current.longitude));
    let mDistance = kmDistance * 1000;
    rClient.incrbyfloat(PERS_REDIS_KEYS.odo, mDistance);
    rClient.incrbyfloat(PERS_REDIS_KEYS.tripA, mDistance);
    rClient.incrbyfloat(PERS_REDIS_KEYS.tripB, mDistance);
};

let recordingTripA = null;
let lastRecordedDataA = null;

const run = () => {
    getCurrentData(rClient, (err, o) => {
        if (err) {
            logError('Error while getting current data from REDIS:', err);
            return;
        } else if (o.gpsAlive) {
            if (o.recordTripIDA) {
                if (recordingTripA && o.recordTripIDA != recordingTripA.tripID) recordingTripA = null;
                let point = transformIntoTripPoint((recordingTripA) ? recordingTripA.tripID : null, o);
                if (!lastRecordedDataA || pointNeedsUpdate(lastRecordedDataA, point)) {
                    if (!recordingTripA) {
                        Trip.findOrCreate({
                            where: { id: o.recordTripIDA },
                            defaults: { id: o.recordTripIDA }
                        }).then(response => {
                            let trip = response[0];
                            recordingTripA = trip;
                            rClient.set(REDIS_KEYS.recordTripIDA, recordingTripA.tripID);
                            prClient.set(REDIS_KEYS.recordTripIDA, recordingTripA.tripID);
                            point.tripID = recordingTripA.tripID;
                            TripPoint
                                .create(point)
                                .then(p => {
                                    updateOdoMeters(prClient, lastRecordedDataA, p);
                                    lastRecordedDataA = p;
                                    setTimeout(run, config.get('interval'));
                                })
                                .catch(err => {
                                    logError(`Failed to create Point`, err);
                                    setTimeout(run, config.get('interval'));
                                });
                        }).catch(err => {
                            logError(`Failed to create Trip ${o.recordTripIDA}: `, err);
                            setTimeout(run, config.get('interval'));
                        });
                    } else {
                        TripPoint
                            .create(point)
                            .then(p => {
                                updateOdoMeters(prClient, lastRecordedDataA, p);
                                lastRecordedDataA = p;
                                setTimeout(run, config.get('interval'));
                            })
                            .catch(err => {
                                logError(`Failed to create Point`, err);
                                setTimeout(run, config.get('interval'));
                            });
                    }
                } else {
                    setTimeout(run, config.get('interval'));
                }
            } else {
                setTimeout(run, config.get('interval'));
            }
        } else {
            setTimeout(run, config.get('interval'));
        }
    });
};

const rConfig = config.get('redis');
const prConfig = config.get('persistentRedis');

const rClient = redis.createClient(
    rConfig.port,
    rConfig.host
);
const prClient = redis.createClient(
    prConfig.port,
    prConfig.host
);

rClient.on('error', (err) => { logError('Error in Redis Connection', err) });
prClient.on('error', (err) => { logError('Error in Persistent Redis Connection', err) });

rClient.on('connect', () => {
    logInfo('Connected to Redis Host');
    run();
});
prClient.on('connect', () => { logInfo('Connected to Persistent Redis Host') });

