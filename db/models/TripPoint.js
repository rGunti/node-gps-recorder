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

const Sequelize = require('sequelize');
const db = require('../dbpool');
const Trip = require('./Trip');

const TripPoint = db.define('trip_points', {
    pointID: {
        field: 'id',
        type: Sequelize.DataTypes.BIGINT,
        primaryKey: true,
        autoIncrement: true
    },
    tripID: {
        field: 'trip_id',
        type: Sequelize.DataTypes.INTEGER,
        references: {
            model: Trip,
            key: 'id'
        }
    },
    latitude:          { field: 'lat',         type: Sequelize.DataTypes.DECIMAL(13, 10),  allowNull: true },
    longitude:         { field: 'lon',         type: Sequelize.DataTypes.DECIMAL(13, 10),  allowNull: true },
    altitude:          { field: 'alt',         type: Sequelize.DataTypes.SMALLINT,         allowNull: true },
    fixMode:           { field: 'fix',         type: Sequelize.DataTypes.TINYINT.UNSIGNED, allowNull: true },
    gpsSpeed:          { field: 'gps_spd',     type: Sequelize.DataTypes.DECIMAL(6, 3),    allowNull: true },
    accuracyLat:       { field: 'acc_lat',     type: Sequelize.DataTypes.DECIMAL(6, 3),    allowNull: true },
    accuracyLon:       { field: 'acc_lon',     type: Sequelize.DataTypes.DECIMAL(6, 3),    allowNull: true },
    accuracyAlt:       { field: 'acc_alt',     type: Sequelize.DataTypes.DECIMAL(6, 3),    allowNull: true },
    accuracySpd:       { field: 'acc_spd',     type: Sequelize.DataTypes.DECIMAL(6, 3),    allowNull: true },
    vehicleSpeed:      { field: 'vehicle_spd', type: Sequelize.DataTypes.SMALLINT,         allowNull: true },
    vehicleIntakeTemp: { field: 'vehicle_tmp', type: Sequelize.DataTypes.SMALLINT,         allowNull: true },
    vehicleIntakeMAP:  { field: 'vehicle_map', type: Sequelize.DataTypes.TINYINT.UNSIGNED, allowNull: true },
    vehicleRPM:        { field: 'vehicle_rpm', type: Sequelize.DataTypes.DECIMAL(7, 2),    allowNull: true },
    createdAt:         { field: 'created_at',  type: Sequelize.DataTypes.TIME },
    updatedAt:         { field: 'updated_at',  type: Sequelize.DataTypes.TIME }
});
module.exports = TripPoint;

