'use strict';

const express = require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
require('dotenv').config();

const { Kafka } = require('kafkajs');
const hbase = require('hbase');

const port = Number(process.argv[2]);

const url = new URL(process.argv[3]);
console.log("This is the REST url", url);

const hclient = hbase({
    host: url.hostname,
    path: url.pathname || "/",
    port: url.port,
    protocol: url.protocol.slice(0, -1),
    encoding: 'latin1'
});

const kafka = new Kafka({
    clientId: 'bus-routes',
    brokers: process.env.k_brokers.split(','),
    ssl: true,
    sasl: {
        mechanism: process.env.k_mechanism,
        username: process.env.k_username,
        password: process.env.k_password
    }
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "vehicle_stop_distance" });

let kafkaInitialized = false;
let pendingResolvers = [];

async function initProducer() {
    if (!producer._connected) {
        await producer.connect();
        producer._connected = true;
        console.log("Kafka producer connected");
    }
}

async function send_kafka_msg(route, stop_lat, stop_lon) {
    await initProducer();

    console.log("Requesting nearest vehicle:", route, stop_lat, stop_lon);

    await producer.send({
        topic: "ldepablo1_vehicles",
        messages: [
            {
                key: route,
                value: JSON.stringify({
                    route,
                    ts: Date.now(),
                    stop_lat,
                    stop_lon
                })
            }
        ]
    });

    console.log("Message published");
}

async function initConsumer() {
    if (kafkaInitialized) return;

    await consumer.connect();
    await consumer.subscribe({ topic: "ldepablo1_vehicle_stops", fromBeginning: false });

    consumer.run({
        eachMessage: async ({ message }) => {
            const raw = message.value.toString();

            let parsed = null;
            try {
                parsed = JSON.parse(raw);
            } catch (err) {
                console.log("Parse error in Kafka message:", err);
            }

            const resolve = pendingResolvers.shift();
            if (resolve) resolve(parsed);
        }
    });

    kafkaInitialized = true;
}

async function receive_kafka_msg() {
    await initConsumer();

    return new Promise(resolve => {
        pendingResolvers.push(resolve);

        setTimeout(() => resolve(null), 20000);
    });
}

function cells_to_stops(cells) {
    const rows = {};

    cells.forEach(cell => {
        const rk = cell.key;
        if (!rows[rk]) rows[rk] = { rk };

        const [family, qualifier] = cell.column.split(':');
        if (family !== 'info') return;

        const valStr = Buffer.from(cell['$'], 'latin1').toString();

        if (qualifier === "stop_lat" || qualifier === "stop_lon") {
            rows[rk][qualifier] = parseFloat(valStr);
        } else {
            rows[rk][qualifier] = valStr;
        }
    });

    return Object.values(rows);
}

function get_route_stops(route, callback) {
    const prefix = route + "_";

    hclient.table('route_to_stop_hb').scan(
        {
            filter: { type: "PrefixFilter", value: prefix },
            maxVersions: 1
        },
        (err, cells) => {
            if (err) return callback(err, null);
            const stops = cells_to_stops(cells);
            callback(null, stops);
        }
    );
}

function find_avg_speed(route_id, callback) {
    const hour = new Date().getHours();
    const hour_str = hour.toString().padStart(2, '0');
    const hour_key = `${route_id}_${hour_str}`;

    hclient.table('speed_aggs_hb')
        .row(hour_key)
        .get(['info:avg_speed_mph'], (err, cells) => {
            if (err) return callback(err, null);

            if (!cells || cells.length === 0) {
                console.log(`[ETA] No speed found for key ${hour_key}`);
                return callback(null, null);
            }

            const cell = cells.find(c => c.column === "info:avg_speed_mph");
            if (!cell) return callback(null, null);

            const avg = parseFloat(Buffer.from(cell['$'], 'latin1').toString());
            if (isNaN(avg)) return callback(null, null);

            console.log(`[ETA] Using speed rk=${hour_key} --> ${avg.toFixed(2)} mph`);
            return callback(null, avg);
        });
}

app.use(express.static('public'));

app.get('/stops.html', function (req, res) {
    const route = req.query['rt'];
    if (!route) return res.status(400).send("Missing route parameter");

    console.log("Looking up stops for:", route);

    get_route_stops(route, function (err, stops) {
        console.log("Stops returned:", stops);

        if (err) return res.status(500).send("Error fetching stops from HBase");

        const template = filesystem.readFileSync("stops.mustache").toString();
        const html = mustache.render(template, { route, stops });
        res.send(html);
    });
});

app.get('/closest.html', function (req, res) {
    const route = req.query['rt'];
    const stop_id = req.query['stop_id'];

    get_route_stops(route, async (err, stops) => {
        const stop = stops.find(s => s.stop_id === stop_id);
        if (!stop) return res.send("Stop not found for this route.");

        const { stop_lat, stop_lon } = stop;

        await send_kafka_msg(route, stop_lat, stop_lon);
        const nearest_v = await receive_kafka_msg();

        if (!nearest_v || nearest_v.error) {
            return res.send("No vehicles currently running on this route.");
        }

        const distance_miles = nearest_v.distance_miles;

        find_avg_speed(route, (err2, avg_speed_mph) => {
            if (err2) return res.send("Error reading speed table.");

            let eta = null;
            if (avg_speed_mph && distance_miles != null) {
                eta = (distance_miles / avg_speed_mph) * 60;
            }

            const template = filesystem.readFileSync("closest.mustache").toString();

            const html = mustache.render(template, {
                route,
                stop,
                vehicle: nearest_v,
                distance_miles: distance_miles ? distance_miles.toFixed(2) : "N/A",
                has_eta: eta !== null,
                eta_mins: eta ? eta.toFixed(1) : null,
                avg_speed_mph: avg_speed_mph ? avg_speed_mph.toFixed(1) : "N/A"
            });

            res.send(html);
        });
    });
});

app.listen(port);