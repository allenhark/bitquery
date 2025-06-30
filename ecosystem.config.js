module.exports = {
    apps: [{
        name: 'kafka-consumer',
        script: 'dist/index.js',
        instances: 4,
        env: require('dotenv').config().parsed
    }]
}