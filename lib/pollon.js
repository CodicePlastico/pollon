var EventEmitter = require('events').EventEmitter

class Pollon extends EventEmitter {

  constructor() {
    super()
    this.getBatchData = () => { }
    this.setSingleDataAsProcessed = () => { }
    this.handleSingleData = () => { }
    this.defaultSleepTimeMs = 5000
    this.stopped = true
    this._handleBatchDataBound = this._handleBatchData.bind(this)
    this._processSingleDataBound = this._processSingleData.bind(this)
    this._pollBound = this._poll.bind(this)
  }

  start(configuration) {
    if (!this.stopped) {
      return
    }
    this.stopped = false
    this.getBatchData = configuration.source.getBatchData
    this.setSingleDataAsProcessed = configuration.source.setSingleDataAsProcessed
    this.handleSingleData = configuration.handleSingleData
    this.sleepTimeMs = configuration.sleepTimeMs || this.defaultSleepTimeMs
    this._poll()
  }

  stop() {
    this.stopped = true
  }

  _poll() {
    const Pollon = this

    if (Pollon.stopped) {
      Pollon.emit('stopped')
      return
    }

    Pollon
      .getBatchData()
      .catch(err => Pollon.emit('error', new Error(`Get batch data failed: ${err.message}`)))
      .then(Pollon._handleBatchDataBound)
      .catch(err => Pollon.emit('error', new Error(`Generic error polling data: ${err.message}`)))
  }

  _handleBatchData(batch) {
    const Pollon = this

    if (batch && batch.data && batch.data.length > 0) {
      return Promise
        .all(batch.data.map(singleData => Pollon._processSingleDataBound(singleData)))
        .catch(err => Pollon.emit('error', new Error(`Generic error handling batch data: ${err.message}`)))
        .then(Pollon._pollBound)
    }

    Pollon.emit('empty')
    setTimeout(Pollon._pollBound, Pollon.sleepTimeMs)
  }

  _processSingleData(data) {
    const Pollon = this
    this.emit('processing_single_data', data)

    return Pollon
      .handleSingleData(data)
      .then(() => Pollon.setSingleDataAsProcessed(data))
      .catch(err => Pollon.emit('error', new Error(`'Unexpected data handler failure: ${err.message}`)))
  }
}

module.exports = Pollon