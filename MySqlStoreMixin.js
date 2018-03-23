/*
Copyright (C) 2016 Tony Mobily

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
var promisify = require('util').promisify

var MySqlStoreMixin = (superclass) => class extends superclass {

  constructor () {
    super()
    this.connection = this.constructor.connection
    this.table = this.constructor.table
  }

  static get connection () {
    return null
  }

  static get table () {
    return null
  }

  _checkVars () {
    if (!this.connection) throw new Error('The static property "connection" must be set')
    if (!this.table) throw new Error('The static property "table" must be set')
    this.connection.queryP = promisify(this.connection.query)
  }

  // Input: request.params
  // Output: an object
  async implementFetch (request) {
    this._checkVars()

    return (await this.connection.queryP(`SELECT * FROM ${this.table} WHERE id = ?`, request.params.id))[0]
  }

  // Input: request.body, request.options.[placement,placementAfter]
  // Output: an object (saved record)
  async implementInsert (request) {
    this._checkVars()

    let insertResults = await this.connection.queryP(`INSERT INTO ${this.table} SET ?`, request.body)
    let selectResults = await this.connection.queryP(`SELECT * FROM ${this.table} WHERE id = ?`, insertResults.insertId)
    return selectResults[0]
  }

  // Input:
  // - request.params (query)
  // - request.body (data)
  // - request.options.field (field name if it's a one-field update)
  // - request.options.[placement,placementAfter] (for record placement)
  // Output: an object (updated record)
  async implementUpdate (request) {
    this._checkVars()

    await this.connection.queryP(`UPDATE ${this.table} SET ? WHERE id = ?`, [request.body, request.params.id])
    return (await this.connection.queryP(`SELECT * FROM ${this.table} WHERE id = ?`, request.params.id))[0]
  }

  // Input: request.params
  // Output: an object (deleted record)
  async implementDelete (request) {
    this._checkVars()

    let record = (await this.connection.queryP(`SELECT * FROM ${this.table} WHERE id = ?`, request.params.id))[0]
    await this.connection.queryP(`DELETE FROM ${this.table} WHERE id = ?`, [request.params.id])
    return record
  }

  // Input: request.params, request.options.[conditionsHash,ranges.[skip,limit],sort]
  // Output: { dataArray, total, grandTotal }
  async implementQuery (request) {
    this._checkVars()

    // let ch = request.options.conditionsHash
    let ranges = request.options.ranges
    let args = []

    // Add query conditions
    var whereStr = ' 1=1'

    /*
    if (ch.prop1) {
      whereStr = whereStr + ' AND name LIKE ?'
      args.push('%' + ch.prop1 + '%')
    }
    if (ch.prop2) {
      whereStr = whereStr + ' AND name = ?'
      args.push(ch.prop2)
    }
    */

    // Add ranges
    args.push(ranges.skip)
    args.push(ranges.limit)

    var result = await this.connection.queryP(`SELECT * FROM ${this.table} WHERE ${whereStr} LIMIT ?,?`, args)
    var grandTotal = (await this.connection.queryP(`SELECT COUNT (*) as grandTotal FROM ${this.table} WHERE ${whereStr}`, args))[0].grandTotal

    return { data: result, grandTotal: grandTotal }
  }
}

exports = module.exports = MySqlStoreMixin
