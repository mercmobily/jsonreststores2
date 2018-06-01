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
    this.connection.queryP = promisify(this.connection.query)
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
  }

  _selectFields (prefix) {
    var l = []

    // Always return isProperty
    l.push(`${prefix}${this.idProperty}`)

    // Return all fields from the schema that are not marked as "silent"
    for (var k in this.schema.structure) {
      if (!this.schema.structure[k].silent) l.push(`${prefix}${k}`)
    }

    // Link everything up, and that's it!
    return l.join(',')
  }

  // If a positionField is set, then delete body.beforeId -- before saving it
  // in this.data, so that it can be used for positioning
  async beforeValidate (request, method) {
    if (this.positionField) {
      request.beforeId = request.body[this.beforeIdField]
      delete request.body[this.beforeIdField]
    }

    return super.beforeValidate(request, method)
  }
  // Input: request.params
  // Output: an object
  async implementFetch (request) {
    this._checkVars()

    var fields = this._selectFields(`${this.table}.`)
    return (await this.connection.queryP(`SELECT ${fields} FROM ${this.table} WHERE id = ?`, request.params.id))[0]
  }

  // Make sure the positionField is updated depending on beforeID passed:
  // undefined    => leave it where it was (if it had a position) or place it last (if it didn't have a position)
  // null         => place it last
  // number       => valid record   => place it before that record, "making space"
  //              => INvalid record => place it last
  async _calculatePosition (request) {
    // No position field: exit right away
    if (typeof this.positionField === 'undefined') return

    var last = async () => {
      request.body[this.positionField] = (await this.connection.queryP(`SELECT max(${this.positionField}) as maxPosition FROM ${this.table} WHERE ${wherePositionFilter}`, positionQueryArgs))[0].maxPosition + 1
    }

    var positionQueryArgs = []
    var wherePositionFilter
    if (this.positionFilter.length === 0) wherePositionFilter = '1 = 1'
    else {
      let source = request.doc || request.body
      let r = []
      for (let k of this.positionFilter) {
        if (typeof source[k] === 'undefined') throw new Error(`Field must have a value as it's a positional one: ${k}`)
        r.push(`${k} = ?`)
        positionQueryArgs.push(source[k])
      }
      wherePositionFilter = ' ' + r.join('AND') + ' '
    }

    // undefined    => leave it where it was (if it had a position) or place it last (if it didn't have a position)
    if (typeof request.beforeId === 'undefined') {
      if (request.doc && typeof request.doc[this.positionField] !== 'undefined') {
        request.body[this.positionField] = request.doc[this.positionField]
      } else {
        await last()
      }

    // null         => place it last
    } else if (request.beforeId === null) {
      await last()

    // number       => valid record   => place it before that record, overwriting previous positio
    //                 INvalid record => place it last
    } else {
      var beforeIdItem = (await this.connection.queryP(`SELECT id,${this.positionField} FROM ${this.table} WHERE id = ? AND ${wherePositionFilter}`, [ request.beforeId, ...positionQueryArgs ]))[0]

      // number       => valid record   => place it before that record, "making space"
      if (beforeIdItem) {
        await this.connection.queryP(`UPDATE ${this.table} SET ${this.positionField} = ${this.positionField} + 1 WHERE ${this.positionField} >= ?  AND ${wherePositionFilter} ORDER BY position DESC`, [ beforeIdItem[this.positionField] || 0, ...positionQueryArgs ])
        request.body[this.positionField] = beforeIdItem[this.positionField]
      //              => INvalid record => place it last
      } else {
        await last()
      }
    }
  }

  // Input: request.body, request.options.[placement,placementAfter]
  // Output: an object (saved record)
  async implementInsert (request) {
    this._checkVars()

    await this._calculatePosition(request)

    // var fields = this._selectFields(`${this.table}.`)
    let insertResults = await this.connection.queryP(`INSERT INTO ${this.table} SET ?`, request.body)
    var bogusRequest = { session: request.session, params: { id: insertResults.insertId } }
    return this.implementFetch(bogusRequest)
  }

  // Input:
  // - request.params (query)
  // - request.body (data)
  // - request.options.field (field name if it's a one-field update)
  // - request.options.[placement,placementAfter] (for record placement)
  // Output: an object (updated record)
  async implementUpdate (request) {
    this._checkVars()

    await this._calculatePosition(request)

    // var fields = this._selectFields(`${this.table}.`)
    await this.connection.queryP(`UPDATE ${this.table} SET ? WHERE id = ?`, [request.body, request.params.id])

    var bogusRequest = { session: request.session, params: { id: request.params.id } }
    return this.implementFetch(bogusRequest)
    // return (await this.connection.queryP(`SELECT ${fields} FROM ${this.table} WHERE id = ?`, request.params.id))[0]
  }

  // Input: request.params
  // Output: an object (deleted record)
  async implementDelete (request) {
    this._checkVars()

    var fields = this._selectFields(`${this.table}.`)

    let record = (await this.connection.queryP(`SELECT ${fields} FROM ${this.table} WHERE id = ?`, request.params.id))[0]
    await this.connection.queryP(`DELETE FROM ${this.table} WHERE id = ?`, [request.params.id])
    return record
  }

  defaultConditions (request, args, whereStr, prefix = '') {
    var ch = request.options.conditionsHash
    for (let k in ch) {
      // Add fields that are in the searchSchema
      if (this.searchSchema.structure[k] && this.schema.structure[k] && String(ch[k]) !== '') {
        args.push(ch[k])
        whereStr = whereStr + ` AND ${prefix}${k} = ?`
      }
    }
    return { args, whereStr }
  }

  makeSortString (sort) {
    var sortStr = ''
    if (Object.keys(sort).length) {
      let l = []
      sortStr = ' ORDER BY '
      for (let k in sort) {
        l.push(k + ' ' + (Number(sort[k]) === 1 ? 'DESC' : 'ASC'))
      }
      sortStr = sortStr + l.join(',')
    }
    return sortStr
  }

  // Input: request.params, request.options.[conditionsHash,ranges.[skip,limit],sort]
  // Output: { dataArray, total, grandTotal }
  async implementQuery (request) {
    this._checkVars()

    let args = []
    var whereStr = ' 1=1'

    // Make up default conditions
    ;({ args, whereStr } = this.defaultConditions(request, args, whereStr))

    // Add ranges
    args.push(request.options.ranges.skip)
    args.push(request.options.ranges.limit)

    // Set up sort
    var sortStr = this.makeSortString(request.options.sort)

    // Make up list of fields
    var fields = this._selectFields(`${this.table}.`)

    var result = await this.connection.queryP(`SELECT ${fields} FROM ${this.table} WHERE ${whereStr} ${sortStr} LIMIT ?,?`, args)
    var grandTotal = (await this.connection.queryP(`SELECT COUNT (*) as grandTotal FROM ${this.table} WHERE ${whereStr}`, args))[0].grandTotal

    return { data: result, grandTotal: grandTotal }
  }
}

exports = module.exports = MySqlStoreMixin
