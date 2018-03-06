/*
Copyright (C) 2016 Tony Mobily

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

/*
NOTE. When creating a store, you can take the following shortcuts:
  * Don't specify `paramIds`. If not specified, it will be worked out from publicURL
  * Don't specify `idProperty`. If idProperty not specified, it will be assumed last element of paramIds
  * Don't specify `paramIds` in schema. They will be added to the schema as `{type: 'id' }` automatically
  * Don't specify `searchSchema`. It will be worked out taking all schema element marked as `searchable: true` (except paramIds)
*/

var e = require('allhttperrors')
var path = require('path')
// var DO = require('deepobject')
// var { asyncForEach, asyncMap } = require('p-iterator')

var registry = {}

var Store = class {
  // ****************************************************
  // *** ATTRIBUTES THAT CAN TO BE DEFINED IN PROTOTYPE
  // ****************************************************

  static get sortableFields () { return [] }
  static get publicURLprefix () { return null }
  static get publicURL () { return null } // Not mandatory (if you want your store to be API-only for some reason)

  static get schema () { return null }
  static get idProperty () { return null } // If not set in prototype, taken as last item of paramIds)
  static get paramIds () { return [] } // Only allowed if publicURL is not set
  static get searchSchema () { return null } // If not set in prototype, worked out from `schema` by constructor

  static get storeName () { return null }
  static get _singleFields () { return {} } // Fields that can be updated singularly

  // ****************************************************
  // *** ATTRIBUTES THAT DEFINE STORE'S BEHAVIOUR
  // ****************************************************

  static get handlePut () { return false }
  static get handlePost () { return false }
  static get handleGet () { return false }
  static get handleGetQuery () { return false }
  static get handleDelete () { return false }

  static get positioning () { return false }    // If set, will make fields re-positionable
  static get defaultSort () { return null }  // If set, it will be applied to all getQuery calls

  // ****************************************************
  // *** ERRORS
  // ****************************************************

  // Default error objects which might be used by this module.
  static get BadRequestError () { return e.BadRequestError }
  static get UnauthorizedError () { return e.UnauthorizedError }
  static get ForbiddenError () { return e.ForbiddenError }
  static get NotFoundError () { return e.NotFoundError }
  static get PreconditionFailedError () { return e.PreconditionFailedError }
  static get UnprocessableEntityError () { return e.UnprocessableEntityError }
  static get NotImplementedError () { return e.NotImplementedError }
  static get ServiceUnavailableError () { return e.ServiceUnavailableError }

  // Static getter/setter which will actually manipulate the one `registry` variable

  static get registry () { return registry }
  static set registry (r) { registry = r }
  static getStore (storeName) { return registry[ storeName ] }
  static deleteStore (storeName) { delete registry[ storeName ] }
  static getAllStores () { return registry }

  // Methods that MUST be implemented for the store to be functional

  async implementFetchOne (request) {
    throw (new Error('implementFetchOne not implemented, store is not functional'))
  }

  async implementInsert (request, forceId) {
    throw (new Error('implementInsert not implemented, store is not functional'))
  }

  async implementUpdate (request, deleteUnsetFields) {
    throw (new Error('implementUpdate not implemented, store is not functional'))
  }

  async implementDelete (request) {
    throw (new Error('implementDelete not implemented, store is not functional'))
  }

  async implementQuery (request) {
    throw (new Error('implementQuery not implemented, store is not functional'))
  }

  async implementReposition (doc, where, beforeId) {
    throw (new Error('implementReposition not implemented, store is not functional'))
  }

  // ****************************************************
  // *** BEFORE AND AFTER HOOKS
  // ****************************************************

  async beforeCheckParamIds (request, method) { }
  async afterCheckParamIds (request, method) { }

  async beforeCheckPermissions (request, method) { }
  async afterCheckPermissions (request, method) { }

  async beforeValidate (request, method) { }
  async afterValidate (request, method) { }

  async beforeDbOperationFetchOne (request, method) { }
  async beforeDbOperationQuery (request, method) { }
  async beforeDbOperationInsert (request, method) { }
  async beforeDbOperationUpdate (request, method) { }
  async beforeDbOperationDelete (request, method) { }

  async afterDbOperationFetchOne (request, method) { }
  async afterDbOperationQuery (request, method) { }
  async afterDbOperationInsert (request, method) { }
  async afterDbOperationUpdate (request, method) { }
  async afterDbOperationDelete (request, method) { }

  async beforeReposition (request, method) { }
  async afterReposition (request, method) { }

  async beforeReturn (request, method) { }

  // ****************************************************
  // *** IMPORTANT IMPLEMENTATION HOOKS
  // ****************************************************

  async checkPermissions (request, method) { return { granted: true } }
  async send (request, method) { return () => {} }

  // ****************************************************
  // *** ERROR-MANAGING HELPER FUNCTIONS
  // ****************************************************

  formatErrorResponse (error) {
    if (error.errors) {
      return { message: error.message, errors: error.errors }
    } else {
      return { message: error.message }
    }
  }

  logError (request, error) {
  }

  // **************************************************************************
  // *** END OF FUNCTIONS/ATTRIBUTES THAT NEED/CAN BE OVERRIDDEN BY DEVELOPERS
  // **************************************************************************

  constructor () {
    var self = this
    var Self = this.constructor
    var k

    // Copy values over from the class' static values
    self.sortableFields = Self.sortableFields
    self.publicURLprefix = Self.publicURLprefix
    self.publicURL = Self.publicURL
    self.schema = Self.schema
    self.idProperty = Self.idProperty
    self.paramIds = Self.paramIds
    self.searchSchema = Self.searchSchema
    self.storeName = Self.storeName

    self.handlePost = Self.handlePost
    self.handlePut = Self.handlePut
    self.handleGet = Self.handleGet
    self.handleGetQuery = Self.handleGetQuery
    self.handleDelete = Self.handleDelete
    self.positioning = Self.positioning
    self.defaultSort = Self.defaultSort

    // This will contain the single fields
    self._singleFields = {}

    // The store name must be defined
    if (self.storeName === null) {
      throw (new Error('You must define a store name for a store in constructor class'))
    }

    if (typeof (registry[ self.storeName ]) !== 'undefined') {
      throw new Error('Cannot instantiate two stores with the same name: ' + self.storeName)
    }

    // The schema must be defined
    if (self.schema == null) {
      throw (new Error('You must define a schema'))
    }

    // If paramId is not specified, takes it from publicURL
    if (self.paramIds.length === 0 && typeof (self.publicURL) === 'string') {
      self.paramIds = (self.publicURL + '/').match(/:.*?\/+/g).map(
        function (i) {
          return i.substr(1, i.length - 2)
        }
      )
    }

    // If idProperty is not set, derive it from self._lastParamId()
    if (!self.idProperty) {
      if (self.paramIds.length === 0) {
        throw (new Error('Your store needs to set idProperty, or alternatively set paramIds (idProperty will be the last paramId). Store: ' + self.storeName))
      }

      // Sets self.idProperty, which (as for the principle of
      // least surprise) must be the last paramId passed to
      // the Self.
      self.idProperty = self._lastParamId()
    }

    // By default, paramIds are set in schema as { type: 'id' } so that developers
    // can be lazy when defining their schemas
    for (var i = 0, l = self.paramIds.length; i < l; i++) {
      k = self.paramIds[ i ]
      if (typeof (self.schema.structure[ k ]) === 'undefined') {
        self.schema.structure[ k ] = { type: 'id' }
      }
    }

    // If onlineSearchSchema wasn't defined, then set it as a copy of the schema where
    // fields are `searchable`, EXCLUDING the paramIds fields.
    if (self.searchSchema == null) {
      var searchSchemaStructure = { }
      for (k in self.schema.structure) {
        if (self.schema.structure[ k ].searchable && self.paramIds.indexOf(k) === -1) {
          searchSchemaStructure[ k ] = self.schema.structure[ k ]
        }
      }
      self.searchSchema = new self.schema.constructor(searchSchemaStructure)
    }

    // Make up a hash of single fields
    for (k in self.schema.structure) {
      if (self.schema.structure[ k ].singleField) {
        self._singleFields[ k ] = self.schema.structure[ k ]
      }
    }

    Self.registry[ self.storeName ] = self
  }

  // Simple function that shallow-copies an object.
  _co (o) { return Object.assign({}, o) }

  // Will call implementReposition based on options.
  // -putBefore is an id.
  // -putDefaultPosition is 'start' or 'end'.
  // -existing is true or false: true for existing records false for new ones
  //
  // When calling implementReposition:
  // - where can be 'start', 'end' or 'before'
  // - beforeId is only meaningful for 'before' (tell is where to place it)
  // - existing is boolean, and it's only meaningful if this.position is there
  async _repositionBasedOnOptions (fullDoc, putBefore, putDefaultPosition, existing) {
    // No position field: nothing to do
    if (!this.positioning) return

    // CASE #1: putBefore is set: where = at, beforeId = putBefore
    if (putBefore) {
      await this.implementReposition(fullDoc, 'before', putBefore)

    // CASE #2: putDefaultPosition is set: where = putDefaultPosition, beforeId = null
    } else if (putDefaultPosition) {
      await this.implementReposition(fullDoc, putDefaultPosition, null)

    // CASE #3: putBefore and putDefaultPosition are not set. IF it's a new record, where = end, beforeId = null
    } else if (!existing) {
      await this.implementReposition(fullDoc, 'end', null)
    }
    // CASE #4: don't do anything.
  }

  getFullPublicURL () {
    // No prefix: return the publicURL straight
    if (!this.publicURLPrefix) return this.publicURL

    return path.join(this.publicURLPrefix, this.publicURL)
  }

  _lastParamId () {
    return this.paramIds[ this.paramIds.length - 1 ]
  }

  _sleep (ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
  }

  // Check that paramsId are actually legal IDs using
  // paramsSchema.
  async _checkParamIds (request, skipIdProperty) {
    var self = this
    var fieldErrors = []

    // Params is empty: nothing to do, optimise a little
    if (request.params.length === 0) return

    // Check that ALL paramIds do belong to the schema
    self.paramIds.forEach(function (k) {
      if (typeof (self.schema.structure[ k ]) === 'undefined') {
        throw new Error('This paramId must be in schema: ' + k)
      }
    })

    // If it's a remote request, check that _all_ paramIds are in params
    // (Local API requests can avoid passing paramIds)
    if (request.remote) {
      self.paramIds.forEach(function (k) {
        // "continue" if id property is to be skipped
        if (skipIdProperty && k === self.idProperty) return

        // Required paramId not there: puke!
        if (typeof (request.params[ k ]) === 'undefined') {
          fieldErrors.push({ field: k, message: 'Field required in the URL: ' + k })
        }
      })
      // If one of the key fields was missing, puke back
      if (fieldErrors.length) throw new Store.BadRequestError({ errors: fieldErrors })
    };

    // Prepare skipParams and skipCast, depending on skipIdProperty
    var skipFields = [ ]
    if (skipIdProperty) {
      skipFields.push(self.idProperty)
    }

    // Validate request.params
    var { validatedObject, errors } = await self.schema.validate(request.params, { onlyObjectValues: true, skipFields })
    if (errors.length) throw new Store.BadRequestError({ errors: errors })

    request.params = validatedObject
  }

  _enrichBodyWithParamIdsIfRemote (request) {
    var self = this

    if (request.remote) {
      self.paramIds.forEach(function (paramId) {
        if (typeof (request.params[ paramId ]) !== 'undefined') {
          request.body[ paramId ] = request.params[ paramId ]
        }
      })
    }
  }

  async _makePost (request) {
    var self = this

    // This is the 'doc' as such
    request.doc = null

    // Check that the method is implemented
    if (!self.handlePost && request.remote) throw new Store.NotImplementedError()

    // Check the IDs
    await self.beforeCheckParamIds(request, 'post')
    await self._checkParamIds(request, true)
    await self.afterCheckParamIds(request, 'post')

    // Add paramIds to body
    self._enrichBodyWithParamIdsIfRemote(request)

    // Run validation, throw an error if it fails
    await self.beforeValidate(request, 'post')
    var { validatedObject, errors } = await self.schema.validate(request.body, { skipFields: [ self.idProperty ] })
    request.bodyBeforeValidation = request.body
    request.body = validatedObject
    if (errors.length) throw new Store.UnprocessableEntityError({ errors: errors })
    await self.afterValidate(request, 'post')

    // Check permissions
    await self.beforeCheckPermissions(request, 'post')
    var { granted, message } = await self.checkPermissions(request, 'post')
    if (!granted) throw new Store.ForbiddenError(message)
    await self.afterCheckPermissions(request, 'post')

    // Execute actual DB operation
    await self.beforeDbOperationInsert(request, 'post')
    request.doc = await self.implementInsert(request, 'post')
    await self.afterDbOperationInsert(request, 'post')

    // Reposition if needed
    await self.beforeReposition(request, 'post')
    await self._repositionBasedOnOptions(request.doc, request.options.putBefore, request.options.putDefaultPosition, false)
    await self.afterReposition(request, 'post')

    // Send over to the client
    await self.beforeReturn(request, 'post')
    return request.doc
  }

  async _makePut (request) {
    var self = this

    // This is the 'doc' as such
    request.doc = null

    // Check that the method is implemented
    if (!self.handlePut && request.remote) throw new Store.NotImplementedError()

    // Check the IDs
    await self.beforeCheckParamIds(request, 'put')
    await self._checkParamIds(request, true)
    await self.afterCheckParamIds(request, 'put')

    // Add paramIds to body
    self._enrichBodyWithParamIdsIfRemote(request)

    // Run validation, throw an error if it fails
    await self.beforeValidate(request, 'put')
    var { validatedObject, errors } = await self.schema.validate(request.body)
    request.bodyBeforeValidation = request.body
    request.body = validatedObject
    if (errors.length) throw new Store.UnprocessableEntityError({ errors: errors })
    await self.afterValidate(request, 'put')

    // Fetch the record
    await self.beforeDbOperationFetchOne(request, 'put')
    request.doc = await self.implementFetchOne(request, 'put')
    await self.afterDbOperationFetchOne(request, 'put')

    request.putNew = !request.doc
    request.putExisting = !!request.doc

    // Check permissions
    await self.beforeCheckPermissions(request, 'put')
    let { granted, message } = await self.checkPermissions(request, 'put')
    if (!granted) throw new Store.ForbiddenError(message)
    await self.afterCheckPermissions(request, 'put')

    if (!request.doc) {
      // Execute actual DB operation
      await self.beforeDbOperationInsert(request, 'put')
      request.doc = await self.implementInsert(request, 'put')
      await self.afterDbOperationInsert(request, 'put')
    } else {
      // Execute actual DB operation
      await self.beforeDbOperationUpdate(request, 'put')
      request.doc = await self.implementUpdate(request, 'put')
      await self.afterDbOperationUpdate(request, 'put')
    }

    // Reposition if needed
    await self.beforeReposition(request, 'put')
    await self._repositionBasedOnOptions(request.doc, request.options.putBefore, request.options.putDefaultPosition, !!request.putExisting)
    await self.afterReposition(request, 'put')

    // Send over to the client
    await self.beforeReturn(request, 'put')
    return request.doc
  }

  async _makeGet (request) {
    var self = this

    // This is the 'doc' as such
    request.doc = null

    // Check that the method is implemented
    if (!self.handleGet && request.remote) throw new Store.NotImplementedError()

    // Check the IDs
    await self.beforeCheckParamIds(request, 'get')
    await self._checkParamIds(request, true)
    await self.afterCheckParamIds(request, 'get')

    // Execute actual DB operation
    await self.beforeDbOperationFetchOne(request, 'get')
    request.doc = await self.implementFetchOne(request, 'get')
    await self.afterDbOperationFetchOne(request, 'get')

    // Check permissions
    await self.beforeCheckPermissions(request, 'get')
    var { granted, message } = await self.checkPermissions(request, 'get')
    if (!granted) throw new Store.ForbiddenError(message)
    await self.afterCheckPermissions(request, 'get')

    // Send over to the client
    await self.beforeReturn(request, 'get')
    return request.doc
  }

  async _makeGetQuery (request) {
    var self = this

    // This is the 'doc' as such
    request.doc = null

    // Check that the method is implemented
    if (!self.handleGet && request.remote) throw new Store.NotImplementedError()

    // Check the IDs
    await self.beforeCheckParamIds(request, 'getQquery')
    await self._checkParamIds(request, true)
    await self.beforeCheckParamIds(request, 'getQuery')

    // Validate the search schema
    var { validatedObject, errors } = await self.schema.validate(request.params, { onlyObjectValues: true })
    if (errors.length) throw new self.BadRequestError({ errors: errors })

    request.options.conditionsHash = validatedObject

    // Check permissions
    await self.beforeCheckPermissions(request, 'getQuery')
    var { granted, message } = await self.checkPermissions(request, 'getQuery')
    if (!granted) throw new Store.ForbiddenError(message)
    await self.afterCheckPermissions(request, 'getQuery')

    // Execute actual DB operation
    await self.beforeDbOperationQuery(request, 'getQuery')
    request.doc = await self.implementQuery(request, 'getQuery')
    await self.afterDbOperationQuery(request, 'getQuery')

    // Send over to the client
    await self.beforeReturn(request, 'getQuery')
    return request.doc
  }

  async _makeDelete (request) {
    var self = this

    // This is the 'doc' as such
    request.doc = null

    // Check that the method is implemented
    if (!self.handleDelete && request.remote) throw new Store.NotImplementedError()

    // Check the IDs
    await self.beforeCheckParamIds(request, 'delete')
    await self._checkParamIds(request, true)
    await self.beforeCheckParamIds(request, 'delete')

    // Fetch the record
    await self.beforeDbOperationFetchOne(request, 'delete')
    request.doc = await self.implementFetchOne(request, 'delete')
    await self.afterDbOperationFetchOne(request, 'delete')

    // Check permissions
    await self.beforeCheckPermissions(request, 'delete')
    var { granted, message } = await self.checkPermissions(request, 'delete')
    if (!granted) throw new Store.ForbiddenError(message)
    await self.afterCheckPermissions(request, 'delete')

    // Execute actual DB operation
    await self.beforeDbOperationDelete(request, 'delete')
    await self.implementDelete(request, 'delete')
    await self.afterDbOperationDelete(request, 'delete')

    // Send over to the client
    await self.beforeReturn(request, 'delete')
    return request.doc
  }

  apiGetQuery (options) {
    options = options || {}

    // Make up the request
    var request = {}
    request.remote = false
    request.body = {}
    if (options.apiParams) request.params = options.apiParams
    else request.params = {}

    request.session = options.session || {}
    request.options = this._co(options)

    this._makeGetQuery(request)
  }

  apiGet (id, options) {
    options = options || {}

    // Make up the request
    var request = {}
    request.remote = false
    request.options = options
    request.body = {}
    if (options.apiParams) request.params = options.apiParams
    else { request.params = {}; request.params[ this.idProperty ] = id }
    request.session = options.session || {}

    // Actually run the request
    this._makeGet(request)
  }

  apiPut (body, options) {
    options = options || {}

    // This will only work if this.idProperty is included in the body object
    if (typeof (body[ this.idProperty ]) === 'undefined') {
      throw (new Error('When calling Store.apiPut with an ID of null, id MUST be in body'))
    }

    // Make up the request
    var request = {}
    request.remote = false
    request.options = options
    request.body = this._co(body)
    if (options.apiParams) request.params = options.apiParams
    else { request.params = {}; request.params[ this.idProperty ] = body[ this.idProperty ] }
    request.session = options.session || {}

    // Actually run the request
    this._makePut(request)
  }

  apiPost (body, options) {
    options = options || {}

    // Make up the request
    var request = {}
    request.remote = false
    request.options = options
    request.params = options.apiParams || {}
    request.session = options.session || {}
    request.body = this._co(body)

    // Actually run the request
    this._makePost(request)
  }

  apiDelete (id, options, next) {
    options = options || {}

    // Make up the request
    var request = {}
    request.body = {}
    request.options = options
    if (options.apiParams) request.params = options.apiParams
    else { request.params = {}; request.params[ this.idProperty ] = id }
    request.session = options.session || {}

    // Actually run the request
    this._makeDelete(request, next)
  }
}

exports = module.exports = Store
