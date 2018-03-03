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
  * Don't specify `onlineSearchSchema`. It will be worked out taking all schema element marked as
    `searchable: true` (except paramIds)
*/

var e = require('allhttperrors')
var SimpleDbLayerMixin = require('./SimpleDbLayerMixin.js')
var path = require('path')
var DO = require('deepobject')
var { asyncForEach, asyncMap } = require('p-iterator')

var _co = function (o) {
  var newO = {}
  for (var k in o) if (o.hasOwnProperty(k)) newO[ k ] = o[ k ]
  return newO
}

var registry = {}

var Store = class {
  //
  // ***********************************************************
  // *** ATTRIBUTES THAT ALWAYS NEED TO BE DEFINED IN PROTOTYPE
  // ***********************************************************

  static get storeName () { return null }
  static get schema () { return null }
  static get _singleFields () { return {} } // Fields that can be updated singularly

  static get artificialDelay () { return 0 }

  // ****************************************************
  // *** ATTRIBUTES THAT CAN TO BE DEFINED IN PROTOTYPE
  // ****************************************************

  static get onlineSearchSchema () { return null } // If not set in prototype, worked out from `schema` by constructor
  static get sortableFields () { return [] }
  static get publicURLprefix () { return null }
  static get publicURL () { return null } // Not mandatory (if you want your store to be API-only for some reason)
  static get idProperty () { return null } // If not set in prototype, taken as last item of paramIds)
  static get paramIds () { return [] } // Only allowed if publicURL is not set

  // ****************************************************
  // *** ATTRIBUTES THAT DEFINE STORE'S BEHAVIOUR
  // ****************************************************

  static get handlePut () { return false }
  static get handlePost () { return false }
  static get handleGet () { return false }
  static get handleGetQuery () { return false }
  static get handleDelete () { return false }

  static get echoAfterPut () { return true }
  static get echoAfterPost () { return true }
  static get echoAfterDelete () { return true }

  static get chainErrors () { return 'none' }  // can be 'none' (do not chain), 'all' (chain all), 'nonhttp' (chain non-HTTP errors)

  static get position () { return false }    // If set, will make fields re-positionable
  static get defaultSort () { return null }  // If set, it will be applied to all getQuery calls

  // Static getter/setter which will actually manipulate the one `registry` variable

  static get registry () { return registry }
  static set registry (r) { registry = r }

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
  // *** FUNCTIONS THAT CAN BE OVERRIDDEN BY DEVELOPERS
  // ****************************************************

  // Permission stock function
  async checkPermissions (request, method, cb) { return true }

  // after* functions
  async afterValidate (request, method) { return null }
  async afterCheckPermissions (request, method) { return null }
  async afterDbOperation (request, method) { return null }
  async afterEverything (request, method) { return null }

  logError (error) { }  // eslint-disable-line

  formatErrorResponse (error) {
    if (error.errors) {
      return { message: error.message, errors: error.errors }
    } else {
      return { message: error.message }
    }
  }

  // **************************************************************************
  // *** END OF FUNCTIONS/ATTRIBUTES THAT NEED/CAN BE OVERRIDDEN BY DEVELOPERS
  // **************************************************************************

  constructor () {
    var self = this
    var k

    // Set artificialDelay from the constructor's default
    if (typeof (registry[ Store.storeName ]) !== 'undefined') {
      throw new Error('Cannot instantiate two stores with the same name: ' + self.storeName)
    }

    // The store name must be defined
    if (Store.storeName === null) {
      throw (new Error('You must define a store name for a store in constructor class'))
    }

    // The schema must be defined
    if (Store.schema == null) {
      throw (new Error('You must define a schema'))
    }

    // If paramId is not specified, takes it from publicURL
    if (Store.paramIds.length === 0 && typeof (Store.publicURL) === 'string') {
      self.paramIds = (self.publicURL + '/').match(/:.*?\/+/g).map(
        function (i) {
          return i.substr(1, i.length - 2)
        }
      )
    }

    // If idProperty is not set, derive it from self._lastParamId()
    if (!Store.idProperty) {
      if (Store.paramIds.length === 0) {
        throw (new Error('Your store needs to set idProperty, or alternatively set paramIds (idProperty will be the last paramId). Store: ' + self.storeName))
      }

      // Sets self.idProperty, which (as for the principle of
      // least surprise) must be the last paramId passed to
      // the store.
      Store.idProperty = self._lastParamId()
    }

    // By default, paramIds are set in schema as { type: 'id' } so that developers
    // can be lazy when defining their schemas
    for (var i = 0, l = Store.paramIds.length; i < l; i++) {
      k = Store.paramIds[ i ]
      if (typeof (Store.schema.structure[ k ]) === 'undefined') {
        Store.schema.structure[ k ] = { type: 'id' }
      }
    }

    // If onlineSearchSchema wasn't defined, then set it as a copy of the schema where
    // fields are `searchable`, EXCLUDING the paramIds fields.
    if (self.onlineSearchSchema == null) {
      var onlineSearchSchemaStructure = { }
      for (k in self.schema.structure) {
        if (self.schema.structure[ k ].searchable && self.paramIds.indexOf(k) === -1) {
          onlineSearchSchemaStructure[ k ] = self.schema.structure[ k ]
        }
      }
      self.onlineSearchSchema = new self.schema.constructor(onlineSearchSchemaStructure)
    }

    self._singleFields = {}
    for (k in self.schema.structure) {
      if (self.schema.structure[ k ].singleField) {
        self._singleFields[ k ] = self.schema.structure[ k ]
      }
    }

    registry[ self.storeName ] = self
  }

  // Simple function that shallow-copies an object. This should be used
  // every time  prepareBody, extrapolateDocProxy or prepareBeforeSendProxy are
  // overridden (in order to pass a copy of the object)
  static get _co () { return _co }


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
    if (!this.position) return

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
      if (fieldErrors.length) throw new self.BadRequestError({ errors: fieldErrors })
    };

    // Prepare skipParams and skipCast, depending on skipIdProperty
    var skipParams = {}
    var skipCast = [ ]
    if (skipIdProperty) {
      skipParams[ self.idProperty ] = [ 'required' ]
      skipCast.push(self.idProperty)
    }

    // Validate request.params
    var { params, errors } = await self.schema.validate(request.params, { onlyObjectValues: true, skipParams, skipCast })
    if (errors.length) throw new self.BadRequestError({ errors: errors })

    request.params = params
  }

  _sendError (request, method, error) {
    var self = this

    // It's a local call: simply call the callback passed by the caller
    if (!request.remote) throw error

    // This will happen when _sendError is passed an error straight from a callback
    // The idea is that jsonreststores _always_ throws an HTTP error of some sort.

    switch (self.chainErrors) {
      case 'all':
        throw error

      case 'none':
      case 'nonhttp':

        // CASE #1: It's not an HTTP error and it's meant to chain non-HTTP errors: chain (call next)
        if (typeof (e[ error.name ]) === 'undefined' && self.chainErrors === 'nonhttp') {
          throw error

        // CASE :2: Any other case. It might be an HTTP error or a JS error. Needs to handle both cases
        } else {
          // It's not an HTTP error: make up a new one, and incapsulate original error in it
          if (typeof (e[ error.name ]) === 'undefined') {
            error = new self.ServiceUnavailableError({ originalErr: error })
            error.stack = error.originalErr.stack
          }

          // Make up the response body based on the error, attach it to the error itself
          error.formattedErrorResponse = self.formatErrorResponse(error)
          error.originalMethod = method

          // Send the response, with `error` as pseudo-method
          self.sendData(request, 'error', error)
        }
        break
    }

    self.logError(error)
  }

  async _checkPermissionsProxy (request, method) {
    // It's an API request: permissions are totally skipped
    if (!request.remote) return true

    return this.checkPermissions(request, method)
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

  errorInSending (request, method, data, when, error) {
    this.logError(error)
  }

  // Method that will call the correct `protocolSend?????` method depending on
  // request.protocol
  // To keep the signature short, the status will be worked out depending on
  // what's being sent
  async sendData (request, method, data) {
    var n = 'protocolSend' + request.protocol
    var f = this[ n ]

    var self = this

    // Sets status and responseBody
    var status = 200
    switch (method) {
      case 'post': status = 201; break
      case 'put': if (request.putNew) status = 201; break
      case 'delete': if (data === '') status = 204; break
      case 'error': status = data.httpError; break
    };

    // The method must be implemented
    if (!f) throw new Error('Error: function self.' + n + ' not implemented!')

    // Call the `internalBeforeSendData()` hook
    try {
      await self._internalBeforeSendData(request, method, data)
    } catch (err) {
      await self.errorInSending(request, method, data, 'before', err)
    }

    // Call the function that _actually_ sends data
    try {
      await f.call(self, request, method, data, status)
    } catch (err) {
      if (err) return self.errorInSending(request, method, data, 'during', err)
    }

    // Call the `internalAfterSendData()` hook
    try {
      await self._internalAfterSendData(request, method, data)
    } catch (err) {
      if (err) return self.errorInSending(request, method, data, 'after', err)
    }
  }

  async _internalBeforeSendData (request, method, data) {
  }

  async _internalAfterSendData (request, method, data) {
  }

  protocolListen (protocol, params) {
    var n = 'protocolListen' + protocol
    var f = this[ n ]

    // The method must be implemented
    if (!f) throw new Error('Error: function self.' + n + ' not implemented!')

    f.call(this, params)
  }

  // Check if there is already a record where field `field`
  // already has value `value` and it's not id `id`
  async _isFieldUnique (field, value, id) {
    var self = this

    var conditions

    if (id) {
      conditions = {
        type: 'and',
        args: [
          {
            type: 'eq',
            args: [ field, value ]
          },
          {
            type: 'ne',
            args: [ self.idProperty, id ]
          }
        ]
      }
    } else {
      conditions = {
        type: 'eq',
        args: [ field, value ]
      }
    };

    var records = await self.dbLayer.select(conditions)
    if (records.total) return false
    return true
  }

  async _areFieldsUnique (id, body) {
    var self = this

    var errors = []

    await asyncForEach(Object.keys(self._uniqueFields), async (field) => {
      if (typeof body[ field ] === 'undefined' || body[ field ] === '') return

      // Check the field's uniqueness
      var isUnique = await self._isFieldUnique(field, body[ field ], id)

      // If it's a duplicate, enrich the `errors` array
      if (!isUnique) {
        errors.push({ field: field, message: (self.schema.structure[field].uniqueMessage || 'Field already in database') })
      }
    })

    if (errors.length === 0) return { allUnique: true }

    // Errors: return them, along with `false`
    return { allUnique: true, errors }
  }

  async _makePost (request) {
    var self = this

    try {
      // Prepare request.data
      request.data = request.data || {}

      // Check that the method is implemented
      if (!self.handlePost && request.remote) throw new self.NotImplementedError()

      // Check the IDs
      await self._checkParamIds(request, true)

      var protectedFields = []
      Object.keys(self.schema.structure).forEach((field) => {
        if (self.schema.structure[ field ].protected) protectedFields.push(field)
      })

      // Protected field are not allowed here
      // (Except the ones marked in `bodyComputed`)
      protectedFields.forEach((field) => {
        if (typeof (request.body[ field ]) !== 'undefined') {
          // NOTE: Will only delete it if it wasn't marked as "computed" in the request.
          if (typeof (request.bodyComputed) === 'object' && request.bodyComputed != null && !request.bodyComputed[ field ]) {
            delete request.body[ field ]
          }
        }
      })

      await self._doAutoLookup(request, 'post')

      var preparedBody = await self.prepareBody(request, 'post', request.body)

      // Request is changed, old value is saved
      request.bodyBeforePrepare = request.body
      request.body = preparedBody

      var skipParamsObject = {}
      skipParamsObject[ self.idProperty ] = [ 'required' ]
      self._enrichBodyWithParamIdsIfRemote(request)

      // Delete _children which mustn't be here regardless
      delete request.body._children

      // Make up a hash of CHANGED body fields
      // WHY would protected fields be defined?
      // BECAUSE prepareBody might have done it, or a field might be bodyComputed (effectively exceptions to early deletion)
      var changedBodyFields = {}
      protectedFields.forEach((field) => {
        if (typeof (request.body[ field ]) !== 'undefined') {
          changedBodyFields[ field ] = true
        }
      })

      // Run validation, throw an error if it fails
      var { validatedBody, errors } = await self.schema.validate(request.body,
        { skipParams: skipParamsObject,
          skipCast: [ self.idProperty ]
        }
      )
      if (errors.length) throw new self.UnprocessableEntityError({ errors: errors })

      // Validation might have set some defaults on protected fields.
      // Unless they were marked as changed, DELETE those.
      protectedFields.forEach((field) => {
        if (!changedBodyFields[ field ]) {
          delete validatedBody[ field ]
        }
      })

      request.bodyBeforeValidation = request.body
      request.body = validatedBody

      await self.afterValidate(request, 'post')

      var un = await self._areFieldsUnique(null, request.body)
      if (!un.allUnique) throw new self.UnprocessableEntityError({ errors: un.errors })

      // Actually check permissions
      var { granted, message } = await self._checkPermissionsProxy(request, 'post')
      if (!granted) throw new self.ForbiddenError(message)

      await self.afterCheckPermissions(request, 'post')

      // Clean up body from things that are not to be submitted
      self.schema.cleanup(request.body, 'doNotSave')

      var forceId = await self.schema.makeId(request.body)

      request.data.fullDoc = await self.implementInsert(request, forceId)

      await self._repositionBasedOnOptions(request.data.fullDoc, request.options.putBefore, request.options.putDefaultPosition, false)

      await self.afterDbOperation(request)

      request.data.doc = await self.extrapolateDocProxy(request, 'post', request.data.fullDoc)

      request.data.preparedDoc = await self.prepareBeforeSendProxy(request, 'post', request.data.doc)

      await self.afterEverything(request, 'post')

      if (request.remote) {
        if (self.echoAfterPost) {
          self.sendData(request, 'post', request.data.preparedDoc)
        } else {
          self.sendData(request, 'post', '')
        }
      } else {
        return { preparedDoc: request.data.preparedDoc, request }
      }

    // Catch errors, run _sendError with the right arguments if there was an exception
    } catch (e) {
      return self._sendError(request, 'post', e)
    }
  }

  async _makePut (request, next) {
    var self = this

    // Prepare request.data
    request.data = request.data || {}

    if (!self.handlePut && !request.options.field && request.remote) {
      throw new self.NotImplementedError()
    }

    // DETOUR: It's a reposition. Not allowed here!
    if (typeof (request.options.putBefore) !== 'undefined') {
      throw new Error('Option putBefore not allowed in OneFieldStore')
    }

    // Check the IDs
    await self._checkParamIds(request, false)


    var protectedFields = []
    Object.keys(self.schema.structure).forEach((field) => {
      if (self.schema.structure[ field ].protected) protectedFields.push(field)
    })

    // Protected field are not allowed here
    // (Except the ones marked in `bodyComputed`)
    protectedFields.forEach((field) => {
      if (typeof (request.body[ field ]) !== 'undefined') {
        // NOTE: Will only delete it if it wasn't marked as "computed" in the request.
        if (typeof (request.bodyComputed) === 'object' && request.bodyComputed != null && !request.bodyComputed[ field ]) {
          delete request.body[ field ]
        }
      }
    })

    await self._doAutoLookup(request, 'put')

    request.bodyBeforePrepare = request.body
    request.body = await self.prepareBody(request, 'put', request.body)

    self._enrichBodyWithParamIdsIfRemote(request)

    // Delete _children which mustn't be here regardless
    delete request.body._children

    if (request.options.field) {
      var errorsInPiggyField = []

      // Only the single field is allowed in body (and the paramId fields)
      for (var field in request.body) {
        if (self.paramIds.indexOf(field) === -1 && field !== request.options.field) {
          errorsInPiggyField.push({ field: field, message: 'Field not allowed because not a paramId nor the single field: ' + field + ' in ' + self.storeName })
        }
      }

      // If it's a single field, then the single field's value MUST be set in body
      if (typeof request.body[ request.options.field ] === 'undefined') {
        errorsInPiggyField.push({ field: request.options.field, message: 'When putting onto a field, that field must be in the payload' })
      }

      // If there was an error, then quit it
      if (errorsInPiggyField.length) throw new self.UnprocessableEntityError({ errors: errorsInPiggyField })
    }

    // Make up a hash of CHANGED body fields
    // WHY would protected fields be defined?
    // BECAUSE prepareBody might have done it, or a field might be bodyComputed (effectively exceptions to early deletion)
    var changedBodyFields = {}
    protectedFields.forEach((field) => {
      if (typeof (request.body[ field ]) !== 'undefined') {
        changedBodyFields[ field ] = true
      }
    })

    var { validatedBody, errors } = await self.schema.validate(request.body,
      { onlyObjectValues: !!request.options.field
      }
    )

      // Validation might have set some defaults on protected fields.
      // Unless they were marked as changed, DELETE those.
      protectedFields.forEach((field) => {
        if (!changedBodyFields[ field ]) {
          delete validatedBody[ field ]
        }
      })

      request.bodyBeforeValidation = request.body
      request.body = validatedBody

      if (errors.length) throw new self.UnprocessableEntityError({ errors: errors })

      await self.afterValidate(request, 'put')

      var fullDoc = await self.implementFetchOne(request)

      // OneFieldStores will only ever work on already existing records
      if (!fullDoc && request.options.field) throw new self.NotFoundError()

      // Check the 'overwrite' option:
      // * if it's on, then the record must be existing,
      // * if it's off, then the record must be new
      if (typeof (request.options.overwrite) !== 'undefined') {
        if (fullDoc && !request.options.overwrite) throw new self.PreconditionFailedError()
        else if (!fullDoc && request.options.overwrite) throw new self.PreconditionFailedError()
      }

      // BIG FORK HERE. The workflow will be different for new records
      // and for existing records

      // CASE #1: It's a new record
      if (!fullDoc) {
        request.putNew = true

        var un = await self._areFieldsUnique(null, request.body)
        if (!un.allUnique) throw new self.UnprocessableEntityError({ errors: un.errors })


                      // Actually check permissions
                      self._checkPermissionsProxy(request, 'put', function (err, granted, message) {
                        if (err) return self._sendError(request, 'put', next, err)

                        if (!granted) return self._sendError(request, 'put', next, new self.ForbiddenError(message))

                        self.afterCheckPermissions(request, 'put', function (err) {
                          if (err) return self._sendError(request, 'put', next, err)

                          // Clean up body from things that are not to be submitted
                          // if( self.schema ) self.schema.cleanup( body, 'doNotSave' );
                          self.schema.cleanup(request.body, 'doNotSave')

                          // Since it's a new record, if there were any defaults from the
                          // previous validation, assign it.
                          // But ONLY if the field hasn't already been assigned by another hook before

                          self.implementInsert(request, null, function (err, fullDoc) {
                            if (err) return self._sendError(request, 'put', next, err)

                            request.data.fullDoc = fullDoc

                            self._repositionBasedOnOptions(request.data.fullDoc, request.options.putBefore, request.options.putDefaultPosition, false, function (err) {
                              if (err) return self._sendError(request, 'put', next, err)

                              self.afterDbOperation(request, 'put', function (err) {
                                if (err) return self._sendError(request, 'put', next, err)

                                self.extrapolateDocProxy(request, 'put', request.data.fullDoc, function (err, doc) {
                                  if (err) return self._sendError(request, 'put', next, err)

                                  request.data.doc = doc

                                  self.prepareBeforeSendProxy(request, 'put', request.data.doc, function (err, preparedDoc) {
                                    if (err) return self._sendError(request, 'put', next, err)

                                    request.data.preparedDoc = preparedDoc

                                    self.afterEverything(request, 'put', function (err) {
                                      if (err) return self._sendError(request, 'put', next, err)

                                      if (request.remote) {
                                        if (self.echoAfterPut) {
                                          self.sendData(request, 'put', request.data.preparedDoc)
                                        } else {
                                          self.sendData(request, 'put', '')
                                        }
                                      } else {
                                        next(null, request.data.preparedDoc, request)
                                      }
                                    })
                                  })
                                })
                              })
                            })
                          })
                        })
                      })
                    })

                  // It's an EXISTING doc: it will need to be an update, _and_ permissions will be
                  // done on inputted data AND existing doc
                  } else {
                    request.data.fullDoc = fullDoc
                    request.putExisting = true

                    self._areFieldsUnique(fullDoc[ self.idProperty ], request.body, function (err, allUnique, errors) {
                      if (err) return self._sendError(request, 'post', next, err)

                      if (!allUnique) return self._sendError(request, 'post', next, new self.UnprocessableEntityError({ errors: errors }))

                      self.extrapolateDocProxy(request, 'put', request.data.fullDoc, function (err, doc) {
                        if (err) return self._sendError(request, 'put', next, err)

                        request.data.doc = doc

                        // Actually check permissions
                        self._checkPermissionsProxy(request, 'put', function (err, granted, message) {
                          if (err) return self._sendError(request, 'put', next, err)

                          if (!granted) return self._sendError(request, 'put', next, new self.ForbiddenError(message))

                          self.afterCheckPermissions(request, 'put', function (err) {
                            if (err) return self._sendError(request, 'put', next, err)

                            // Clean up body from things that are not to be submitted
                            // if( self.schema ) self.schema.cleanup( body, 'doNotSave' );
                            self.schema.cleanup(request.body, 'doNotSave')

                            // Since it's a existing record, if body isn't assigned it and existing record has a value,
                            // assign the existing value
                            protectedFields.forEach((field) => {
                              if (typeof (request.body[ field ]) === 'undefined' && typeof (request.data.doc[ field ]) !== 'undefined') {
                                request.body[ field ] = request.data.doc[ field ]
                              }
                            })

                            self.implementUpdate(request, !request.options.field, function (err, fullDocAfter) {
                              if (err) return self._sendError(request, 'put', next, err)

                              // Update must have worked -- if it hasn't, there was a (bad) problem
                              if (!fullDocAfter) return self._sendError(request, 'put', next, new Error('Error re-fetching document after update in put'))

                              request.data.fullDocAfter = fullDocAfter

                              self._repositionBasedOnOptions(request.data.fullDoc, request.options.putBefore, request.options.putDefaultPosition, true, function (err) {
                                if (err) return self._sendError(request, 'put', next, err)

                                self.afterDbOperation(request, 'put', function (err) {
                                  if (err) return self._sendError(request, 'put', next, err)

                                  self.extrapolateDocProxy(request, 'put', request.data.fullDocAfter, function (err, docAfter) {
                                    if (err) return self._sendError(request, 'put', next, err)

                                    request.data.docAfter = docAfter

                                    self.prepareBeforeSendProxy(request, 'put', request.data.docAfter, function (err, preparedDoc) {
                                      if (err) return self._sendError(request, 'put', next, err)

                                      request.data.preparedDoc = preparedDoc

                                      self.afterEverything(request, 'put', function (err) {
                                        if (err) return self._sendError(request, 'put', next, err)

                                        // Manipulate fullDoc: at this point, it's the WHOLE database record,
                                        // whereas I only want returned paramIds AND the piggyField
                                        // if( request.options.field ){
                                        //  for( var field in fullDocAfter ){
                                        //    if( self.paramIds.indexOf( field ) == -1 && field != request.options.field ) delete fullDocAfter[ field ];
                                        //  }
                                        // }

                                        if (request.remote) {
                                          if (self.echoAfterPut) {
                                            self.sendData(request, 'put', request.data.preparedDoc)
                                          } else {
                                            self.sendData(request, 'put', '')
                                          }
                                        } else {
                                          next(null, request.data.preparedDoc, request)
                                        }
                                      })
                                    })
                                  })
                                })
                              })
                            })
                          })
                        })
                      })
                    })
                  } // Existing or new doc
                } // continueAfterFetch
              })
            })
          })
        })
      })
    })
  }

  _makeGetQuery (request, next) {
    var self = this

    if (typeof (next) !== 'function') next = function () {}

    // Prepare request.data
    request.data = request.data || {}

    // Check that the method is implemented
    if (!self.handleGetQuery && !request.options.field && request.remote) {
      return self._sendError(request, 'getQuery', next, new self.NotImplementedError())
    }

    // Check the IDs. If there is a problem, it means an ID is broken:
    // return a BadRequestError
    self._checkParamIds(request, true, function (err) {
      if (err) return self._sendError(request, 'getQuery', next, err)

      self._doAutoLookup(request, 'getQuery', function (err) {
        if (err) return self._sendError(request, 'post', next, err)

        self._checkPermissionsProxy(request, 'getQuery', function (err, granted, message) {
          if (err) return self._sendError(request, 'getQuery', next, err)

          if (!granted) return self._sendError(request, 'getQuery', next, new self.ForbiddenError(message))

          self.afterCheckPermissions(request, 'getQuery', function (err) {
            if (err) return self._sendError(request, 'getQuery', next, err)

            self.onlineSearchSchema.validate(request.options.conditionsHash, { onlyObjectValues: true }, function (err, conditionsHash, errors) {
              if (err) return self._sendError(request, 'getQuery', next, err)

              // Errors in casting: give up, run away
              if (errors.length) return self._sendError(request, 'getQuery', next, new self.BadRequestError({ errors: errors }))

              // Actually assigning cast and validated conditions to `options`
              request.options.conditionsHash = conditionsHash

              self.afterValidate(request, 'getQuery', function (err) {
                if (err) return self._sendError(request, 'getQuery', next, err)

                var inn = function (o) { return require('util').inspect(o, { depth: 10 }) }
                // console.log("CONDITION HASH:", inn( conditionsHash ) );
                // console.log("QUERY CONDITIONS:", inn( self.queryConditions ) );

                // Resolve queryConditions (with all #variable# replacement, `each` etc.
                // properly expanded and ready to be fed to `implementQuery`)
                request.options.resolvedQueryConditions = self._resolveQueryConditions(
                  self.queryConditions,
                  request.options.conditionsHash,
                  self.onlineSearchSchema.structure,
                  request
                )
                // console.log("RESOLVED QUERY CONDITIONS:", inn( request.options.resolvedQueryConditions ) );

                // TODO: Document if it sticks
                self.manipulateQueryConditions(request, 'getQuery', function (err) {
                  if (err) return self._sendError(request, 'getQuery', next, err)

                  self.implementQuery(request, function (err, fullDocs, total, grandTotal) {
                    if (err) return self._sendError(request, 'getQuery', next, err)

                    // Make `total` and `grandTotal` part of the request, exposing them to all callbacks
                    request.data.fullDocs = fullDocs
                    request.data.total = total
                    request.data.grandTotal = grandTotal

                    self.afterDbOperation(request, 'getQuery', function (err) {
                      if (err) return self._sendError(request, 'getQuery', next, err)

                      self._extrapolateDocProxyAndprepareBeforeSendProxyAll(request, 'getQuery', request.data.fullDocs, function (err, docs, preparedDocs) {
                        if (err) return self._sendError(request, 'getQuery', next, err)

                        request.data.docs = docs
                        request.data.preparedDocs = preparedDocs

                        self.afterEverything(request, 'getQuery', function (err) {
                          if (err) return self._sendError(request, 'getQuery', next, err)

                          // Remote request: set headers, and send the doc back (if echo is on)
                          if (request.remote) {
                            self.sendData(request, 'getQuery', request.data.preparedDocs)
                          // Local request: simply return the doc to the asking function
                          } else {
                            next(null, request.data.preparedDocs, request)
                          }
                        })
                      })
                    })
                  })
                })
              })
            })
          })
        })
      })
    })
  }

  _makeGet (request, next) {
    var self = this

    if (typeof (next) !== 'function') next = function () {}

    // Prepare request.data
    request.data = request.data || {}

    // Check that the method is implemented
    if (!self.handleGet && request.remote) {
      return self._sendError(request, 'get', next, new self.NotImplementedError())
    }

    // Check the IDs
    self._checkParamIds(request, false, function (err) {
      if (err) return self._sendError(request, 'get', next, err)

      self._doAutoLookup(request, 'get', function (err) {
        if (err) return self._sendError(request, 'post', next, err)

        // Fetch the doc.
        self.implementFetchOne(request, function (err, fullDoc) {
          if (err) return self._sendError(request, 'get', next, err)

          if (!fullDoc) return self._sendError(request, 'get', next, new self.NotFoundError())

          request.data.fullDoc = fullDoc

          self.afterDbOperation(request, 'get', function (err) {
            if (err) return self._sendError(request, 'get', next, err)

            self.extrapolateDocProxy(request, 'get', fullDoc, function (err, doc) {
              if (err) return self._sendError(request, 'get', next, err)

              request.data.doc = doc

              // Check the permissions
              self._checkPermissionsProxy(request, 'get', function (err, granted, message) {
                if (err) return self._sendError(request, 'get', next, err)

                if (!granted) return self._sendError(request, 'get', next, new self.ForbiddenError(message))

                self.afterCheckPermissions(request, 'get', function (err) {
                  if (err) return self._sendError(request, 'get', next, err)

                  // "preparing" the doc. The same function is used by GET for collections
                  self.prepareBeforeSendProxy(request, 'get', doc, function (err, preparedDoc) {
                    if (err) return self._sendError(request, 'get', next, err)

                    request.data.preparedDoc = preparedDoc

                    // Just in case: clean up any field that returned from the schema, and shouldn't have been
                    // there in the first place
                    self.schema.cleanup(preparedDoc, 'doNotSave')

                    self.afterEverything(request, 'get', function (err) {
                      if (err) return self._sendError(request, 'get', next, err)

                      // Manipulate preparedDoc: at this point, it's the WHOLE database record,
                      // whereas I only want returned paramIds AND the piggyField
                      // if( request.options.field ){
                      //  for( var field in preparedDoc ){
                      //    if( ! self.paramIds[ field ] && field != request.options.field ) delete preparedDoc[ field ];
                      //  }
                      // }

                      // Remote request: set headers, and send the doc back
                      if (request.remote) {
                        // Send "prepared" doc
                        self.sendData(request, 'get', preparedDoc)

                        // Local request: simply return the doc to the asking function
                      } else {
                        next(null, preparedDoc, request)
                      }
                    })
                  })
                })
              })
            })
          })
        })
      })
    })
  }

  _makeDelete (request, next) {
    var self = this

    if (typeof (next) !== 'function') next = function () {}

    // Prepare request.data
    request.data = request.data || {}

    // Check that the method is implemented
    if (!self.handleDelete && request.remote) {
      return self._sendError(request, 'delete', next, new self.NotImplementedError())
    }

    // Check the IDs
    self._checkParamIds(request, false, function (err) {
      if (err) return self._sendError(request, 'delete', next, err)

      self._doAutoLookup(request, 'delete', function (err) {
        if (err) return self._sendError(request, 'post', next, err)

        // Fetch the doc.
        self.implementFetchOne(request, function (err, fullDoc) {
          if (err) return self._sendError(request, 'delete', next, err)

          if (!fullDoc) return self._sendError(request, 'delete', next, new self.NotFoundError())

          request.data.fullDoc = fullDoc

          self.extrapolateDocProxy(request, 'delete', fullDoc, function (err, doc) {
            if (err) return self._sendError(request, 'delete', next, err)

            request.data.doc = doc

            // Check the permissions
            self._checkPermissionsProxy(request, 'delete', function (err, granted, message) {
              if (err) return self._sendError(request, 'delete', next, err)

              if (!granted) return self._sendError(request, 'delete', next, new self.ForbiddenError(message))

              self.afterCheckPermissions(request, 'delete', function (err) {
                if (err) return self._sendError(request, 'delete', next, err)

                // Actually delete the document
                self.implementDelete(request, function (err, deletedRecord) {
                  if (err) return self._sendError(request, 'delete', next, err)

                  // If nothing was returned, we have a problem: the record wasn't found (it
                  // must have disappeared between implementFetchOne() above and now)
                  if (!deletedRecord) return self._sendError(request, 'delete', next, new Error("Error deleting a record in 'delete`: record to be deleted not found"))

                  self.afterDbOperation(request, 'delete', function (err) {
                    if (err) return self._sendError(request, 'delete', next, err)

                    self.prepareBeforeSendProxy(request, 'delete', doc, function (err, preparedDoc) {
                      if (err) return self._sendError(request, 'delete', next, err)

                      request.data.preparedDoc = preparedDoc

                      self.afterEverything(request, 'delete', function (err) {
                        if (err) return self._sendError(request, 'delete', next, err)

                        if (request.remote) {
                          if (self.echoAfterDelete) {
                            self.sendData(request, 'delete', preparedDoc)
                          } else {
                            self.sendData(request, 'delete', '')
                          }
                        } else {
                          next(null, doc, request)
                        }
                      })
                    })
                  })
                })
              })
            })
          })
        })
      })
    })
  }

  apiGetQuery (options, next) {
    // Make up the request
    var request = new Object()

    request.remote = false
    request.body = {}
    if (options.apiParams) request.params = options.apiParams
    else request.params = {}

    request.session = options.session || {}
    request.options = this._co(options)
    request.options.delete = request.options.delete || !!this.deleteAfterGetQuery

    // Actually run the request
    this._makeGetQuery(request, next)
  }

  apiGet (id, options, next) {
    // Make `options` argument optional
    var len = arguments.length

    if (len == 2) { next = options; options = {} };

    var request = new Object()

    request.remote = false
    request.options = options
    request.body = {}
    if (options.apiParams) request.params = options.apiParams
    else { request.params = {}; request.params[ this.idProperty ] = id }
    request.session = options.session || {}

    // Actually run the request
    this._makeGet(request, next)
  }

  apiPut (body, options, next) {
    // This will only work if this.idProperty is included in the body object
    if (typeof (body[ this.idProperty ]) === 'undefined') {
      throw (new Error('When calling Store.apiPut with an ID of null, id MUST be in body'))
    }

    // Make `options` argument optional
    var len = arguments.length
    if (len == 2) { next = options; options = {} };

    // Make up the request
    var request = new Object()
    request.remote = false
    request.options = options
    request.body = this._co(body)
    if (options.apiParams) request.params = options.apiParams
    else { request.params = {}; request.params[ this.idProperty ] = body[ this.idProperty ] }
    request.session = options.session || {}

    delete request.body._children

    // Actually run the request
    this._makePut(request, next)
  }

  apiPost (body, options, next) {
    // Make `options` argument optional
    var len = arguments.length
    if (len == 2) { next = options; options = {} };

    // Make up the request
    var request = new Object()
    request.remote = false
    request.options = options
    request.params = options.apiParams || {}
    request.session = options.session || {}
    request.body = this._co(body)

    delete request.body._children

    // Actually run the request
    this._makePost(request, next)
  }

  apiDelete (id, options, next) {
    // Make `options` argument optional
    var len = arguments.length
    if (len === 2) { next = options; options = {} };

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

// Get store from the class' registry
Store.getStore = function (storeName) {
  return Store.registry[ storeName ]
}

// Delete the store from the class' registry
Store.deleteStore = function (storeName) {
  delete Store.registry[ storeName ]
}

// Get all stores as a hash
Store.getAllStores = function () {
  return Store.registry
}

// Initialise all stores, running their .init() function
Store.init = function () {
  Object.keys(Store.registry).forEach(function (key) {
    var store = Store.registry[ key ]
    store.init()
  })
}

exports = module.exports = Store

/* Store's own "class" variables */
Store.artificialDelay = 0
Store.registry = {}

// Embed important mixins so that they are available
// without an extra require (they are VERY common)
Store.SimpleDbLayerMixin = SimpleDbLayerMixin
Store.HTTPMixin = HTTPMixin
