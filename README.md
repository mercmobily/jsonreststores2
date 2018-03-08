JsonRestStores TWO
==================

JsonRestStore2 (just JsonRestStores from here on) is a rewrite of [JsonRestStores](https://github.com/mercmobily/jsonreststores)

JsonRestStores is the best way to create **self-documenting** (coming back soon) REST stores that return JSON data

Rundown of features:

* **DRY approach**. Create complex applications keeping your code short and tight, without repeating yourself.
* **Down-to-earth**. It does what developers _actually_ need, using existing technologies.
* **Database-agnostic**. When creating a store, all you have to do is write the essential queries to fetch/modify data.
* **Protocol-agnostic**. HTTP is only one of the possible protocols.
* **Schema based**. Anything coming from the client will be validated and cast to the right type.
* **File uploads**. It automatically supports file uploads, where the field will be the file's path
* **API-ready**. Every store function can be called via API, which bypass permissions constraints
* **Tons of hooks**. You can hook yourself to every step of the store processing process
* **Authentication hooks**. Only implement things once, and keep authentication tight and right.
* **Mixin-based**. You can add functionality easily.
* **Inheriting stores**. You can easily derive a store from another one.
* **Great documentation**. (Work in progress after the rewrite)

# Introduction to (JSON) REST stores

Here is an introduction on REST, JSON REST, and this module. If you are a veteran of REST stores, you can probably just skim through this.

## Implementing REST stores

Imagine that you have a web application with bookings, and users connected to each booking, and that you want to make this information available via a JSON Rest API. You would have to define the following routes in your application:

* `GET /bookings/`
* `GET /bookings/:bookingId`
* `PUT /bookings/:bookingId`
* `POST /bookings/`
* `DELETE /bookings:/bookingId`

And then to access users for that booking:

* `GET /bookings/:bookingId/users/`
* `GET /bookings/:bookingId/users/:userId`
* `PUT /bookings/:bookingId/users/:userId`
* `POST /bookings/:bookingId/users/`
* `DELETE /bookings/:bookingId/users/:userId`

And then -- again -- to get/post data from individual fields with one call:

* `GET /bookings/:bookingId/users/:userId/field1`
* `PUT /bookings/:bookingId/users/:userId/field1`
* `GET /bookings/:bookingId/users/:userId/field2`
* `PUT /bookings/:bookingId/users/:userId/field2`
* `GET /bookings/:bookingId/users/:userId/field3`
* `PUT /bookings/:bookingId/users/:userId/field3`

It sounds simple enough (although it's only two tables and it already looks rather boring). It gets tricky when you consider that:

* You need to make sure that permissions are always carefully checked. For example, only users that are part of booking `1234` can `GET /bookings/1234/users`
* When implementing `GET /bookings/`, you need to parse the URL in order to enable data filtering (for example, `GET /bookings?dateFrom=1976-01-10&name=Tony` will need to filter, on the database, all bookings made after the 10th of January 1976 by Tony).
* When implementing `GET /bookings/`, you need to return the right `Content-Range` HTTP headers in your results so that the clients know what range they are getting.
* When implementing `GET /bookings/`, you also need to make sure you take into account any `Range` header set by the client, which might only want to receive a subset of the data
* When implementing `/bookings/:bookingId/users/:userId`, you need to make sure that the bookingId exists
* With `POST` and `PUT`, you need to make sure that data is validated against some kind of schema, and return the appropriate errors if it's not.
* With `PUT`, you need to consider the HTTP headers `If-match` and `If-none-match` to see if you can//should//must overwrite existing records
* All unimplemented methods should return a `501 Unimplemented Method` server response
* You need to implement all of the routes for individual fields, and add permissions

This is only a short list of obvious things: there are many more to consider. The point is, when you make a store you should be focusing on the important parts (the data you gather and manipulate, and permission checking) rather than repetitive, boilerplate code.

With JsonRestStores, you can create JSON REST stores without ever worrying about any one of those things. You can concentrate on what _really_ matters: your application's data, permissions and logic.

If you are new to REST and web stores, you will probably benefit by reading a couple of important articles. Understanding the concepts behind REST stores will make your life easier. I suggest you read [John Calcote's article about REST, PUT, POST, etc.](http://jcalcote.wordpress.com/2008/10/16/put-or-post-the-rest-of-the-story/). It's a fantastic read, and I realised that it was written by John, who is a long term colleague and fellow writer at Free Software Magazine, only after posting this link here!

# Dependencies overview

Jsonreststores is a module that creates managed routes for you, and integrates very easily with existing ExpressJS applications.

Here is a list of modules used by JsonRestStores. You should be at least slightly familiar with them.

* [SimpleSchema - Github](https://github.com/mercmobily/SimpleSchema2). This module makes it easy (and I mean, really easy) to define a schema and validate/cast data against it. It's really simple to extend a schema as well. It's a no-fuss module.

* [Allhttperrors](https://npmjs.org/package/allhttperrors). A simple module that creats `Error` objects for all of the possible HTTP statuses.

Note that all of these modules are fully unit-tested, and are written and maintained by me.

# Your first Json REST store

Creating a store with JsonRestStores is very simple. Here is code that can be plugged directtly into an Express application:

    var Store = require('jsonreststores2') // The main JsonRestStores module
    var HTTPMixin = require('jsonreststores2/HTTPMixin') // HTTP Mixin
    var Schema = require('simpleschema2')  // The main schema module

    // This is for MySql
    var promisify = require('util').promisify
    var mysql = require('mysql')

    // ======= BEGIN: Normally in separate file in production =============
    // Create MySql connection. NOTE: in production this will likely be placed
    // in a separate file, so that `connection` can be required and shared...
    var connection = mysql.createPool({
      host: 'localhost',
      user: 'user',
      password: 'YOUR PASSOWORD',
      database: 'testing',
      port: 3306
    })
    // Promisified versions of the query function for MySql
    connection.queryP = promisify(connection.query)
    // ======= END: Normally in separate file in production =============

    // Make up an object that will contain all of the stores
    var stores = {}

    // Basic definition of the managers store
    class Managers extends HTTPMixin(Store) {
      static get schema () {
        return new Schema({
          name: { type: 'string', trim: 60 },
          surname: { type: 'string', searchable: true, trim: 60 }
        })
      }

      static get storeName () { return 'managers' }
      static get publicURL () { return '/managers/:id' }

      static get handlePut () { return true }
      static get handlePost () { return true }
      static get handleGet () { return true }
      static get handleGetQuery () { return true }
      static get handleDelete () { return true }

      async implementFetchOne (request) {
        return (await connection.queryP('SELECT * FROM managers WHERE id = ?', request.params.id))[0]
      }

      async implementInsert (request) {
        let insertResults = await connection.queryP('INSERT INTO managers SET ?', request.body)
        let selectResults = await connection.queryP('SELECT * FROM managers WHERE id = ?', insertResults.insertId)
        return selectResults[0] || null
      }

      async implementUpdate (request) {
        await connection.queryP('UPDATE managers SET ? WHERE id = ?', [request.body, request.params.id])
        return (await connection.queryP('SELECT * FROM managers WHERE id = ?', request.params.id))[0]
      }

      async implementDelete (request) {
        let record = (await connection.queryP('SELECT * FROM managers WHERE id = ?', request.params.id))[0]
        await connection.queryP('DELETE FROM managers WHERE id = ?', [request.params.id])
        return record
      }

      async implementQuery (request) {
        var result = await connection.queryP(`SELECT * FROM managers LIMIT ?,?`, [ request.options.ranges.skip, request.options.ranges.limit ])
        var grandTotal = await connection.query(`COUNT(*) FROM managers`)
        return { data: result, grandTotal: grandTotal }
      }
    }

    stores.managers = new Managers()
    exports = module.exports = stores

In app.js, after `app.use(express.static)`, you would have:

    var stores = require('./stores.js');
    stores.managers.protocolListenHTTP({app: app});

You will also need to create your MySql table in the 'testing' database:

    CREATE TABLE managers (
      id INT(10) PRIMARY KEY NOT NULL AUTO_INCREMENT,
      name VARCHAR(60),
      surname VARCHAR(60)
    );

That's it: this is enough to add, to your Express application, a a full store which will handly properly all of the HTTP calls.
Note that the database side of things has no sanity check: that's because sanity checking has _already_ happened at this stage.

Also note that this setup will query a MySql database; however, _anything_ can be the source of data.


Note that:

* `Managers` is a new constructor function that inherits from `Store` (the main constructor for JSON REST stores) mixed in with `HTTPMixin` (which implements `protocolListenHTTP()`
* `schema` is an object of type Schema that will define what's acceptable in a REST call.
* `publicURL` is the URL the store is reachable at. ***The last one ID is the most important one***: the last ID in `publicURL` (in this case it's also the only one: `id`) defines which field, within your schema, will be used as _the_ record ID when performing a PUT and a GET (both of which require a specific ID to function).
* `storeName` (_mandatory_) needs to be a unique name for your store.
* `handleXXX` are attributes which will define how your store will behave. If you have `handlePut: false` and a client tries to PUT, they will receive an `NotImplemented` HTTP error.
* `protocolListen( 'HTTP', { app: app } )` creates the right Express routes to receive HTTP connections for the `GET`, `PUT`, `POST` and `DELETE` methods.

## The store in action

A bit of testing with `curl`:

    $ curl -i -XGET  http://localhost:3000/managers/
    HTTP/1.1 200 OK
    X-Powered-By: Express
    Content-Type: application/json; charset=utf-8
    Content-Length: 2
    ETag: "223132457"
    Date: Mon, 02 Dec 2013 02:20:21 GMT
    Connection: keep-alive

    []

    curl -i -X POST -d "name=Tony&surname=Mobily"  http://localhost:3000/managers/
    HTTP/1.1 201 Created
    X-Powered-By: Express
    Location: /managers/2
    Content-Type: application/json; charset=utf-8
    Content-Length: 54
    Date: Mon, 02 Dec 2013 02:21:17 GMT
    Connection: keep-alive

    {
      "id": 2,
      "name": "Tony",
      "surname": "Mobily"
    }

    curl -i -X POST -d "name=Chiara&surname=Mobily"  http://localhost:3000/managers/
    HTTP/1.1 201 Created
    X-Powered-By: Express
    Location: /managers/4
    Content-Type: application/json; charset=utf-8
    Content-Length: 54
    Date: Mon, 02 Dec 2013 02:21:17 GMT
    Connection: keep-alive

    {
      "id": 4,
      "name": "Chiara",
      "surname": "Mobily"
    }

    $ curl -i -GET  http://localhost:3000/managers/
    HTTP/1.1 200 OK
    X-Powered-By: Express
    Content-Type: application/json; charset=utf-8
    Content-Length: 136
    ETag: "1058662527"
    Date: Mon, 02 Dec 2013 02:22:29 GMT
    Connection: keep-alive

    [
      {
        "id": 2,
        "name": "Tony",
        "surname": "Mobily"
      },
      {
        "id": 4,
        "name": "Chiara",
        "surname": "Mobily"
      }
    ]


    $ curl -i -X PUT -d "name=Merc&surname=Mobily"  http://localhost:3000/managers/2
    HTTP/1.1 200 OK
    X-Powered-By: Express
    Location: /managers/2
    Content-Type: application/json; charset=utf-8
    Content-Length: 54
    Date: Mon, 02 Dec 2013 02:23:50 GMT
    Connection: keep-alive

    {
      "id": 2,
      "name": "Merc",
      "surname": "Mobily"
    }

    $ curl -i -XGET  http://localhost:3000/managers/2
    HTTP/1.1 200 OK
    X-Powered-By: Express
    Content-Type: application/json; charset=utf-8
    Content-Length: 54
    ETag: "-264833935"
    Date: Mon, 02 Dec 2013 02:24:58 GMT
    Connection: keep-alive

    {
      "id": 2,
      "name": "Merc",
      "surname": "Mobily"
    }

It all works!

# NOTE: DOCUMENTATION UPDATED TO THIS POINT

# Implementing data-oriented calls

Data-oriented calls are the bridge between your stores and your actual data. Data can be stored in a database, text files, or even remotely.

Here is the list of calls that must be implemented -- and which methods need them:

 * `implementFetchOne(request)`. Required for methods `put`, `get`, `delete`.
 * `implementInsert(request)`. Required for methods `post` and `put` (new records)
 * `implementUpdate(request)`. Required for method `put` (existing records)
 * `implementDelete(request`. Required for method `delete`.
 * `implementQuery(request)`. Required for methods `getQuery`.

Looking it from a different perspective, here are the `implement***` methods you will need to implement for each method to work properly:

 * `get`: `implementFetchOne()`.
 * `getQuery`: `implementQuery()`
 * `put`: `implementFetchOne()`, `implementInsert()`, `implementUpdate()`
 * `post`: `implementInsert()`
 * `delete`: `implementDelete()`

When developing these methods, it's important to make sure that they function exactly as expected.

## `implementFetchOne(request)`

This method is used to fetch a single record from the data source. The attributes taken into consideration in `request` are:

* `request.remote`.
* `request.params`. Sets the filter to search for the record to be fetched. For local request, it's acceptable to just match the key in `request.params` matching `self.idProperty`. For remote requests, it's best to filter data so that every key in `request.params` matches the equivalent key in the record.

The callback `cb()` must be called with the fetched element as its second parameter, or `null` if a match wasn't found.

## `implementInsert(request)`

This method is used to add a single record to the data source. No attribute is taken into consideration in `request`.

If `forceId` is set, then a shallow copy of the record should be made (with `this._co()`) and the object's `idProperty` key should be forced to be `forceId`.

The callback `cb()` must be called with the record once written on the data source.

## `implementUpdate(request)`

This method is used to update a single record in the data source. The attributes taken into consideration in `request` are:

* `request.remote`.
* `request.params`. Sets the filter to search for the record to be updated. For local request, it's acceptable to just match the key in `request.params` matching `self.idProperty`. For remote requests, it's best to filter data so that every key in `request.params` matches the equivalent key in the record.
* `request.body`. The fields to be updated in the data source.

If `deleteUnsetFields` is set to `true`, then all fields that are not set in `request.body` must be deleted from the matched record in the data source.

The callback `cb()` must be called with the updated element as its second parameter, or `null` if a match wasn't found.

## `implementDelete(request)`

This method is used to delete a single record from the data source. The attributes taken into consideration in `request` are:

* `request.remote`.
* `request.params`. Sets the filter to search for the record to be deleted. For local request, it's acceptable to just match the key in `request.params` matching `self.idProperty`. For remote requests, it's best to filter data so that every key in `request.params` matches the equivalent key in the record.

The callback `cb()` must be called with the fetched element as its second parameter, or `null` if a match wasn't found.

## `implementQuery(request)`

This method is used to fetch a set of records from the data source. The attributes taken into consideration in `request` are:

* `request.remote`.
* `request.params`. For remote requests, adds filtering restrictions to the query so that only matching records will be fetched. For local request, such extra filtering should be avoided.
* `request.options`. The options object that will define what data is to be fetched. Specifically:
  * `request.options.conditions`: an object specifying key/value criteria to be applied for filtering
  * `request.options.sort`: an object specifying how sorting should happen. For example `{ surname: -1, age: 1  }`.
  * `request.options.range`: an object with up to attributes:
    * `limit` must make sure that only a limited number of records are fetched;
    * `skip` must make sure that a number of records are skipped.
  * `request.options.delete`: if set to `true`, each fetched record will be deleted after fetching. The default is the store's own `self.deleteAfterGetQuery` attribute (which is itself `false` by default).
  * `request.options.skipHardLimitOnQueries`: if set to `true`, the limitation set by the store's own attribute `hardLimitOnQueries` will not be applied.

## Custom `searchSchema`

In JsonRestStores you actually define what fields are acceptable as filters with the parameter `searchSchema`, which is defined exactly as a schema. So, writing this is equivalent to the code just above:

    var Managers = declare( Store, {

      schema: new Schema({
        name   : { type: 'string', trim: 60 },
        surname: { type: 'string', searchable: true, trim: 60 },
      }),

      onlineSearchSchema: new Schema( {
        surname: { type: 'string', trim: 60 },
      }),

      storeName: 'managers',
      publicURL: '/managers/:id',

      handlePut: true,
      handlePost: true,
      handleGet: true,
      handleGetQuery: true,
      handleDelete: true,
    });

    var managers = new Managers();

    JsonRestStores.init();
    managers.protocolListen( 'HTTP', { app: app } );;

If `searchSchema` is not defined, JsonRestStores will create one based on your main schema by doing a shallow copy, excluding `paramIds` (which means that, in this case, `id` is not added automatically to `searchSchema`, which is most likely what you want).

If you define your own `searchSchema`, you are able to decide exactly how you want to filter the values. For example you could define a different default, or trim value, etc. However, in common applications you can probably live with the auto-generated `onlineSearchSchema`.


# Naming conventions for stores

It's important to be consistent in naming conventions while creating stores. In this case, code is clearer than a thousand bullet points:

## Naming convertions for simple stores

    var Managers = declare( Store, {

      schema: new Schema({
        // ...
      });

      publicUrl: '/managers/:id',

      storeName: `managers`
      // ...
    }
    var managers = new Managers();

    var People = declare( Store, {

      schema: new Schema({
        // ...
      });

      publicUrl: '/people/:id',

      storeName: `people`
      // ...
    }
    var people = new People();

    JsonRestStores.init();
    managers.protocolListen( 'HTTP', { app: app } );;
    people.protocolListen( 'HTTP', { app: app } );;

* Store names anywhere lowercase and are plural (they are collections representing multiple entries)
* Store constructors (derived from `Store`) are in capital letters (as constructors, they should be)
* Store variables are in small letters (they are normal object variables)
* `storeName` attributes are in small letters (to follow the lead of variables)
* URL are in small letters (following the stores' names, since everybody knows that `/Capital/Urls/Are/Silly`)

# Permissions

By default, everything is allowed: stores allow pretty much anything and anything; anybody can DELETE, PUT, POST, etc. Furtunately, JsonRestStores allows you to decide exactly what is allowed and what isn't, by overriding specific methods.

Every method runs the method `checkPermissions()` before continuing. If everything went fine, `checkPermissions()` will return `true`; if it returns `false`, along with a message, it means that permission wasn't granted.

The `checkPermissions()` method has the following signature:

    checkPermissions: function( request, method)

Here:

* `request`. It is the request object
* `method`. It can be `post`, `put`, `get`, `getQuery`, `delete`

Here is an example of a store only allowing deletion only to specific admin users:

    // The basic schema for the WorkspaceUsers table
    var WorkspaceUsers = declare( Store, {

      schema: new Schema({
        email     :  { type: 'string', trim: 128, searchable: true, sortable: true  },
        name      :  { type: 'string', trim: 60, searchable: true, sortable: true  },
      }),

      storeName:  'WorkspaceUsers',
      publicURL: '/workspaces/:workspaceId/users/:id',

      handlePut: true,
      handlePost: true,
      handleGet: true,
      handleGetQuery: true,
      handleDelete: true,

      checkPermissions: function( request, method, cb ){

        // This will only affect `delete` methods
        if( method !== 'delete' ) return cb( null, true );

        // User is logged in: all good
        if( request._req.session.user ){ // ._
          cb( null, true );

        // User is not logged in: fail!
        } else {
          cb( null, false, "Must login" );
        }
      },

    });
    var workspaceUsers = new WorkspaceUsers();
    workspaceUsers.protocolListen( 'HTTP', { app: app } );;

Permission checking can be as simple, or as complex, as you need it to be.

Note that if your store is derived from another one, and you want to preserve your master store's permission model, you can run `super.checkPermissions()`:

      async checkPermissions (request, method) {

        // Run the parent's permission check. If it failed, honours the failure
        let { granted, message } = super.checkPermissions(request, method)
        if (!granted) return { granted, message }

        // We are only enriching for `put`.
        // In any other case, will go along with the parent's response
        if (method === 'put') return { granted, message }

        // User is admin (id: 1 )
        if( request.session.user === 1){ return { granted: true }
        else return { granted: false, message: 'Only admin can do this'}
      },

Please note that `checkPermissions()` is only run for local requests, with `remote` set to false. All requests coming from APIs will ignore the method.


## A note on `publicURL` and `paramIds`

When you define a store like this:

    var Managers = declare( Store, {

      schema: new Schema({
        name   : { type: 'string', trim: 60 },
        surname: { type: 'string', trim: 60 },
      }),

      storeName: 'managers',
      publicURL: '/managers/:id',

      handlePut: true,
      handlePost: true,
      handleGet: true,
      handleGetQuery: true,
      handleDelete: true,

      hardLimitOnQueries: 50,
    });

    managers.protocolListen( 'HTTP', { app: app } );;

The `publicURL` is used to:

* Add `id: { type: id }` to the schema automatically. This is done so that you don't have to do the grunt work of defining `id` in the schema if they are already in `publicURL`.
* Create the `paramIds` array for the store. In this case, `paramIds` will be `[ 'id' ]`.

So, you could reach the same goal without `publicURL`:

    var Managers = declare( Store, {

      schema: new Schema({
        id     : { type: 'id' },
        name   : { type: 'string', trim: 60 },
        surname: { type: 'string', trim: 60 },
      }),

      storeName: 'managers',
      paramIds: [ 'id' ],

      handlePut: true,
      handlePost: true,
      handleGet: true,
      handleGetQuery: true,
      handleDelete: true,

      hardLimitOnQueries: 50,
    });

    var managers = new Managers();
    JsonRestStores.init();
    managers.protocolListen( 'HTTP', { app: app } );; // This will throw()

Note that:
 * The `id` parameter had to be defined in the schema
 * The `paramIds` array had to be defined by hand
 * `managers.protocolListen( 'HTTP', { app: app } );` can't be used as the public URL is not there

This pattern is much more verbose, and it doesn't allow the store to be placed online with `protocolListenHTTP()`.

In any case, the property `idProperty` is set as last element of `paramIds`; in this example, it is `id`.

In the documentation, I will often refers to `paramIds`, which is an array of element in the schema which match the ones in the route. However, in all examples I will declare stores using the "shortened" version.

# File uploads

JsonRestStores in itself doesn't manage file uploads. The reason is simple: file uploads are a _very_ protocol-specific feature. For example, you can decide to upload files, along with your form, by using `multipart-formdata` as your `Content-type`, to instruct the browser to encode the information (before sending it to the sever) in a specific way that accommodates multiple file uploads. However, this is a separate issue to the store itself, which will only ever store the file's _name_, rather than the raw data.

Basicaly, all of the features to uplaod files in JsonRestStores are packed in HTTPMixin. This is how you do it:

    var VideoResources = declare( [ Store ], {

      schema: new HotSchema({
        fileName         : { default: '', type: 'string', protected: true, singleField: true, trim: 128, searchable: false },
        description      : { default: '', type: 'string', trim: 1024, searchable: false },
      }),

      uploadFields: {
        fileName: {
          destination: '/home/www/node/deployed/node_modules/wonder-server/public/resources',
        },
      },

      storeName:  'videoResources',

      publicURL: '/videosResources/:id',
      hotExpose: true,

      handlePut: true,
      handlePost: true,
      handleGet: true,

    });
    stores.videoTemplatesResources = new VideoTemplatesResources();

In this store:

* The store has an `uploadFields` attribute, which lists the fields that will represent file paths resulting from the successful upload
* The field is marked as `protected`; remember that the field represents the file's path, and that you don't want users to directly change it.

For this store, HTTPMixin will do the following:

* add a middleware in stores with `uploadFields`, adding the ability to parse `multipart/formdata` input from the client
* save the files in the required location
* set `req.body.fileName` as the file's path, and `req.bodyComputed.fileName` to true.

JsonRestStores will simply see values in `req.body`, blissfully unaware of the work done to parse the requests' input and the work to automatically populate `req.body` with the form values as well as the file paths.

The fact that the fields are protected meand that you are _not_ forced to re-upload files every time you submit the form: if the values are set (thanks to an upload), they will change; if they are not set, they will be left "as is".

On the backend, JsonRestStores uses `multer`, a powerful multipart-parsing module. However, this is basically transparent to JsonRestStores and to developers, except some familiarity with the configuration functions.

## Configuring file uploads

The store above only covers a limited use case. Remembering that the `file` object has the following fields:

* `fieldname` - Field name specified in the form
* `originalname` - Name of the file on the user's computer
* `encoding` - Encoding type of the file
* `mimetype` - Mime type of the file
* `size` - Size of the file in bytes
* `destination` - The folder to which the file has been saved
* `filename` - The name of the file within the destination
* `path` - The full path to the uploaded file


In order to configure file uploads, you can set three attributes when you declare you store:

* `uploadFilter` -- to filter incoming files based on their names or fieldName
* `uploadLimits` -- to set some upload limits, after which JsonRestStores will throw a `UnprocessableEntity` error.
* `uploadFields` -- it can have two properties: `destination` and `fileName`.

Here are these options in detail:

### `uploadFilter`

This allows you to filter files based on their names and `fieldName`s. You only have limited amount of information for each file:

    { fieldname: 'fileName',
      originalname: 'test.mp4',
      encoding: '7bit',
      mimetype: 'video/mp4' }

This is especially useful if you want to check for swearwords in the file name, or the file type. You can throw and error if the file type doesn't correspond to what you were expecting:

    uploadFilter: function( req, file, cb ){
      if( file.mimetype != 'video/mp4') return cb( null, false );
      cb( null, true );
    },

### `uploadLimits`

This allows you to set specific download limits. The list comes straight from `busbuy`, on which JsonRestStores is based:

* `fieldNameSize` -- Max field name size (in bytes) (Default: 100 bytes).
* `fieldSize` -- Max field value size (in bytes) (Default: 1MB).
* `fields` -- Max number of non-file fields (Default: Infinity).
* `fileSize` -- For multipart forms, the max file size (in bytes) (Default: Infinity).
* `files` -- For multipart forms, the max number of file fields (Default: Infinity).
* `parts` -- For multipart forms, the max number of parts (fields + files) (Default: Infinity).
* `headerPairs` -- For multipart forms, the max number of header key=>value pairs to parse Default: 2000 (same as node's http).

For example, the most typical use case would be:

    uploadLimits: {
      fileSize: 50000000 // 50 Mb
    },

### `uploadFields`

This is the heart of the upload abilities of JsonRestStores.

It accepts two parameters:

#### `destination`

This parameter is mandatory, and defines where the files connected to that field will be stored. It can either be a string, or a function with the following signature: `function( req, file, cb )`. It will need to call the callback `cb` with `cb( null, FULL_PATH )`. For example:

    uploadFields: {

      avatarImage: {
        destination: function (req, file, cb) {
          // This can depend on req, or file's attribute
          cb( null, '/tmp/my-uploads');
        }
      }

    },

#### `fileName`

It's a function that will determine the file name. By default, it will be a function that works out the file name from the field name and either the record's ID (for PUT requests, where the ID is known) or a random string (for POST requests, where the ID is not known).

If you don't set it, it will be:

uploadFields: {

      avatarImage: {
        destination: function (req, file, cb) {
          // This can depend on req, or file's attribute
          cb( null, '/tmp/my-uploads');
        },

        // If the ID is there (that's the case with a PUT), then use it. Otherwise,
        // simply generate a random string
        fileName: function( req, file, cb ){
          var id = req.params[ this.idProperty ];
          if( ! id ) id = crypto.randomBytes( 20 ).toString( 'hex' );

          // That's it
          return cb( null, file.fieldname + '_' + id ); // ._
        }
      }
    },

The default function works fine in most cases. However, you may want to change it.

#### `uploadErrorProcessor`

By default, when there is an error, the file upload module `multer` will throw and error. It's  much better to encapsulate those errors in HTTP errors. This is what uploadErrorProcessor does. By default, it's defined as follow (although you can definitely change it if needed):

    uploadErrorProcessor: function( err, next ){
      var ReturnedError = new this.UnprocessableEntityError( (err.field ? err.field : '' ) + ": " + err.message );
      ReturnedError.OriginalError = err;
      return next( ReturnedError );
    },


# `hardLimitOnQueries`: limit the number of records

If your store has the `hardLimitOnQueries` set, any `getQuery` method (that is, a `GET` without the final `id`, and therefore fetching elements based on a filter) will never return more than `hardLimitOnQueries` results (unless you are using JsonRestStore's API, and manually set `options.skipHardLimitOnQueries` to `true`).

# Inheriting a store from another one


# Self-documetation

This feature hasn't yet been ported from Jsonreststores

# Errors returned and error management

JsonRestStores has very careful error management.

## The error objects

This is the comprehensive list of errors the class can create:

  * `BadRequestError` Like this: `new BadRequestError( { errors: errors } )`
  * `UnauthorizedError`
  * `ForbiddenError`
  * `NotFoundError`
  * `PreconditionFailedError`
  * `UnprocessableEntityError` Like this: `UnprocessableEntityError( { errors: errors } )`
  * `NotImplementedError`
  * `ServiceUnavailableError`. Like this: `ServiceUnavailableError( { originalErr: error } )`

These error constructors are borrowed from the [Allhttperrors](https://npmjs.org/package/allhttperrors) module -- you should its short and concise documentation. The short version is that `errorObject.httpError` will be set, and the constructor can have either a string or an object as parameters.

The error objects are all pretty standard. However:

* `ServiceUnavailableError` errors will be created with an `originalErr` parameter containing the original error object. For example if the database server goes down, the module will return a `ServiceUnavailableError` error object which will include the original MongoDB error in its `originalErr` parameter.

* `UnprocessableEntityError` and `BadRequestError` are both created when field validation fails. They error objects will always have an `errors` attribute, which will represent an array of errors as they were returned by SimpleSchema. For example:


    [
      { field: 'nameOfFieldsWithProblems', message: 'Message to the user for this field' },
      { field: 'nameOfAnotherField', message: 'Message to the user for this other field' },
    ]

JsonRestStores only ever throws (generic) Javascript errors if the class constructor was called incorrectly, or if an element in `paramIds` is not found within the schema. So, it will only ever happen if you use the module incorrectly. Any other case is chained through.

## Error management

At some point in your program, one of your callbacks might have the dreaded `err` first parameter set to an error rather than null. This might happen  with your database driver (for example your MongoDB process dies), or within your own module (validation after a `PUT` fails).

JsonRestStores allows you to decide what to do when this happens.

### `chainErrors`

You can control what happens when an error occurs with the `chainErrors` attribute. There are three options:

#### `all`

If you have `chainErrors: all` in your class definition: JsonRestStores will simply call `next( error )` where `error` is the error object. This means that it will be up to another Express middleware to deal with the problem.

#### `none`

If you have `chainErrors: none` in your class definition: if there is a problem, JsonRestStores will _not_ call the `next()` callback at all: it will respond to the client directly, after formatting it with the object's `self.formatErrorResponse()` method (see below).

Do this if you basically want to make absolute sure that every single request will end right there, whether it went well or not. If you do this, you might want to define your own `self.formatErrorResponse()` method for your store classes, so that the output is what you want it to be.

#### `nonhttp`

If you have `chainErrors: nonhttp` in your class definition, JsonRestStores will only call `next( err ) ` for non-HTTP errors --  any other problem (like your MongoDB server going down) will be handled by the next Express error management middleware. Use this if you want the server to respond directly in case of an HTTP problem (again using `self.formatErrorResponse()` to send a response to the client), but then you want to manage other problems (for example a MongoDB problem) with Express.

### `self.formatErrorResponse()`

In those cases where you decide to send a response to the client directly (with `chainErrors` being `none` or `nonhttp`), the server will send a response to the client directly. The body of the response will depend on what you want to send back.

The stock `self.formatErrorResponse()` method will simply return a Json representation of the error message and, if present, the `errors` array within the error.

### `self.logError()`

Whenever an error happens, JsonRestStore will run `self.logError()`. This happens regardless of what `self.chainErrors` contains (that is, whether the error is chained up to Express or it's managed internally). Note that there is no `callback` parameter to this method: since it's only a logging method, if it fails, it fails.


## The response

The reponse is delivered by JsonRestStores using the function `protocolSendHTTP` for HTTP transport, and `protocolSendXXX` for XXX transport. At the moment, only HTTP is implemented via the `HTTPMixin` mixin. `protocolSendXXX` has the following signature:

    protocolSendXXX( request, method, data, status, cb )

### Response in case everything went well

In case there was no error, the parameters are as follows:

* `request`: the request object
* `method`: the method being handled; it can be `get`, `getQuery`, `put`, `post`, `delete`,
* `data`: the data being delivered.
* `status`: the status set by JsonRestStores. For non-errors, the status is set as:
  * `get`: 200.
  * `getQuery`: 200
  * `put`: 201 for newly created records, and 200 for overwriting of existing records
  * `post`: 201
  * `delete`: 204

### Response in case of error

In case there was an error, the store will only send data if `chainError` is set to `none` or `nonhttp` (and the error isn't an HTTP error). Otherwise, it will simply call the callback with the error as its first parameter, as per normal in node (it will be up to your application to manage the error, which is simply pussed up the callback chain).

If chaining is off, and a response is to be sent to the client, then `protocolSendXXX` will be used. In this case, the `data` argument of `protocolSendXXX` will be set as the error itself, with two properties added to it:
  * `formattedErrorResponse` (the formatted error response, formatted thanks to the store's `formatErrorResponse()` method)
  * `originalMethod` (the method that triggered the error).

Also, `status` will be set to the HTTP status code.

_`protocolSendXXX()` is always passed an HTTP error._ In case the error was caused by something else (for example, the connection to the database dropped), the error will be set as 503, and the error object will also have a `originalErr` attribute which represents the original error that triggered the 503 HTTP error.

# Receiving HTTP requests and sending HTTP responses: `HTTPMixin`


# Store APIs

JsonRestStores allows you to run store methods from within your programs, rather than accessing them via URL. This is especially useful if you have a store and want to simulate an HTTP request within your own programs. Note that for database-backed methods you should use SimpleDbLaye methods (you can access the SimpleDbLayer table object in your store via `store.dbLayer`).

The API is really simple:

* `Store.apiGet( id, options, next( err, doc ) {})`
* `Store.apiGetQuery( options, next( err, queryDocs ){} )`
* `Store.apiPut(  body, options, next( err, doc ){} )`
* `Store.apiPost( body, options, next( err, doc ){} )`
* `Store.apiDelete( id, options, next( err, doc ){} )`

The `next()` call is the callback called at the end.

When using the API, the `options` object is especially important, as it defines how the API will work. When a request comes from a remote operation, the `options` object is populated depending on the requested URL and HTTP headers. When using the API, you need to popuate `options` manually in order to obtain what you desire. `options` is especially important while querying, as that's where you define what you filter and order the results by.

Since there is no HTTP connection to extrapolate options from, the `options` parameter in every API call is assigned directly to `request.options`. For all of the available options, refer to the [The options object](the-options-object) section in this guide.

All normal hooks are called when using these functions. However:

* Any check on `paramIds` is turned off: you are free to query a store without any pre-set automatic filtering imposed by `paramIds`. If your store has a `publicURL` of `/workspaces/:workspaceId/users/:id`, and you request `GET /workspaces/10/user/11`, in remote requests the `user` data source will be looked up based on _both_ `workspaceId` and `id`. In API (non-remote) requests, the lookup will only happen on `id`.
* `request.params` is automatically set to a hash object where the `idProperty` attribute matches the passed object's ID property. For example, for `{ id: 10, colour: 'red' }`, the `request.params` object is automatically set to  { `id: 10 }`. (This is true for all methods except `getQuery` and `post`, which don't accept objects with IDs). Note that you can pass `options.apiParams` to force `request.params` to whatever you like.
* All `store.handleXXX` properties are ignored: all methods will work
* The `request.remote` variable is set to false
* Permissions checking methods are not called at all: permission is always granted
* When the request is done, rather than ending data through using the transport of choice (for example HTTP), the `next()` callback is called with the results.


# Conclusion
