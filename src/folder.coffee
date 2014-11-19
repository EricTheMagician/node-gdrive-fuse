fs = require 'fs-extra'
Fiber = require 'fibers'
Future = require 'fibers/future'
rest = require 'restler'
pth = require 'path'
mmm = require('mmmagic')
winston = require 'winston'
hashmap = require('hashmap').HashMap
logger = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)({ level: 'info' }),
      new (winston.transports.File)({ filename: '/tmp/GDriveF4JS.log', level:'debug' })
    ]
  })


config = fs.readJSONSync 'config.json'
google = require 'googleapis'
oauth2Client = new google.auth.OAuth2(config.clientId, config.clientSecret, config.redirectUrl)
oauth2Client.setCredentials config.accessToken
google.options({ auth: oauth2Client, user: config.email })
drive = google.drive({ version: 'v2' })
refreshToken =  (cb) ->
  oauth2Client.refreshAccessToken (err,tokens) ->
    if err
      refreshToken(cb)
    else
      config.accessToken = tokens
      fs.outputJsonSync 'config.json', config
      cb()

######################################
########## Wrap functions ############
######################################

writeFile = Future.wrap(fs.writeFile)
open = Future.wrap(fs.open)
read = Future.wrap(fs.read,5)
write = Future.wrap fs.write
stat = Future.wrap(fs.stat)
writeFile = Future.wrap(fs.writeFile)

#since fs.exists does not return an error, wrap it using an error
exists = Future.wrap (path, cb) ->
  fs.exists path, (success)->
    cb(null,success)

close = Future.wrap (path,cb) ->
  fs.close path, (err) ->
    cb(err, true)

Magic = mmm.Magic;
magic = new Magic(mmm.MAGIC_MIME_TYPE)
detectFile = Future.wrap ( file,cb ) ->
  magic.detectFile(file, cb)

uploadTree = new hashmap()
uploadLocation = pth.join(config.cacheLocation, 'upload')
############################################
######### Upload Helper Functions ##########
############################################

uploadUrl = "https://www.googleapis.com/upload/drive/v2/files?uploadType=resumable"
rangeRegex =  ///
   ^(bytes)\=  #remove keyword bytes
   (\d*)        #finds the start
   \-         #finds the separator
   (\d*)$       #finds the end
  ///

getRangeEnd = (range) ->
  return parseInt(range.match(rangeRegex)[3])

_getNewRangeEnd = (location, fileSize, cb) ->
  rest.put location, {
    headers:
      "Authorization": "Bearer #{config.accessToken.access_token}"
      "Content-Length": 0
      "Content-Range": "bytes */#{fileSize}"
    }
  .on 'complete', (res, resp) ->
    if res instanceof Error
      cb(res)
    else
      if resp.statusCode == 401
        # console.log "refreshing access token"
        # console.log resp
        # console.log res.error.errors
        cb(null, -1)
        return null      
      
      #if the link is dead or bad
      if resp.statusCode == 404 or resp.statusCode == 410
        cb(null, -1)
        return null

      range = resp.headers.range
      unless range #sometimes, it doesn't return the range, so assume it is 0.
        range = resp.headers.Range
        unless range
          cb(null, -1)
          return null
      [start,end] = range.match(/(\d*)-(\d*)/)
      cb(null,parseInt(end))
getNewRangeEnd = Future.wrap _getNewRangeEnd
_getUploadResumableLink =  (parentId, fileName, fileSize, mime, cb) ->
  data =
    "parents": ["id": parentId]
    "title": fileName

  rest.postJson uploadUrl, data, {
    timeout: 300000
    headers:
      "Authorization": "Bearer #{config.accessToken.access_token}"
      "X-Upload-Content-Type": mime
      "X-Upload-Content-Length": fileSize
    }
  .on 'complete', (result, resp) ->
    if result instanceof Error
      cb(result)
    else
      if resp.statusCode == 401
        console.log "refreshing access token"
        fn = ->
          _getUploadResumableLink(parentId, fileName, fileSize, mime, cb)

        refreshToken(fn)
      else if resp.statusCode == 200
        cb(null, resp.headers.location)
      else
        # console.log resp.statusCode
        console.log resp.headers
        console.log resp.req._headers
        console.log result
        cb(resp.statusCode)
getUploadResumableLink = Future.wrap _getUploadResumableLink
_uploadData = (location, start, fileSize, mime, fd, buffer, cb) ->
  Fiber ->
    bytesRead = read(fd,buffer,0,buffer.length, start).wait()
    bytesRead =  bytesRead[0]
    end = start + bytesRead - 1
    rest.put location, {
      headers:
        "Authorization": "Bearer #{config.accessToken.access_token}"
        "Content-Length": bytesRead
        "Content-Range": "bytes #{start}-#{end}/#{fileSize}"
      data: buffer.slice(0,bytesRead)
    }
    .on 'complete', (res,resp) ->
      if res instanceof Error
        logger.debug "There was an error with uploading data, retrying"
        logger.debug "res", res
        logger.debug "resp", resp
        callback = (err,end) ->
          cb err, {
            statusCode: resp.statusCode
            rangeEnd: end
          }

         _getNewRangeEnd(location, fileSize, callback)
         return null        
      else
        if resp.statusCode == 400 or resp.statusCode == 401
          logger.debug "there was an error uploading data"
          cb resp.statusCode, {
            statusCode: resp.statusCode
            rangeEnd: 0
          }
          return null

        if resp.statusCode == 308 #success on resume
          md5Server = resp.headers["x-range-md5"]
          rangeEnd = getRangeEnd(resp.headers.range)
          cb null, {
            statusCode: 308
            rangeEnd: rangeEnd
          }
          return null

        if 200 <= resp.statusCode <= 201
          cb null, {
            statusCode: 201
            result: res
          }
          return null

        if resp.statusCode == 410
          logger.debug "got status code 410 while uploading"
          logger.debug "result", res
          logger.debug "response",resp
          
          cb err, {
            statusCode: resp.statusCode
            rangeEnd: end
          }


          return null


        if resp.statusCode >= 500
          callback = (err,end) ->
            cb err, {
              statusCode: resp.statusCode
              rangeEnd: end
            }


          end = _getNewRangeEnd(location, fileSize, callback)
 
          return null


        console.log "uncaugt state for file uploading"
        console.log resp.statusCode
        console.log resp.headers
        console.log res
        cb(resp.statusCode)



  .run()
uploadData = Future.wrap _uploadData

lockUploadTree = false
saveUploadTree = ->
  unless lockUploadTree
    lockUploadTree = true
    toSave = {}
    for key in uploadTree.keys()
      value = uploadTree.get key
      toSave[key] = value
    logger.debug "saving upload tree"
    fs.outputJsonSync pth.join(config.cacheLocation, 'data','uploadTree.json'), toSave
    lockUploadTree = false




######################################
######################################
######################################

class GFolder
  @uploadChunkSize: 1024*1024*16
  constructor: (@id, @parentid, @name, @ctime, @mtime, @permission, @children = []) ->

  getAttr: (cb) =>
    attr =
      mode: 0o40777,
      size: 4096 #standard size of a directory
      nlink: @children.length + 1,
      mtime: new Date(@mtime),
      ctime: new Date(@ctime)
    cb(0,attr)

  upload: (fileName, originalPath, cb) =>
    folder = @
    upFile = uploadTree.get originalPath
    filePath = pth.join uploadLocation, upFile.cache
    if upFile.uploading
      return null
    upFile.uploading = true   
    uploadTree.set originalPath, upFile 
    Fiber ->

      mime = detectFile(filePath).wait()
      fsize = stat(filePath).wait().size;
      if fsize == 0
        return null
      buffer = new Buffer(GFolder.uploadChunkSize)

      if upFile.location
        location = upFile.location
        try
          end = getNewRangeEnd(location, fsize).wait()
          if end <= 0
            delete upFile.location
            logger.debug "tried to get new range for #{originalPath}, but it was #{end}"            
          else
            start = end + 1
            logger.debug "got new range end for #{originalPath}: #{end}"
        catch e
          logger.debug "tried to get new range for #{originalPath} but there was an error"
          logger.debug e
          delete upFile.location
        
      
      unless upFile.location
        logger.log 'debug', "getting upload link to upload #{fileName}"
        location = getUploadResumableLink( folder.id, fileName, fsize, mime ).wait()
        upFile.location = location
        uploadTree.set originalPath, upFile 
        saveUploadTree()
        start = 0


      logger.log 'debug', "starting to upload file #{fileName}"
      fd = open(filePath, 'r').wait()

      while start < fsize
        result = uploadData(location, start, fsize, mime, fd, buffer).wait()
        if 300 <= result.statusCode  < 400
          start = result.rangeEnd + 1
        else if result.statusCode == 401 or result.statusCode == 410          
          fs.closeSync(fd)
          folder.upload(fileName, originalPath, cb)
          upFile.uploading = false
          return null

        else
          start = fsize
          fs.closeSync(fd)

      logger.log 'debug', "finished uploading #{fileName}"      
      unless result.result
        logger.error "result from file uploading was empty."
        logger.error result
        cb(result)
        return null
      cb(null, result.result)

    .run()

#load upload Tree
if fs.existsSync(pth.join(config.cacheLocation, 'data','uploadTree.json'))
  fs.readJson pth.join(config.cacheLocation, 'data','uploadTree.json'), (err, data) ->
    for key in Object.keys(data)
      value = data[key]
      value.uploading = false
      uploadTree.set key, value


module.exports.GFolder = GFolder
module.exports.uploadTree = uploadTree
module.exports.saveUploadTree = saveUploadTree
