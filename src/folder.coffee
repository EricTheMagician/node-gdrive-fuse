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
refreshToken =  () ->
  oauth2Client.refreshAccessToken (err,tokens) ->
    config.accessToken = tokens
    fs.outputJsonSync 'config.json', config


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
getNewRangeEnd = Future.wrap (location, fileSize, cb) ->
  rest.put location, {
    headers:
      "Authorization": "Bearer #{config.accessToken.access_token}"
      "Content-Length": 0
      "Content-Range": "bytes */#{fileSize}"
  }.on 'complete', (res, resp) ->
    if res instanceof Error
      cb(res)
    else
      range = resp.headers.Range
      [start,end] = range.match(/(\d*)-(\d*)/)
      cb(null,end)
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
        refreshToken()
        _getUploadResumableLink(parentId, fileName, fileSize, mime, cb)
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
        cb(err)
      else
        if resp.statusCode == 400 or resp.statusCode == 401
          refreshToken()
          _uploadData(location, start, fileSize, mime, fd, buffer, cb)
          return null

        if resp.statusCode == 308 #success on resume
          md5Server = resp.headers["x-range-md5"]
          rangeEnd = getRangeEnd(resp.headers.range)
          # if (rangeEnd - start + 1) == bytesRead
          #   console.log "end expected"
          #   md5Local = MD5(buffer)
          # else
          #   console.log "end different size"
          #   md5Local = MD5(buffer.slice(0,end-start))
          # console.log "server", md5Server
          # console.log "local", md5Local
          # console.log resp.headers
          # console.log "end: #{rangeEnd}\tend?: #{rangeEnd-start+1}\tbytesRead: #{bytesRead}"

          # if md5Server != md5Local #ma? fileSize, mime, fd, buffer, cb)
          # else
          cb null, {
            statusCode: 308
            rangeEnd: rangeEnd
          }
          return null

        if 200 <= resp.statusCode <= 201
          console.log res
          cb null, {
            statusCode: 201
            result: res
          }
          return null

        if resp.statusCode >= 500
          end =getNewRangeEnd(location, fileSize).wait()
          cb null, {
            statusCode: resp.statusCode
            rangeEnd: end
          }
          return null

        console.log resp.statusCode
        console.log resp.headers



  .run()
uploadData = Future.wrap _uploadData



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

  upload: (fileName, filePath, cb) =>
    folder = @
    Fiber ->

      mime = detectFile(filePath).wait()
      fsize = stat(filePath).wait().size;
      buffer = new Buffer(GFolder.uploadChunkSize)

      logger.log 'debug', "getting upload link to upload #{fileName}"
      location = getUploadResumableLink( folder.id, fileName, fsize, mime ).wait()

      logger.log 'debug', "starting to upload file #{fileName}"
      fd = open(filePath, 'r').wait()
      start = 0

      while start < fsize
        result = uploadData(location, start, fsize, mime, fd, buffer).wait()
        if result.statusCode >= 300
          start = result.rangeEnd + 1
        else
          start = fsize
          fs.closeSync(fd)

      logger.log 'debug', "finished uploading #{fileName}"
      cb(null, result.result)

    .run()

#load upload Tree
if fs.existsSync(pth.join(config.cacheLocation, 'data','uploadTree.json'))
  fs.readJson pth.join(config.cacheLocation, 'data','uploadTree.json'), (err, data) ->
    for key in Object.keys(data)
      uploadTree.set key, data[key]


module.exports.GFolder = GFolder
module.exports.uploadTree = uploadTree
module.exports.saveUploadTree = ->
  toSave = {}
  for key in uploadTree.keys()
    value = uploadTree.get key
    toSave[key] = value

  fs.outputJsonSync pth.join(config.cacheLocation, 'data','uploadTree.json'), toSave
