fs = require 'fs-extra'
rest = require 'restler'
request = require 'request'
pth = require 'path'
mmm = require('mmmagic')
winston = require 'winston'
hashmap = require('hashmap').HashMap
f = require("./file")
logger = f.logger

if fs.existsSync 'config.json'
  config = fs.readJSONSync 'config.json'
else
  config = {}


config.cacheLocation ||= '/tmp/cache'
google = require 'googleapis'
oauth2Client = new google.auth.OAuth2(config.clientId, config.clientSecret, config.redirectUrl)
oauth2Client.setCredentials config.accessToken
google.options({ auth: oauth2Client, user: config.email })
drive = google.drive({ version: 'v2' })
lockRefresh = false

refreshToken =  (cb) ->
  unless lockRefresh
    lock = true
    oauth2Client.refreshAccessToken (err,tokens) ->
      if err
        logger.debug "There was an error with refreshing access token"
        logger.debug err
        refreshToken(cb)
      else
        config.accessToken = tokens
        fs.outputJson 'config.json', config, (err) ->
          if err
            logger.debug "failed to save config from folder.coffee"
          else
            logger.debug "succesfully saved config from folder.coffee"
            cb()
          return
        lock = false
      return
  else
    cb()

  return

Magic = mmm.Magic;
magic = new Magic(mmm.MAGIC_MIME_TYPE)

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

getNewRangeEnd = (location, fileSize, cb) ->
  rest.put location, {
    headers:
      "Authorization": "Bearer #{config.accessToken.access_token}"
      "Content-Length": 0
      "Content-Range": "bytes */#{fileSize}"
    }
  .on 'complete', (res, resp) ->
    if res instanceof Error
      logger.debug "there was a problem getting a new range end"
      logger.debug "result", res
      logger.debug "resp", resp
      fn = ->
        getNewRangeEnd(location, fileSize, cb)
        return
      refreshToken(fn)
      return

    else

      #if the link is dead or bad
      if resp.statusCode == 404 or resp.statusCode == 410 or resp.statusCode == 401
        logger.debug "the link is no longer valid"
        cb(resp.statusCode, -1)
        return

      range = resp.headers.range || resp.headers.Range
      unless range #sometimes, it doesn't return the range, so assume it is 0.
        logger.error resp.headers
        logger.error res
        cb(resp.statusCode, -1)
        return
      
      end = getRangeEnd(range)
      cb(null,end)

    return
  return
getUploadResumableLink =  (parentId, fileName, fileSize, mime, cb) ->
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
      logger.debug "there was an error with getting a new upload link"
      logger.debug "result", result
      logger.debug "response", resp
      fn = ->
        getUploadResumableLink(parentId, fileName, fileSize, mime, cb)
        return

      refreshToken(fn)
    else
      if resp.statusCode == 401 or resp.statusCode == 400
        if parseInt(resp.headers['content-length']) > 0
          logger.error "There was an error with getting a new resumable link"
          logger.error result
          if result.error
            error = result.error.errors[0]
            idx = error.message.indexOf("Media type")   
            if idx >= 0
              cb("invalid mime");
              return

        else 
          logger.debug result

        logger.debug "refreshing access token while getting resumable upload links"
        fn = ->
          getUploadResumableLink(parentId, fileName, fileSize, mime, cb)
          return

        refreshToken(fn)
      else if resp.statusCode == 200
        cb(null, resp.headers.location)
      else
        # console.log resp.statusCode
        console.log resp.headers
        console.log resp.req._headers
        console.log result
        cb(resp.statusCode)

    return

  return
uploadData = (location, fileLocation, start, fileSize, mime, cb) ->

  #read the data
  readStreamOptions = 
    start: start

  requestOptions = 
    method: "PUT"
    url: location
    headers:
      "content-type": mime
      "Authorization": "Bearer #{config.accessToken.access_token}"
      "Content-Length": (fileSize) - start
      "Content-Range": "bytes #{start}-#{fileSize-1}/#{fileSize}"
  requestCallback = (err, resp, body) ->

    if err
      callback = (err,end) ->
        cb err, {
          rangeEnd: end
        }
        return
      getNewRangeEnd(location, fileSize, callback)
      return

    if resp.statusCode == 400 or resp.statusCode == 401 or resp.statusCode == 410
      callback = (err,end) ->
        logger.debug end
        cb err, {
          statusCode: resp.statusCode
          rangeEnd: end
        }
        return

      getNewRangeEnd(location, fileSize, callback)
      
      
      return

    if resp.statusCode == 404
      cb 404, JSON.parse(body)
    if resp.statusCode == 308 #success on resume
      rangeEnd = getRangeEnd(resp.headers.range)
      cb null, {
        statusCode: 308
        rangeEnd: rangeEnd
      }
      return

    if 200 <= resp.statusCode <= 201
      logger.info "Finished uploading?"
      logger.info JSON.parse(body)
      cb null, {
        statusCode: 201
        rangeEnd: fileSize
        result: JSON.parse(body)
      }
      return


    if resp.statusCode >= 500
      callback = (err,end) ->
        cb null, fd, {
          statusCode: resp.statusCode
          rangeEnd: end
        }
        return


      getNewRangeEnd(location, fileSize, callback)

      return


    logger.error "uncaugt state for file uploading"
    logger.error resp.statusCode
    logger.error resp.headers
    logger.error body    
    callback = (err,end) ->
      cb err, {
        statusCode: resp.statusCode
        rangeEnd: end
      }
      return

    getNewRangeEnd(location, fileSize, callback)



  once = false

  fs.createReadStream( fileLocation, readStreamOptions)
  .pipe(
    request(requestOptions, requestCallback)
  )
  .on 'error', (err)->
    logger.log "error", err
    logger.error err
    cb(err, {rangeEnd: start-1})




  return

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
  return




######################################
######################################
######################################

class GFolder
  @uploadChunkSize: 1024*1024*16
  constructor: (@id, @parentid, @name, @ctime, @mtime, @inode, @permission, @children = [], @mode = 0o40777) ->

  getAttrSync: () =>
    attr =
      mode: @mode,
      size: 4096 #standard size of a directory
      nlink: @children.length + 1,
      mtime: @mtime,
      ctime: @ctime,
      inode: @inode
    return attr
    
  getAttr: (cb) =>
    attr =
      mode: @mode,
      size: 4096 #standard size of a directory
      nlink: @children.length + 1,
      mtime: @mtime,
      ctime: @ctime,
      inode: @inode
    cb(0,attr)
    return

  upload: (fileName, originalPath, cb) =>
    folder = @
    upFile = uploadTree.get originalPath
    unless upFile
      return
    filePath = pth.join uploadLocation, upFile.cache
    #if the file is already being uploaded, don't try again.   
    if upFile and upFile.uploading
      logger.debug "#{fileName} is already being uploaded"
      return


    fs.stat filePath, (err, stats) ->
      if err or stats == undefined        
        logger.debug "there was an errror while trying to upload file #{fileName} with path #{originalPath}"
        logger.debug err
        if err.code == "ENOENT"
          #file was delete
          uploadTree.remove originalPath
        cb(err)

        return
      size = stats.size

      #sometimes, the operating system will create a file of size 0. Simply delete it.

      if size == 0
        fs.unlink filePath, (err) ->
          if err
            logger.debug "there was an error removing a file of size 0, #{filePath}"
            logger.debug err
          return
        return

      fn = ->
        fs.stat filePath, (err, stats2) ->
          if err or stats2 == undefined
            logger.debug "there was an errror while trying to upload file #{fileName} with path #{originalPath}"
            if err.code == "ENOENT"
              #file was delete
              uploadTree.remove originalPath
            cb(err)

            return
          if size != stats2.size #make sure that the cache file is not being written to. mv will create, close and reopen
            fn2 = ->
              folder.upload(fileName, originalPath, cb)
              return
            setTimeout fn2, 10000

          #if the file is already being uploaded, don't try again.
          if upFile.uploading
            return

          upFile.uploading = true   
          magic.detectFile filePath, (err, mime) ->
            if err
              logger.debug "There was an error with detecting mime type"
              logger.debug err

            #if the mime type is binary, set it to application/octect stream so google will accept it
            if mime == 'binary'
              mime = 'application/octet-stream'

            cbUploadData = (err, res) ->
              if err
                logger.error "There was an error with uploading data"
                logger.error err
                logger.error res
                cbfn = -> 
                  up = uploadTree.get(originalPath)
                  up.uploading = false
                  delete up.location               
                  folder.upload(fileName, originalPath, cb)
                  return
                setTimeout cbfn, 60000
                return
              else
                start = res.rangeEnd + 1
                if start < size              
                  uploadData upFile.location, filePath, start, size, mime, cbUploadData
                else
                  logger.debug "successfully uploaded file #{originalPath}"
                  cb(null, res.result)                      
                return                    
              return
            cbNewLink = (err, location) ->
              if err
                cb(err)
                return

              upFile.location = location
              uploadTree.set originalPath, upFile 
              saveUploadTree()

              #once new link is obtained, start uploading
              uploadData location, filePath, 0, size, mime, cbUploadData
              return

            cbNewEnd = (err, end) ->
              if err 
                delete upFile.location
                logger.debug "there was an error with getting a new range end for #{originalPath}"
                logger.debug "err", err
                getUploadResumableLink folder.id, fileName, size, mime, cbNewLink

                return

              if end <= 0
                logger.debug "tried to get new range for #{originalPath}, but it was #{end}"
                delete upFile.location           
                getUploadResumableLink folder.id, fileName, size, mime, cbNewLink
              else
                start = end + 1
                logger.debug "got new range end for #{originalPath}: #{end}"
                #once new range end is obtained, start uploading in chunks
                uploadData location, filePath, start, size, mime, cbUploadData
              return


            logger.log 'debug', "starting to upload file #{fileName}"      
            if upFile.location
              location = upFile.location

              getNewRangeEnd(location,size, cbNewEnd)
              return
            else
              getUploadResumableLink folder.id, fileName, size, mime, cbNewLink
            
            return

          return
        return
      setTimeout fn, 5000
      return
    return

#load upload Tree
if fs.existsSync(pth.join(config.cacheLocation, 'data','uploadTree.json'))
  logger.info "loading upload tree"
  fs.readJson pth.join(config.cacheLocation, 'data','uploadTree.json'), (err, data) ->
    for key in Object.keys(data)
      value = data[key]
      value.uploading = false
      uploadTree.set key, value
    return


module.exports.GFolder = GFolder
module.exports.uploadTree = uploadTree
module.exports.saveUploadTree = saveUploadTree
