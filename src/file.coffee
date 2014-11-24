pth = require 'path'
fs = require 'fs-extra'
hashmap = require( 'hashmap' ).HashMap
rest = require 'restler'
winston = require 'winston'

logger = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)({ level: 'debug' }),
      new (winston.transports.File)({ filename: '/tmp/GDriveF4JS.log', level:'debug' })
    ]
  })
module.exports.logger = logger

######################################
######### Setup File Config ##########
######################################

config = fs.readJSONSync 'config.json'

#download location
downloadLocation = pth.join config.cacheLocation, 'download'
fs.ensureDirSync downloadLocation

#upload location
uploadLocation = pth.join config.cacheLocation, 'upload'
fs.ensureDirSync uploadLocation

downloadTree = new hashmap()


google = require 'googleapis'
oauth2Client = new google.auth.OAuth2(config.clientId, config.clientSecret, config.redirectUrl)
refreshToken =  (cb) ->
  oauth2Client.refreshAccessToken (err,tokens) ->
    if err
      refreshToken(cb)
    else
      config.accessToken = tokens
      fs.outputJsonSync 'config.json', config
      cb()
    return
  return
oauth2Client.setCredentials config.accessToken

buf0 = new Buffer(0)

######################################
######### Create File Class ##########
######################################

class GFile

  @chunkSize: 1024*1024*16 #set default chunk size to 16. this should be changed at run time

  constructor: (@downloadUrl, @id, @parentid, @name, @size, @ctime, @mtime, @permission) ->

  @download = (url, start,end, size, cb ) ->
    rest.get url, {
      decoding: "buffer"
      timeout: 300000
      headers:
        "Authorization": "Bearer #{config.accessToken.access_token}"
        "Range": "bytes=#{start}-#{end}"
    }
    .on 'complete', (result, response) ->
      if result instanceof Error        
        cb(result)
      else
        #check to see if token is expired
        if response.statusCode == 401 or response.statusCode == 403
          logger.debug "There was an error while downloading. refreshing Token"
          logger.debug response.headers
          fn = ->
            GFile.download(url, start,end, size,cb )
            return
          refreshToken(fn)          
        else
          cb(null, result)
      return
    return

  getAttr: (cb) =>
    attr =
      mode: 0o100777,
      size: @size,
      nlink: 1,
      mtime: new Date(@mtime),
      ctime: new Date(@ctime)
    cb(0,attr)
    return

  recursive: (start,end) =>
    file = @
    path = pth.join(downloadLocation, "#{file.id}-#{start}-#{end}")
    fs.exists path, (exists) ->
      unless exists
        unless downloadTree.has("#{file.id}-#{start}")
          downloadTree.set("#{file.id}-#{start}", 1)

          callback = (err,result) ->
            unless err
              if result instanceof Buffer
                fs.writeFile path,result, (err) ->
                  downloadTree.remove("#{file.id}-#{start}")
                  return
            return
          GFile.download(file.downloadUrl, start,end, file.size,callback)
      return

    return
  read: (start,end, readAhead, cb) =>
    file = @
    chunkStart = Math.floor((start)/GFile.chunkSize)* GFile.chunkSize
    chunkEnd = Math.min( Math.ceil(end/GFile.chunkSize) * GFile.chunkSize, file.size)-1

    path = pth.join(downloadLocation, "#{file.id}-#{chunkStart}-#{chunkEnd}")

    if downloadTree.has("#{file.id}-#{chunkStart}")
      fn = ->
        file.read(start, end, readAhead, cb)
        return
      setTimeout fn, 50
      return

    #try to read the file
    fs.exists path, (exists) ->
      unless exists
        if downloadTree.has("#{file.id}-#{chunkStart}")
          fn = ->
            file.download(start, end, readAhead, cb)
            return
          setTimeout fn, 1000
        else
          file.download start, end, readAhead, cb
        return


      fs.open path, 'r', (err,fd) ->
        readSize = end-start;
        buffer = new Buffer(readSize+1)
        fs.read fd,buffer, 0, readSize+1, start-chunkStart, (err, bytesRead, buffer) ->
          fs.close fd, (err) ->
            cb(buffer.slice(0,bytesRead))
            return
          return
        return
      return

    if readAhead
      if chunkStart <= start < chunkStart + 131072
        file.recursive( Math.floor(file.size / GFile.chunkSize) * GFile.chunkSize, file.size-1)
        file.recursive(chunkStart + i * GFile.chunkSize, chunkEnd + i * GFile.chunkSize) for i in [1..config.advancedChunks]


    return

  download:  (start, end, readAhead, cb) =>
    #if file chunk already exists, just download it
    #else download it    
    file = @
    chunkStart = Math.floor((start)/GFile.chunkSize) * GFile.chunkSize
    chunkEnd = Math.min( Math.ceil(end/GFile.chunkSize) * GFile.chunkSize, file.size)-1 #and make sure that it's not bigger than the actual file
    nChunks = (chunkEnd - chunkStart)/GFile.chunkSize



    if nChunks < 1
      unless downloadTree.has("#{file.id}-#{start}")
        logger.debug "starting to download #{file.name}, chunkStart: #{chunkStart}"
        downloadTree.set("#{file.id}-#{start}", 1)
        callback = (err, result)->          
          if err
            logger.error "there was an error downloading file"
            logger.error err
            cb(buf0)
            return
          if result instanceof Buffer
            path = pth.join(downloadLocation, "#{file.id}-#{chunkStart}-#{chunkEnd}")
            fs.writeFile path, result, (err) ->
              if err
                logger.error "there was an error saving #{path}"
                logger.error err
                cb( buf0)
              else
                downloadTree.remove("#{file.id}-#{start}")
                cb result.slice(start - chunkStart, chunkEnd - end )
              return
              
          else
            cb(result)
          return
        GFile.download(file.downloadUrl, chunkStart, chunkEnd, file.size, callback)

      else        
        fn = ->
          file.read(start, end, readAhead, cb)
          return
        setTimeout fn, 1500


    else if nChunks < 2      
      end1 = chunkStart + GFile.chunkSize - 1
      start2 = chunkStart + GFile.chunkSize

      callback1 = (buffer1) ->
        if buffer1.length == 0
          cb(buffer1)
          return
        callback2 = (buffer2) ->
          if buffer2.length == 0
            cb(buffer1)
            return
          cb( Buffer.concat([buffer1, buffer2]) )
          return

        file.read( start2, end, true, callback2)
        return

      file.read( start, end1,true, callback1)

    else
      logger.debug "too many chunks requested, #{nChunks}"
      cb(buf0)

    return

module.exports.GFile = GFile
